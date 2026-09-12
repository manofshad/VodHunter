"""PostgreSQL connection and schema boundary for application storage."""

from __future__ import annotations

from backend.db_url import normalize_database_url
from pipeline.nmfp_inference import (
    NMFP_EMBEDDING_DIM,
    NMFP_MODEL_VERSION,
    NMFP_PREPROCESSING_VERSION,
)


class PostgresDatabase:
    """Own connection setup and the schema contract shared by repositories.

    Repositories intentionally open short-lived connections through this
    object.  Keeping connection registration and startup validation here means
    the SQL repositories only own their domain queries.
    """

    def __init__(
        self,
        database_url: str,
        *,
        vector_dim: int = NMFP_EMBEDDING_DIM,
        hnsw_ef_search: int = 100,
        model_version: str = NMFP_MODEL_VERSION,
        preprocessing_version: str = NMFP_PREPROCESSING_VERSION,
    ) -> None:
        self.database_url = normalize_database_url(database_url)
        if not self.database_url:
            raise ValueError("DATABASE_URL is required")

        self.vector_dim = int(vector_dim)
        if self.vector_dim != NMFP_EMBEDDING_DIM:
            raise ValueError(f"NMFP storage requires vector_dim={NMFP_EMBEDDING_DIM}")

        self.hnsw_ef_search = max(int(hnsw_ef_search), 1)
        self.model_version = str(model_version).strip()
        self.preprocessing_version = str(preprocessing_version).strip()
        if not self.model_version:
            raise ValueError("model_version is required")
        if not self.preprocessing_version:
            raise ValueError("preprocessing_version is required")

        try:
            import psycopg  # type: ignore
            from pgvector.psycopg import register_vector  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(
                "Postgres storage requires psycopg and pgvector to be installed"
            ) from exc

        self._psycopg = psycopg
        self._register_vector = register_vector

    def connect(self):
        """Open a registered psycopg connection for one repository operation."""

        connection = self._psycopg.connect(self.database_url)
        self._register_vector(connection)
        return connection

    def ensure_schema_ready(self) -> None:
        """Fail closed when migrations or the NMFP index contract are missing."""

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_extension
                        WHERE extname = 'vector'
                    )
                    """
                )
                extension_row = cur.fetchone()
                if not extension_row or not bool(extension_row[0]):
                    raise RuntimeError("Database extension 'vector' is missing; run Alembic migrations")

                required_tables = (
                    "creators",
                    "videos",
                    "fingerprints",
                    "fingerprint_embeddings",
                    "fingerprint_index_metadata",
                    "vod_ingest_state",
                    "search_requests",
                )
                missing_tables: list[str] = []
                for table_name in required_tables:
                    cur.execute(
                        """
                        SELECT EXISTS (
                            SELECT 1
                            FROM information_schema.tables
                            WHERE table_schema = current_schema()
                              AND table_name = %s
                        )
                        """,
                        (table_name,),
                    )
                    row = cur.fetchone()
                    if not row or not bool(row[0]):
                        missing_tables.append(table_name)

                if missing_tables:
                    raise RuntimeError(
                        "Database schema is incomplete; run Alembic migrations "
                        f"(missing tables: {', '.join(missing_tables)})"
                    )

                required_columns = (
                    ("creators", "profile_image_url"),
                    ("videos", "thumbnail_url"),
                    ("videos", "streamed_at"),
                    ("videos", "status"),
                    ("fingerprint_embeddings", "creator_id"),
                    ("fingerprint_embeddings", "model_version"),
                    ("fingerprint_embeddings", "preprocessing_version"),
                    ("search_requests", "job_status"),
                    ("search_requests", "job_stage"),
                    ("search_requests", "started_at"),
                    ("search_requests", "finished_at"),
                    ("search_requests", "tiktok_url"),
                    ("search_requests", "streamed_from"),
                    ("search_requests", "streamed_to"),
                    ("search_requests", "result_payload"),
                    ("search_requests", "model_version"),
                    ("search_requests", "preprocessing_version"),
                    ("search_requests", "model_startup_duration_ms"),
                    ("search_requests", "model_cold_start"),
                    ("search_requests", "fingerprint_preprocessing_duration_ms"),
                    ("search_requests", "fingerprint_inference_duration_ms"),
                    ("search_requests", "fingerprint_duration_ms"),
                    ("search_requests", "query_fingerprint_count"),
                    ("search_requests", "candidate_count"),
                    ("search_requests", "segment_count"),
                )
                missing_columns: list[str] = []
                for table_name, column_name in required_columns:
                    cur.execute(
                        """
                        SELECT EXISTS (
                            SELECT 1
                            FROM information_schema.columns
                            WHERE table_schema = current_schema()
                              AND table_name = %s
                              AND column_name = %s
                        )
                        """,
                        (table_name, column_name),
                    )
                    row = cur.fetchone()
                    if not row or not bool(row[0]):
                        missing_columns.append(f"{table_name}.{column_name}")

                if missing_columns:
                    raise RuntimeError(
                        "Database schema is incomplete; run Alembic migrations "
                        f"(missing columns: {', '.join(missing_columns)})"
                    )

                cur.execute(
                    """
                    SELECT format_type(attribute.atttypid, attribute.atttypmod)
                    FROM pg_attribute AS attribute
                    JOIN pg_class AS relation ON relation.oid = attribute.attrelid
                    JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
                    WHERE namespace.nspname = current_schema()
                      AND relation.relname = 'fingerprint_embeddings'
                      AND attribute.attname = 'embedding'
                      AND attribute.attnum > 0
                      AND NOT attribute.attisdropped
                    LIMIT 1
                    """
                )
                vector_type_row = cur.fetchone()
                expected_vector_type = f"vector({self.vector_dim})"
                actual_vector_type = (
                    str(vector_type_row[0])
                    if vector_type_row and vector_type_row[0]
                    else None
                )
                if actual_vector_type != expected_vector_type:
                    raise RuntimeError(
                        "Database embedding width does not match the configured NMFP model; "
                        f"expected {expected_vector_type}, found {actual_vector_type or 'missing'}"
                    )

                cur.execute(
                    """
                    SELECT model_version, preprocessing_version, embedding_dim
                    FROM fingerprint_index_metadata
                    WHERE singleton = TRUE
                    LIMIT 1
                    """
                )
                metadata_row = cur.fetchone()
                expected_metadata = (
                    self.model_version,
                    self.preprocessing_version,
                    self.vector_dim,
                )
                actual_metadata = (
                    (str(metadata_row[0]), str(metadata_row[1]), int(metadata_row[2]))
                    if metadata_row is not None
                    else None
                )
                if actual_metadata != expected_metadata:
                    raise RuntimeError(
                        "Database fingerprint index metadata is incompatible with the configured NMFP runtime; "
                        f"expected={expected_metadata} found={actual_metadata or 'missing'}"
                    )
