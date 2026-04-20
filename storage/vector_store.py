from __future__ import annotations

from datetime import datetime, timezone
import logging
import time
from typing import Any, List

import numpy as np

from backend.db_url import normalize_database_url
from search.models import SearchJobRecord, SearchRequestLog, SearchRequestOutcome, SearchResult


logger = logging.getLogger("uvicorn.error")


VIDEO_STATUS_INDEXING = "indexing"
VIDEO_STATUS_SEARCHABLE = "searchable"
VIDEO_STATUS_DELETED = "deleted"
VIDEO_STATUS_REINDEX_REQUESTED = "reindex_requested"
VIDEO_STATUSES = (
    VIDEO_STATUS_INDEXING,
    VIDEO_STATUS_SEARCHABLE,
    VIDEO_STATUS_DELETED,
    VIDEO_STATUS_REINDEX_REQUESTED,
)


class VideoMutationError(Exception):
    pass


class VideoNotFoundError(VideoMutationError):
    pass


class VideoOwnerMismatchError(VideoMutationError):
    pass


class InvalidVideoStateTransitionError(VideoMutationError):
    def __init__(self, current_status: str):
        self.current_status = str(current_status)
        super().__init__(f"Cannot apply requested transition from status '{self.current_status}'")


class VectorStore:
    def __init__(
        self,
        database_url: str,
        vector_dim: int = 768,
        hnsw_ef_search: int = 40,
    ):
        self.database_url = self._normalize_database_url(database_url)
        if not self.database_url:
            raise ValueError("DATABASE_URL is required")

        self.vector_dim = int(vector_dim)
        self.hnsw_ef_search = max(int(hnsw_ef_search), 1)

        try:
            import psycopg  # type: ignore
            from pgvector.psycopg import register_vector  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(
                "Postgres storage requires psycopg and pgvector to be installed"
            ) from exc

        self._psycopg = psycopg
        self._register_vector = register_vector

    def _normalize_database_url(self, url: str) -> str:
        return normalize_database_url(url)

    def _connect(self):
        conn = self._psycopg.connect(self.database_url)
        self._register_vector(conn)
        return conn

    def _normalize_video_status(self, status: str) -> str:
        normalized_status = str(status).strip().lower()
        if normalized_status not in VIDEO_STATUSES:
            raise ValueError(f"Invalid video status: {status}")
        return normalized_status

    def _status_from_processed(self, processed: bool) -> str:
        return VIDEO_STATUS_SEARCHABLE if bool(processed) else VIDEO_STATUS_INDEXING

    def _processed_from_status(self, status: str) -> bool:
        return self._normalize_video_status(status) != VIDEO_STATUS_INDEXING

    def ensure_schema_ready(self) -> None:
        with self._connect() as conn:
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
                    ("search_requests", "job_status"),
                    ("search_requests", "job_stage"),
                    ("search_requests", "started_at"),
                    ("search_requests", "finished_at"),
                    ("search_requests", "tiktok_url"),
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

    def append_vectors(self, embeddings: np.ndarray, ids: List[int], creator_id: int | None) -> None:
        if embeddings.size == 0:
            return
        if len(embeddings) != len(ids):
            raise ValueError("embeddings/ids length mismatch")
        if creator_id is None:
            raise ValueError("creator_id is required")

        rows = [
            (
                int(fp_id),
                embeddings[idx].astype(np.float32).tolist(),
                int(creator_id),
                "MIT/ast-finetuned-audioset-10-10-0.4593",
            )
            for idx, fp_id in enumerate(ids)
        ]

        placeholders = ", ".join(["(%s, %s, %s, %s)"] * len(rows))
        params: list[Any] = []
        for row in rows:
            params.extend(row)

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO fingerprint_embeddings (fingerprint_id, embedding, creator_id, model_name)
                    VALUES {placeholders}
                    ON CONFLICT (fingerprint_id) DO UPDATE
                    SET embedding = excluded.embedding,
                        creator_id = excluded.creator_id,
                        model_name = excluded.model_name
                    """,
                    params,
                )

    def store_fingerprints(self, video_id: int, timestamps: np.ndarray) -> List[int]:
        if timestamps.size == 0:
            return []

        timestamp_values = [float(ts) for ts in timestamps]

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH input_rows AS (
                        SELECT
                            %s::bigint AS video_id,
                            ts::double precision AS timestamp_seconds,
                            ord::integer AS ord
                        FROM unnest(%s::double precision[]) WITH ORDINALITY AS t(ts, ord)
                    ),
                    upserted AS (
                        INSERT INTO fingerprints (video_id, timestamp_seconds)
                        SELECT video_id, timestamp_seconds
                        FROM input_rows
                        ON CONFLICT (video_id, timestamp_seconds)
                        DO UPDATE SET timestamp_seconds = excluded.timestamp_seconds
                        RETURNING id, video_id, timestamp_seconds
                    )
                    SELECT upserted.id
                    FROM input_rows
                    JOIN upserted
                      ON upserted.video_id = input_rows.video_id
                     AND upserted.timestamp_seconds = input_rows.timestamp_seconds
                    ORDER BY input_rows.ord
                    """,
                    (int(video_id), timestamp_values),
                )
                rows = cur.fetchall()

        ids = [int(row[0]) for row in rows]
        if len(ids) != len(timestamp_values):
            raise RuntimeError("Failed to resolve fingerprint ids")
        return ids

    def get_video_by_url(self, url: str) -> tuple[int, int, str, str, str | None, bool, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, creator_id, url, title, thumbnail_url, processed, streamed_at
                    FROM videos
                    WHERE url = %s
                    LIMIT 1
                    """,
                    (url,),
                )
                row = cur.fetchone()
        if row is None:
            return None
        return int(row[0]), int(row[1]), str(row[2]), str(row[3]), str(row[4]) if row[4] else None, bool(row[5]), row[6]

    def create_or_get_creator(
        self,
        name: str,
        url: str,
        profile_image_url: str | None = None,
    ) -> int:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO creators (name, url, profile_image_url)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (url)
                    DO UPDATE SET
                        name = excluded.name,
                        profile_image_url = COALESCE(excluded.profile_image_url, creators.profile_image_url)
                    RETURNING id
                    """,
                    (name, url, profile_image_url),
                )
                row = cur.fetchone()
        if row is None:
            raise RuntimeError("Failed to resolve creator id")
        return int(row[0])

    def update_creator_metadata(
        self,
        creator_id: int,
        *,
        name: str | None = None,
        profile_image_url: str | None = None,
    ) -> None:
        assignments: list[str] = []
        values: list[Any] = []

        if name is not None:
            assignments.append("name = %s")
            values.append(name)
        if profile_image_url is not None:
            assignments.append("profile_image_url = %s")
            values.append(profile_image_url)

        if not assignments:
            return

        values.append(int(creator_id))
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE creators SET {', '.join(assignments)} WHERE id = %s",
                    values,
                )

    def get_creator_id_by_name(self, name: str) -> int | None:
        normalized_name = (name or "").strip().lower()
        if not normalized_name:
            raise ValueError("streamer is required")

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id
                    FROM creators
                    WHERE LOWER(name) = %s
                    LIMIT 1
                    """,
                    (normalized_name,),
                )
                row = cur.fetchone()
        if row is None:
            return None
        return int(row[0])

    def create_video(
        self,
        creator_id: int,
        url: str,
        title: str,
        processed: bool,
        thumbnail_url: str | None = None,
        streamed_at: Any = None,
        status: str | None = None,
    ) -> int:
        resolved_status = (
            self._normalize_video_status(status)
            if status is not None
            else self._status_from_processed(processed)
        )
        resolved_processed = self._processed_from_status(resolved_status)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO videos (creator_id, url, title, thumbnail_url, processed, streamed_at, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (int(creator_id), url, title, thumbnail_url, resolved_processed, streamed_at, resolved_status),
                )
                row = cur.fetchone()
        if row is None:
            raise RuntimeError("Failed to create video")
        return int(row[0])

    def update_video_metadata(
        self,
        video_id: int,
        *,
        title: str | None = None,
        thumbnail_url: str | None = None,
        processed: bool | None = None,
        streamed_at: Any = None,
        status: str | None = None,
    ) -> None:
        assignments: list[str] = []
        values: list[Any] = []

        if title is not None:
            assignments.append("title = %s")
            values.append(title)
        if thumbnail_url is not None:
            assignments.append("thumbnail_url = %s")
            values.append(thumbnail_url)
        if status is not None:
            resolved_status = self._normalize_video_status(status)
            assignments.append("status = %s")
            values.append(resolved_status)
            assignments.append("processed = %s")
            values.append(self._processed_from_status(resolved_status))
        elif processed is not None:
            resolved_status = self._status_from_processed(processed)
            assignments.append("processed = %s")
            values.append(bool(processed))
            assignments.append("status = %s")
            values.append(resolved_status)
        if streamed_at is not None:
            assignments.append("streamed_at = %s")
            values.append(streamed_at)

        if not assignments:
            return

        values.append(int(video_id))
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE videos SET {', '.join(assignments)} WHERE id = %s",
                    values,
                )

    def mark_video_processed(self, video_id: int, processed: bool = True) -> None:
        resolved_status = self._status_from_processed(processed)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE videos SET processed = %s, status = %s WHERE id = %s",
                    (bool(processed), resolved_status, int(video_id)),
                )

    def get_video_status(self, video_id: int) -> str | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status
                    FROM videos
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (int(video_id),),
                )
                row = cur.fetchone()
        if row is None or row[0] is None:
            return None
        return self._normalize_video_status(str(row[0]))

    def update_video_status(self, video_id: int, status: str) -> None:
        resolved_status = self._normalize_video_status(status)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE videos SET status = %s, processed = %s WHERE id = %s",
                    (resolved_status, self._processed_from_status(resolved_status), int(video_id)),
                )

    def _lock_video_for_mutation(self, cur, video_id: int) -> tuple[int, str]:
        cur.execute(
            """
            SELECT creator_id, status
            FROM videos
            WHERE id = %s
            LIMIT 1
            FOR UPDATE
            """,
            (int(video_id),),
        )
        row = cur.fetchone()
        if row is None:
            raise VideoNotFoundError()
        return int(row[0]), self._normalize_video_status(str(row[1]))

    def delete_video_index(self, video_id: int, actor_creator_id: int) -> str:
        with self._connect() as conn:
            with conn.cursor() as cur:
                owner_creator_id, current_status = self._lock_video_for_mutation(cur, video_id)
                if owner_creator_id != int(actor_creator_id):
                    raise VideoOwnerMismatchError()
                if current_status == VIDEO_STATUS_DELETED:
                    return VIDEO_STATUS_DELETED
                if current_status != VIDEO_STATUS_SEARCHABLE:
                    raise InvalidVideoStateTransitionError(current_status)

                cur.execute(
                    """
                    DELETE FROM fingerprint_embeddings
                    WHERE fingerprint_id IN (
                        SELECT id
                        FROM fingerprints
                        WHERE video_id = %s
                    )
                    """,
                    (int(video_id),),
                )
                cur.execute(
                    "DELETE FROM fingerprints WHERE video_id = %s",
                    (int(video_id),),
                )
                cur.execute(
                    "DELETE FROM vod_ingest_state WHERE video_id = %s",
                    (int(video_id),),
                )
                cur.execute(
                    "UPDATE videos SET status = %s, processed = %s WHERE id = %s",
                    (
                        VIDEO_STATUS_DELETED,
                        self._processed_from_status(VIDEO_STATUS_DELETED),
                        int(video_id),
                    ),
                )
        return VIDEO_STATUS_DELETED

    def request_video_reindex(self, video_id: int, actor_creator_id: int) -> str:
        with self._connect() as conn:
            with conn.cursor() as cur:
                owner_creator_id, current_status = self._lock_video_for_mutation(cur, video_id)
                if owner_creator_id != int(actor_creator_id):
                    raise VideoOwnerMismatchError()
                if current_status == VIDEO_STATUS_REINDEX_REQUESTED:
                    return VIDEO_STATUS_REINDEX_REQUESTED
                if current_status != VIDEO_STATUS_DELETED:
                    raise InvalidVideoStateTransitionError(current_status)

                cur.execute(
                    "DELETE FROM vod_ingest_state WHERE video_id = %s",
                    (int(video_id),),
                )
                cur.execute(
                    "UPDATE videos SET status = %s, processed = %s WHERE id = %s",
                    (
                        VIDEO_STATUS_REINDEX_REQUESTED,
                        self._processed_from_status(VIDEO_STATUS_REINDEX_REQUESTED),
                        int(video_id),
                    ),
                )
        return VIDEO_STATUS_REINDEX_REQUESTED

    def get_video_with_owner(
        self,
        video_id: int,
    ) -> tuple[int, int, str, str, str | None, str, bool, Any, str, str | None] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        videos.id,
                        videos.creator_id,
                        videos.url,
                        videos.title,
                        videos.thumbnail_url,
                        videos.status,
                        videos.processed,
                        videos.streamed_at,
                        creators.name,
                        creators.profile_image_url
                    FROM videos
                    JOIN creators ON creators.id = videos.creator_id
                    WHERE videos.id = %s
                    """,
                    (int(video_id),),
                )
                row = cur.fetchone()
        if row is None:
            return None
        return (
            int(row[0]),
            int(row[1]),
            str(row[2]),
            str(row[3]),
            str(row[4]) if row[4] else None,
            self._normalize_video_status(str(row[5])),
            bool(row[6]),
            row[7],
            str(row[8]),
            str(row[9]) if row[9] else None,
        )

    def get_vod_ingest_state(self, vod_platform_id: str) -> dict | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT vod_platform_id, video_id, streamer, last_ingested_seconds,
                           last_seen_duration_seconds, updated_at
                    FROM vod_ingest_state
                    WHERE vod_platform_id = %s
                    LIMIT 1
                    """,
                    (vod_platform_id,),
                )
                row = cur.fetchone()
        if row is None:
            return None
        return {
            "vod_platform_id": str(row[0]),
            "video_id": int(row[1]),
            "streamer": str(row[2]),
            "last_ingested_seconds": int(row[3]),
            "last_seen_duration_seconds": int(row[4]),
            "updated_at": str(row[5]),
        }

    def upsert_vod_ingest_state(
        self,
        vod_platform_id: str,
        video_id: int,
        streamer: str,
        last_ingested_seconds: int,
        last_seen_duration_seconds: int,
    ) -> None:
        updated_at = datetime.now(timezone.utc)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO vod_ingest_state (
                        vod_platform_id,
                        video_id,
                        streamer,
                        last_ingested_seconds,
                        last_seen_duration_seconds,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (vod_platform_id) DO UPDATE SET
                        video_id = excluded.video_id,
                        streamer = excluded.streamer,
                        last_ingested_seconds = excluded.last_ingested_seconds,
                        last_seen_duration_seconds = excluded.last_seen_duration_seconds,
                        updated_at = excluded.updated_at
                    """,
                    (
                        vod_platform_id,
                        int(video_id),
                        streamer,
                        int(last_ingested_seconds),
                        int(last_seen_duration_seconds),
                        updated_at,
                    ),
                )

    def delete_vod_ingest_state(self, vod_platform_id: str) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM vod_ingest_state WHERE vod_platform_id = %s",
                    (vod_platform_id,),
                )

    def get_live_ingest_state(self, vod_platform_id: str) -> dict | None:
        return self.get_vod_ingest_state(vod_platform_id)

    def upsert_live_ingest_state(
        self,
        vod_platform_id: str,
        video_id: int,
        streamer: str,
        last_ingested_seconds: int,
        last_seen_duration_seconds: int,
    ) -> None:
        self.upsert_vod_ingest_state(
            vod_platform_id=vod_platform_id,
            video_id=video_id,
            streamer=streamer,
            last_ingested_seconds=last_ingested_seconds,
            last_seen_duration_seconds=last_seen_duration_seconds,
        )

    def query_similar_fingerprint_ids(
        self,
        query_embeddings: np.ndarray,
        top_k: int,
        creator_id: int,
    ) -> tuple[np.ndarray, np.ndarray]:
        if query_embeddings.size == 0:
            return np.empty((0, 0), dtype=np.float32), np.empty((0, 0), dtype=np.int64)

        if int(creator_id) <= 0:
            raise ValueError("creator_id is required")

        query_rows = [query_embeddings[idx].astype(np.float32).tolist() for idx in range(query_embeddings.shape[0])]
        all_scores: list[list[float]] = []
        all_ids: list[list[int]] = []
        primary_query_seconds = 0.0

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET LOCAL hnsw.ef_search = {self.hnsw_ef_search}")
                for q in query_rows:
                    started_at = time.perf_counter()
                    cur.execute(
                        """
                        SELECT fingerprint_id, (1 - (embedding <=> %s::vector)) AS score
                        FROM fingerprint_embeddings
                        WHERE creator_id = %s
                        ORDER BY embedding <=> %s::vector
                        LIMIT %s
                        """,
                        (q, int(creator_id), q, int(top_k)),
                    )
                    rows = cur.fetchall()
                    primary_query_seconds += time.perf_counter() - started_at
                    all_ids.append([int(r[0]) for r in rows])
                    all_scores.append([float(r[1]) for r in rows])

        logger.info(
            "timing event=vector_store_knn query_count=%d creator_id=%d ef_search=%d primary_seconds=%.2f first_result_count=%d",
            len(query_rows),
            int(creator_id),
            self.hnsw_ef_search,
            primary_query_seconds,
            len(all_ids[0]) if all_ids else 0,
        )

        if not all_ids or not all_ids[0]:
            return np.empty((0, 0), dtype=np.float32), np.empty((0, 0), dtype=np.int64)

        k = min(len(row) for row in all_ids)
        if k <= 0:
            return np.empty((0, 0), dtype=np.float32), np.empty((0, 0), dtype=np.int64)

        ids_np = np.array([row[:k] for row in all_ids], dtype=np.int64)
        scores_np = np.array([row[:k] for row in all_scores], dtype=np.float32)
        return scores_np, ids_np

    def get_fingerprint_rows(self, ids: List[int]) -> List[tuple[int, int, float]]:
        if not ids:
            return []

        unique_ids = [int(v) for v in sorted(set(ids))]
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, video_id, timestamp_seconds
                    FROM fingerprints
                    WHERE id = ANY(%s)
                    """,
                    (unique_ids,),
                )
                rows = cur.fetchall()
        return [(int(r[0]), int(r[1]), float(r[2])) for r in rows]

    def get_video_with_creator(self, video_id: int) -> tuple[int, str, str, str, str | None, str | None] | None:
        row = self.get_video_with_owner(video_id)
        if row is None:
            return None
        return (
            int(row[0]),
            str(row[2]),
            str(row[3]),
            str(row[8]),
            str(row[4]) if row[4] else None,
            str(row[9]) if row[9] else None,
        )

    def delete_video_index(self, video_id: int, actor_creator_id: int) -> bool:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT creator_id
                    FROM videos
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (int(video_id),),
                )
                row = cur.fetchone()
                if row is None or int(row[0]) != int(actor_creator_id):
                    return False

                cur.execute(
                    "DELETE FROM fingerprints WHERE video_id = %s",
                    (int(video_id),),
                )
        return True

    def request_video_reindex(self, video_id: int, actor_creator_id: int) -> bool:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT creator_id
                    FROM videos
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (int(video_id),),
                )
                row = cur.fetchone()
                if row is None or int(row[0]) != int(actor_creator_id):
                    return False

                cur.execute(
                    "DELETE FROM vod_ingest_state WHERE video_id = %s",
                    (int(video_id),),
                )
                cur.execute(
                    "DELETE FROM fingerprints WHERE video_id = %s",
                    (int(video_id),),
                )
                cur.execute(
                    "UPDATE videos SET processed = FALSE WHERE id = %s",
                    (int(video_id),),
                )
        return True


    def list_live_sessions(self, limit: int, offset: int) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT videos.id, creators.name, videos.url, videos.title, videos.processed
                    FROM videos
                    JOIN creators ON creators.id = videos.creator_id
                    WHERE videos.url LIKE 'https://twitch.tv/%%' OR videos.url LIKE 'https://www.twitch.tv/%%'
                    ORDER BY videos.id DESC
                    LIMIT %s OFFSET %s
                    """,
                    (int(limit), int(offset)),
                )
                rows = cur.fetchall()
        return [
            {
                "video_id": int(r[0]),
                "creator_name": str(r[1]),
                "url": str(r[2]),
                "title": str(r[3]),
                "processed": bool(r[4]),
            }
            for r in rows
        ]

    def list_searchable_streamers(self) -> list[dict[str, str | None]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT c.name, c.profile_image_url
                    FROM creators c
                    JOIN videos v ON v.creator_id = c.id
                    JOIN fingerprints f ON f.video_id = v.id
                    JOIN fingerprint_embeddings fe ON fe.fingerprint_id = f.id
                    WHERE c.name IS NOT NULL AND BTRIM(c.name) <> ''
                    GROUP BY c.name, c.profile_image_url
                    ORDER BY LOWER(c.name), c.name
                    """
                )
                rows = cur.fetchall()
        return [
            {
                "name": str(r[0]),
                "profile_image_url": str(r[1]) if r[1] else None,
            }
            for r in rows
        ]

    def log_search_request(self, log: SearchRequestLog) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO search_requests (
                        source_app,
                        route,
                        input_type,
                        streamer,
                        creator_id,
                        success,
                        http_status,
                        error_code,
                        error_message,
                        result_reason,
                        found_match,
                        matched_video_id,
                        matched_timestamp_seconds,
                        score,
                        clip_filename,
                        download_source,
                        download_host,
                        input_duration_seconds,
                        total_duration_ms,
                        preprocess_duration_ms,
                        embed_duration_ms,
                        vector_query_duration_ms,
                        alignment_duration_ms
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        log.source_app,
                        log.route,
                        log.input_type,
                        log.streamer,
                        log.creator_id,
                        bool(log.success),
                        log.http_status,
                        log.error_code,
                        log.error_message,
                        log.result_reason,
                        log.found_match,
                        log.matched_video_id,
                        log.matched_timestamp_seconds,
                        log.score,
                        log.clip_filename,
                        log.download_source,
                        log.download_host,
                        log.input_duration_seconds,
                        log.total_duration_ms,
                        log.preprocess_duration_ms,
                        log.embed_duration_ms,
                        log.vector_query_duration_ms,
                        log.alignment_duration_ms,
                    ),
                )

    def create_public_search_job(self, *, tiktok_url: str, streamer: str, creator_id: int | None) -> int:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO search_requests (
                        source_app,
                        route,
                        input_type,
                        streamer,
                        creator_id,
                        success,
                        job_status,
                        job_stage,
                        tiktok_url
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    ("public", "/api/search/clip", "tiktok_url", streamer, creator_id, False, "queued", "validating", tiktok_url),
                )
                row = cur.fetchone()
        if row is None:
            raise RuntimeError("Failed to create public search job")
        return int(row[0])

    def update_search_job_status(
        self,
        search_id: int,
        *,
        status: str | None = None,
        stage: str | None = None,
        started: bool = False,
    ) -> None:
        assignments: list[str] = []
        values: list[Any] = []
        if status is not None:
            assignments.append("job_status = %s")
            values.append(status)
        if stage is not None:
            assignments.append("job_stage = %s")
            values.append(stage)
        if started:
            assignments.append("started_at = COALESCE(started_at, NOW())")
        if not assignments:
            return
        values.append(int(search_id))
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE search_requests
                    SET {', '.join(assignments)}
                    WHERE id = %s
                    """,
                    values,
                )

    def complete_search_job(self, search_id: int, outcome: SearchRequestOutcome) -> None:
        metadata = outcome.execution_metadata
        result = outcome.result
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE search_requests
                    SET success = TRUE,
                        http_status = 200,
                        error_code = NULL,
                        error_message = NULL,
                        result_reason = %s,
                        found_match = %s,
                        matched_video_id = %s,
                        matched_timestamp_seconds = %s,
                        score = %s,
                        clip_filename = %s,
                        download_source = %s,
                        download_host = %s,
                        input_duration_seconds = %s,
                        total_duration_ms = %s,
                        preprocess_duration_ms = %s,
                        embed_duration_ms = %s,
                        vector_query_duration_ms = %s,
                        alignment_duration_ms = %s,
                        job_status = 'completed',
                        job_stage = NULL,
                        finished_at = NOW()
                    WHERE id = %s
                    """,
                    (
                        metadata.result_reason or result.reason,
                        metadata.found_match if metadata.found_match is not None else result.found,
                        metadata.matched_video_id if metadata.matched_video_id is not None else result.video_id,
                        metadata.matched_timestamp_seconds if metadata.matched_timestamp_seconds is not None else result.timestamp_seconds,
                        metadata.score if metadata.score is not None else result.score,
                        outcome.clip_filename,
                        outcome.download_source,
                        outcome.download_host,
                        outcome.input_duration_seconds,
                        outcome.total_duration_ms,
                        metadata.preprocess_duration_ms,
                        metadata.embed_duration_ms,
                        metadata.vector_query_duration_ms,
                        metadata.alignment_duration_ms,
                        int(search_id),
                    ),
                )

    def fail_search_job(
        self,
        search_id: int,
        *,
        error_code: str,
        error_message: str,
        http_status: int,
        input_duration_seconds: float | None = None,
    ) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE search_requests
                    SET success = FALSE,
                        http_status = %s,
                        error_code = %s,
                        error_message = %s,
                        input_duration_seconds = COALESCE(%s, input_duration_seconds),
                        job_status = 'failed',
                        job_stage = NULL,
                        finished_at = NOW()
                    WHERE id = %s
                    """,
                    (int(http_status), error_code, error_message, input_duration_seconds, int(search_id)),
                )

    def get_public_search_job(self, search_id: int) -> SearchJobRecord | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        id,
                        job_status,
                        job_stage,
                        created_at,
                        started_at,
                        finished_at,
                        found_match,
                        streamer,
                        matched_video_id,
                        matched_timestamp_seconds,
                        score,
                        result_reason,
                        error_code,
                        error_message
                    FROM search_requests
                    WHERE id = %s
                      AND source_app = 'public'
                    LIMIT 1
                    """,
                    (int(search_id),),
                )
                row = cur.fetchone()
        if row is None:
            return None
        (
            record_id,
            status,
            stage,
            created_at,
            started_at,
            finished_at,
            found_match,
            streamer,
            matched_video_id,
            matched_timestamp_seconds,
            score,
            result_reason,
            error_code,
            error_message,
        ) = row

        result: SearchResult | None = None
        if status == "completed":
            result = SearchResult(
                found=bool(found_match),
                streamer=str(streamer) if streamer else None,
                video_id=int(matched_video_id) if matched_video_id is not None else None,
                score=float(score) if score is not None else None,
                reason=str(result_reason) if result_reason else None,
            )
            if matched_video_id is not None:
                video_row = self.get_video_with_creator(int(matched_video_id))
                if video_row is not None:
                    video_id, video_url, title, creator_name, thumbnail_url, profile_image_url = video_row
                    result.streamer = creator_name
                    result.profile_image_url = profile_image_url
                    result.video_id = video_id
                    result.video_url = video_url
                    result.thumbnail_url = thumbnail_url
                    result.title = title
                    cur_timestamp = int(matched_timestamp_seconds) if matched_timestamp_seconds is not None else None
                    result.timestamp_seconds = cur_timestamp
                    if cur_timestamp is not None:
                        from search.twitch_time import build_twitch_timestamp_url

                        result.video_url_at_timestamp = build_twitch_timestamp_url(video_url, cur_timestamp)
            else:
                result.profile_image_url = self._get_profile_image_for_streamer(str(streamer) if streamer else None)

        return SearchJobRecord(
            id=int(record_id),
            status=str(status),
            stage=str(stage) if stage else None,
            created_at=self._isoformat(created_at),
            started_at=self._isoformat(started_at),
            finished_at=self._isoformat(finished_at),
            result=result,
            error_code=str(error_code) if error_code else None,
            error_message=str(error_message) if error_message else None,
        )

    def fail_incomplete_public_search_jobs(self, *, error_code: str, error_message: str) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE search_requests
                    SET success = FALSE,
                        http_status = 500,
                        error_code = %s,
                        error_message = %s,
                        job_status = 'failed',
                        job_stage = NULL,
                        finished_at = COALESCE(finished_at, NOW())
                    WHERE source_app = 'public'
                      AND job_status IN ('queued', 'running')
                    """,
                    (error_code, error_message),
                )

    def _get_profile_image_for_streamer(self, streamer: str | None) -> str | None:
        normalized_streamer = (streamer or "").strip().lower()
        if not normalized_streamer:
            return None
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT profile_image_url
                    FROM creators
                    WHERE LOWER(name) = %s
                    LIMIT 1
                    """,
                    (normalized_streamer,),
                )
                row = cur.fetchone()
        if row is None or not row[0]:
            return None
        return str(row[0])

    def _isoformat(self, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        return str(value)
