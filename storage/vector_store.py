from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, List

import numpy as np


class VectorStore:
    def __init__(
        self,
        database_url: str,
        vector_dim: int = 768,
        pgvector_probes: int = 10,
    ):
        self.database_url = self._normalize_database_url(database_url)
        if not self.database_url:
            raise ValueError("DATABASE_URL is required")

        self.vector_dim = int(vector_dim)
        self.pgvector_probes = int(pgvector_probes)

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
        normalized = (url or "").strip()
        if normalized.startswith("postgresql+psycopg://"):
            return "postgresql://" + normalized[len("postgresql+psycopg://") :]
        return normalized

    def _connect(self):
        conn = self._psycopg.connect(self.database_url)
        self._register_vector(conn)
        return conn

    def init_db(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS creators (
                        id BIGSERIAL PRIMARY KEY,
                        name TEXT,
                        url TEXT UNIQUE
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS videos (
                        id BIGSERIAL PRIMARY KEY,
                        creator_id BIGINT REFERENCES creators(id),
                        url TEXT,
                        title TEXT,
                        processed BOOLEAN DEFAULT FALSE
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS fingerprints (
                        id BIGSERIAL PRIMARY KEY,
                        video_id BIGINT REFERENCES videos(id),
                        timestamp_seconds DOUBLE PRECISION
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_fingerprints_video_timestamp
                    ON fingerprints(video_id, timestamp_seconds)
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS live_ingest_state (
                        vod_platform_id TEXT PRIMARY KEY,
                        video_id BIGINT NOT NULL REFERENCES videos(id),
                        streamer TEXT NOT NULL,
                        last_ingested_seconds INTEGER NOT NULL,
                        last_seen_duration_seconds INTEGER NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS fingerprint_embeddings (
                        fingerprint_id BIGINT PRIMARY KEY REFERENCES fingerprints(id) ON DELETE CASCADE,
                        embedding vector({self.vector_dim}) NOT NULL,
                        model_name TEXT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_fingerprint_embeddings_ivfflat_cos
                    ON fingerprint_embeddings USING ivfflat (embedding vector_cosine_ops)
                    WITH (lists = 1000)
                    """
                )
                cur.execute("CREATE INDEX IF NOT EXISTS idx_videos_creator_id ON videos(creator_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_videos_url ON videos(url)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_fingerprints_video_id ON fingerprints(video_id)")

    def append_vectors(self, embeddings: np.ndarray, ids: List[int]) -> None:
        if embeddings.size == 0:
            return
        if len(embeddings) != len(ids):
            raise ValueError("embeddings/ids length mismatch")

        rows = [
            (
                int(fp_id),
                embeddings[idx].astype(np.float32).tolist(),
                "MIT/ast-finetuned-audioset-10-10-0.4593",
            )
            for idx, fp_id in enumerate(ids)
        ]

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO fingerprint_embeddings (fingerprint_id, embedding, model_name)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (fingerprint_id) DO UPDATE
                    SET embedding = excluded.embedding,
                        model_name = excluded.model_name
                    """,
                    rows,
                )

    def store_fingerprints(self, video_id: int, timestamps: np.ndarray) -> List[int]:
        ids: List[int] = []
        with self._connect() as conn:
            with conn.cursor() as cur:
                for ts in timestamps:
                    cur.execute(
                        """
                        INSERT INTO fingerprints (video_id, timestamp_seconds)
                        VALUES (%s, %s)
                        ON CONFLICT (video_id, timestamp_seconds)
                        DO UPDATE SET timestamp_seconds = excluded.timestamp_seconds
                        RETURNING id
                        """,
                        (int(video_id), float(ts)),
                    )
                    row = cur.fetchone()
                    if row is None:
                        raise RuntimeError("Failed to resolve fingerprint id")
                    ids.append(int(row[0]))
        return ids

    def get_video_by_url(self, url: str) -> tuple[int, int, str, str, bool] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, creator_id, url, title, processed
                    FROM videos
                    WHERE url = %s
                    LIMIT 1
                    """,
                    (url,),
                )
                row = cur.fetchone()
        if row is None:
            return None
        return int(row[0]), int(row[1]), str(row[2]), str(row[3]), bool(row[4])

    def create_or_get_creator(self, name: str, url: str) -> int:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO creators (name, url)
                    VALUES (%s, %s)
                    ON CONFLICT (url)
                    DO UPDATE SET name = excluded.name
                    RETURNING id
                    """,
                    (name, url),
                )
                row = cur.fetchone()
        if row is None:
            raise RuntimeError("Failed to resolve creator id")
        return int(row[0])

    def create_video(self, creator_id: int, url: str, title: str, processed: bool) -> int:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO videos (creator_id, url, title, processed)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                    """,
                    (int(creator_id), url, title, bool(processed)),
                )
                row = cur.fetchone()
        if row is None:
            raise RuntimeError("Failed to create video")
        return int(row[0])

    def mark_video_processed(self, video_id: int, processed: bool = True) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE videos SET processed = %s WHERE id = %s",
                    (bool(processed), int(video_id)),
                )

    def get_live_ingest_state(self, vod_platform_id: str) -> dict | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT vod_platform_id, video_id, streamer, last_ingested_seconds,
                           last_seen_duration_seconds, updated_at
                    FROM live_ingest_state
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

    def upsert_live_ingest_state(
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
                    INSERT INTO live_ingest_state (
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

    def query_similar_fingerprint_ids(
        self,
        query_embeddings: np.ndarray,
        top_k: int,
    ) -> tuple[np.ndarray, np.ndarray]:
        if query_embeddings.size == 0:
            return np.empty((0, 0), dtype=np.float32), np.empty((0, 0), dtype=np.int64)

        query_rows = [query_embeddings[idx].astype(np.float32).tolist() for idx in range(query_embeddings.shape[0])]
        all_scores: list[list[float]] = []
        all_ids: list[list[int]] = []

        with self._connect() as conn:
            with conn.cursor() as cur:
                probes = max(int(self.pgvector_probes), 1)
                cur.execute(f"SET LOCAL ivfflat.probes = {probes}")
                for q in query_rows:
                    cur.execute(
                        """
                        SELECT fingerprint_id, (1 - (embedding <=> %s::vector)) AS score
                        FROM fingerprint_embeddings
                        ORDER BY embedding <=> %s::vector
                        LIMIT %s
                        """,
                        (q, q, int(top_k)),
                    )
                    rows = cur.fetchall()
                    all_ids.append([int(r[0]) for r in rows])
                    all_scores.append([float(r[1]) for r in rows])

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

    def get_video_with_creator(self, video_id: int) -> tuple[int, str, str, str] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT videos.id, videos.url, videos.title, creators.name
                    FROM videos
                    JOIN creators ON creators.id = videos.creator_id
                    WHERE videos.id = %s
                    """,
                    (int(video_id),),
                )
                row = cur.fetchone()
        if row is None:
            return None
        return int(row[0]), str(row[1]), str(row[2]), str(row[3])

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
