"""Persistence operations for NMFP fingerprints and embeddings."""

from __future__ import annotations

import logging
import time
from typing import Any

import numpy as np

from search.models import FingerprintCandidate, SearchDateRange
from pipeline.nmfp_inference import (
    NMFP_EMBEDDING_DIM,
    NMFP_MODEL_VERSION,
    NMFP_PREPROCESSING_VERSION,
)
from storage.database import PostgresDatabase


logger = logging.getLogger("uvicorn.error")

NMFP_VECTOR_DIM = NMFP_EMBEDDING_DIM
DEFAULT_NMFP_MODEL_VERSION = NMFP_MODEL_VERSION
DEFAULT_NMFP_PREPROCESSING_VERSION = NMFP_PREPROCESSING_VERSION


class FingerprintRepository:
    """Store and search the fingerprint index for one PostgreSQL database."""

    def __init__(self, database: PostgresDatabase) -> None:
        self.database = database

    @property
    def vector_dim(self) -> int:
        return self.database.vector_dim

    @property
    def hnsw_ef_search(self) -> int:
        return self.database.hnsw_ef_search

    @property
    def model_version(self) -> str:
        return self.database.model_version

    @property
    def preprocessing_version(self) -> str:
        return self.database.preprocessing_version

    def _connect(self):
        return self.database.connect()

    def append_vectors(
        self,
        embeddings: np.ndarray,
        ids: list[int],
        creator_id: int | None,
    ) -> None:
        if embeddings.size == 0:
            return
        if embeddings.ndim != 2 or embeddings.shape[1] != self.vector_dim:
            raise ValueError(f"embeddings must have shape (n, {self.vector_dim})")
        if len(embeddings) != len(ids):
            raise ValueError("embeddings/ids length mismatch")
        if creator_id is None:
            raise ValueError("creator_id is required")

        rows = [
            (
                int(fp_id),
                embeddings[idx].astype(np.float32).tolist(),
                int(creator_id),
                self.model_version,
                self.preprocessing_version,
            )
            for idx, fp_id in enumerate(ids)
        ]
        placeholders = ", ".join(["(%s, %s, %s, %s, %s)"] * len(rows))
        params: list[Any] = []
        for row in rows:
            params.extend(row)

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO fingerprint_embeddings (
                        fingerprint_id,
                        embedding,
                        creator_id,
                        model_version,
                        preprocessing_version
                    )
                    VALUES {placeholders}
                    ON CONFLICT (fingerprint_id) DO UPDATE
                    SET embedding = excluded.embedding,
                        creator_id = excluded.creator_id,
                        model_version = excluded.model_version,
                        preprocessing_version = excluded.preprocessing_version
                    """,
                    params,
                )

    def store_fingerprints(self, video_id: int, timestamps: np.ndarray) -> list[int]:
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

    def query_fingerprint_candidates(
        self,
        query_embeddings: np.ndarray,
        query_timestamps: np.ndarray,
        top_k: int,
        creator_id: int,
        model_version: str | None = None,
        preprocessing_version: str | None = None,
        date_range: SearchDateRange | None = None,
    ) -> list[FingerprintCandidate]:
        """Return ranked, timeline-aware candidates for every query fingerprint."""

        if query_embeddings.size == 0:
            return []
        if query_embeddings.ndim != 2 or query_embeddings.shape[1] != self.vector_dim:
            raise ValueError(f"query_embeddings must have shape (n, {self.vector_dim})")
        if query_timestamps.ndim != 1 or len(query_timestamps) != len(query_embeddings):
            raise ValueError("query_embeddings/query_timestamps length mismatch")
        if int(creator_id) <= 0:
            raise ValueError("creator_id is required")
        if int(top_k) <= 0:
            raise ValueError("top_k must be positive")

        resolved_model_version = str(
            model_version if model_version is not None else self.model_version
        ).strip()
        resolved_preprocessing_version = str(
            preprocessing_version
            if preprocessing_version is not None
            else self.preprocessing_version
        ).strip()
        if not resolved_model_version:
            raise ValueError("model_version is required")
        if not resolved_preprocessing_version:
            raise ValueError("preprocessing_version is required")

        values_sql: list[str] = []
        params: list[Any] = []
        for query_index, (embedding, query_time) in enumerate(
            zip(query_embeddings, query_timestamps)
        ):
            values_sql.append("(%s, %s, %s::vector)")
            params.extend(
                [
                    int(query_index),
                    float(query_time),
                    embedding.astype(np.float32).tolist(),
                ]
            )

        predicates = [
            "fe.creator_id = %s",
            "v.creator_id = %s",
            "fe.model_version = %s",
            "fe.preprocessing_version = %s",
            "v.status IN ('indexing', 'searchable')",
        ]
        params.extend(
            [
                int(creator_id),
                int(creator_id),
                resolved_model_version,
                resolved_preprocessing_version,
            ]
        )
        has_date_bounds = date_range is not None and date_range.has_bounds
        if has_date_bounds:
            predicates.append("v.streamed_at IS NOT NULL")
            if date_range.streamed_from is not None:
                predicates.append("v.streamed_at >= %s")
                params.append(date_range.streamed_from)
            if date_range.streamed_to is not None:
                predicates.append("v.streamed_at < %s")
                params.append(date_range.streamed_to)
        params.append(int(top_k))

        started_at = time.perf_counter()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET LOCAL hnsw.ef_search = {self.hnsw_ef_search}")
                cur.execute(
                    f"""
                    WITH query_fingerprints(query_index, query_time, embedding) AS (
                        VALUES {', '.join(values_sql)}
                    )
                    SELECT
                        query_row.query_index,
                        query_row.query_time,
                        neighbor.fingerprint_id,
                        neighbor.video_id,
                        neighbor.vod_time,
                        neighbor.similarity,
                        neighbor.rank
                    FROM query_fingerprints AS query_row
                    CROSS JOIN LATERAL (
                        SELECT
                            fe.fingerprint_id,
                            f.video_id,
                            f.timestamp_seconds AS vod_time,
                            1 - (fe.embedding <=> query_row.embedding) AS similarity,
                            (
                                ROW_NUMBER() OVER (
                                    ORDER BY fe.embedding <=> query_row.embedding
                                ) - 1
                            )::integer AS rank
                        FROM fingerprint_embeddings AS fe
                        JOIN fingerprints AS f ON f.id = fe.fingerprint_id
                        JOIN videos AS v ON v.id = f.video_id
                        WHERE {' AND '.join(predicates)}
                        -- Keep the inner order distance-only so pgvector can use
                        -- the HNSW index. Tie-breaking happens after LIMIT.
                        ORDER BY fe.embedding <=> query_row.embedding
                        LIMIT %s
                    ) AS neighbor
                    ORDER BY query_row.query_index, neighbor.rank
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
        retrieval_seconds = time.perf_counter() - started_at

        logger.info(
            "timing event=fingerprint_repository_nmfp_candidates query_count=%d "
            "candidate_count=%d creator_id=%d top_k=%d ef_search=%d date_filtered=%s "
            "seconds=%.3f model_version=%s preprocessing_version=%s",
            len(query_embeddings),
            len(rows),
            int(creator_id),
            int(top_k),
            self.hnsw_ef_search,
            has_date_bounds,
            retrieval_seconds,
            resolved_model_version,
            resolved_preprocessing_version,
        )

        return [
            FingerprintCandidate(
                query_index=int(row[0]),
                query_time=float(row[1]),
                fingerprint_id=int(row[2]),
                video_id=int(row[3]),
                vod_time=float(row[4]),
                similarity=float(row[5]),
                rank=int(row[6]),
            )
            for row in rows
        ]
