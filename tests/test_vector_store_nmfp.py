from __future__ import annotations

from datetime import datetime, timezone
import json

import numpy as np
import pytest

from search.models import (
    SearchDateRange,
    SearchExecutionMetadata,
    SearchRequestOutcome,
    SearchResult,
    SearchSegment,
    UnmatchedRange,
)
from storage.vector_store import (
    DEFAULT_NMFP_MODEL_VERSION,
    DEFAULT_NMFP_PREPROCESSING_VERSION,
    NMFP_VECTOR_DIM,
    VectorStore,
)


class FakeCursor:
    def __init__(self, *, rows: list[tuple] | None = None, row: tuple | None = None):
        self.executed: list[tuple[str, tuple | list | None]] = []
        self.rows = list(rows or [])
        self.row = row

    def execute(self, query: str, params=None) -> None:
        self.executed.append((query, params))

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        return self.row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None


class FakeConnection:
    def __init__(self, cursor: FakeCursor):
        self.cursor_value = cursor

    def cursor(self):
        return self.cursor_value

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None


def build_store(cursor: FakeCursor) -> VectorStore:
    store = VectorStore.__new__(VectorStore)
    store.vector_dim = NMFP_VECTOR_DIM
    store.hnsw_ef_search = 40
    store.model_version = DEFAULT_NMFP_MODEL_VERSION
    store.preprocessing_version = DEFAULT_NMFP_PREPROCESSING_VERSION
    store._connect = lambda: FakeConnection(cursor)
    return store


def test_append_vectors_persists_exact_nmfp_versions() -> None:
    cursor = FakeCursor()
    store = build_store(cursor)

    store.append_vectors(
        np.ones((1, NMFP_VECTOR_DIM), dtype=np.float32),
        [17],
        creator_id=9,
    )

    query, params = cursor.executed[0]
    assert "model_version" in query
    assert "preprocessing_version" in query
    assert "model_name" not in query
    assert params[-2:] == [DEFAULT_NMFP_MODEL_VERSION, DEFAULT_NMFP_PREPROCESSING_VERSION]


def test_append_vectors_rejects_non_nmfp_width() -> None:
    store = build_store(FakeCursor())

    with pytest.raises(ValueError, match=r"shape \(n, 128\)"):
        store.append_vectors(np.ones((1, 768), dtype=np.float32), [17], creator_id=9)


def test_query_fingerprint_candidates_batches_rows_and_retains_alignment_evidence() -> None:
    cursor = FakeCursor(
        rows=[
            (0, 0.0, 101, 7, 100.0, 0.91, 0),
            (0, 0.0, 102, 8, 500.0, 0.88, 1),
            (1, 0.5, 103, 7, 100.5, 0.93, 0),
        ]
    )
    store = build_store(cursor)
    date_range = SearchDateRange(
        streamed_from=datetime(2026, 4, 1, tzinfo=timezone.utc),
        streamed_to=datetime(2026, 4, 8, tzinfo=timezone.utc),
    )

    candidates = store.query_fingerprint_candidates(
        query_embeddings=np.ones((2, NMFP_VECTOR_DIM), dtype=np.float32),
        query_timestamps=np.array([0.0, 0.5], dtype=np.float32),
        top_k=10,
        creator_id=9,
        model_version=DEFAULT_NMFP_MODEL_VERSION,
        preprocessing_version=DEFAULT_NMFP_PREPROCESSING_VERSION,
        date_range=date_range,
    )

    assert [(item.query_index, item.query_time, item.fingerprint_id) for item in candidates] == [
        (0, 0.0, 101),
        (0, 0.0, 102),
        (1, 0.5, 103),
    ]
    assert [(item.video_id, item.vod_time, item.similarity, item.rank) for item in candidates] == [
        (7, 100.0, 0.91, 0),
        (8, 500.0, 0.88, 1),
        (7, 100.5, 0.93, 0),
    ]

    assert len(cursor.executed) == 2
    assert "SET LOCAL hnsw.ef_search = 40" in cursor.executed[0][0]
    query, params = cursor.executed[1]
    assert "WITH query_fingerprints" in query
    assert "CROSS JOIN LATERAL" in query
    assert query.count("%s::vector") == 2
    assert "JOIN fingerprints AS f" in query
    assert "JOIN videos AS v" in query
    assert "fe.model_version = %s" in query
    assert "fe.preprocessing_version = %s" in query
    assert "ORDER BY fe.embedding <=> query_row.embedding, fe.fingerprint_id" not in query
    assert "ORDER BY fe.embedding <=> query_row.embedding" in query
    assert "v.streamed_at >= %s" in query
    assert "v.streamed_at < %s" in query
    assert params[-7:] == (
        9,
        9,
        DEFAULT_NMFP_MODEL_VERSION,
        DEFAULT_NMFP_PREPROCESSING_VERSION,
        datetime(2026, 4, 1, tzinfo=timezone.utc),
        datetime(2026, 4, 8, tzinfo=timezone.utc),
        10,
    )


class SchemaCursor(FakeCursor):
    def __init__(
        self,
        *,
        vector_type: str = "vector(128)",
        metadata: tuple | None = (
            DEFAULT_NMFP_MODEL_VERSION,
            DEFAULT_NMFP_PREPROCESSING_VERSION,
            NMFP_VECTOR_DIM,
        ),
    ):
        super().__init__()
        self.vector_type = vector_type
        self.metadata = metadata
        self.last_query = ""

    def execute(self, query: str, params=None) -> None:
        self.last_query = query
        super().execute(query, params)

    def fetchone(self):
        if "FROM pg_extension" in self.last_query:
            return (True,)
        if "FROM information_schema.tables" in self.last_query:
            return (True,)
        if "FROM information_schema.columns" in self.last_query:
            return (True,)
        if "SELECT format_type" in self.last_query:
            return (self.vector_type,)
        if "FROM fingerprint_index_metadata" in self.last_query:
            return self.metadata
        return None


def test_schema_readiness_verifies_nmfp_width_and_versions() -> None:
    cursor = SchemaCursor()
    store = build_store(cursor)

    store.ensure_schema_ready()

    assert any("SELECT format_type" in query for query, _ in cursor.executed)
    assert any("FROM fingerprint_index_metadata" in query for query, _ in cursor.executed)


@pytest.mark.parametrize(
    ("cursor", "message"),
    [
        (SchemaCursor(vector_type="vector(768)"), r"expected vector\(128\)"),
        (SchemaCursor(metadata=("old-model", "old-preprocess", 128)), "incompatible"),
    ],
)
def test_schema_readiness_rejects_incompatible_index(cursor: SchemaCursor, message: str) -> None:
    store = build_store(cursor)

    with pytest.raises(RuntimeError, match=message):
        store.ensure_schema_ready()


def test_complete_search_job_persists_lossless_result_payload_and_metrics() -> None:
    cursor = FakeCursor()
    store = build_store(cursor)
    metadata = SearchExecutionMetadata(
        preprocess_duration_ms=11,
        embed_duration_ms=22,
        vector_query_duration_ms=33,
        alignment_duration_ms=44,
        found_match=True,
        matched_video_id=7,
        matched_timestamp_seconds=100,
        score=0.91,
    )
    # Newer metadata fields are added by the production search-model wave. Set
    # them dynamically so this storage test remains compatible during rollout.
    metadata.model_version = DEFAULT_NMFP_MODEL_VERSION
    metadata.preprocessing_version = DEFAULT_NMFP_PREPROCESSING_VERSION
    metadata.model_startup_duration_ms = 1200
    metadata.fingerprint_duration_ms = 185
    metadata.query_fingerprint_count = 40
    metadata.candidate_count = 400
    metadata.segment_count = 2
    outcome = SearchRequestOutcome(
        result=SearchResult(
            found=True,
            video_id=7,
            timestamp_seconds=100,
            score=0.91,
            query_duration_seconds=20.0,
            segments=[
                SearchSegment(
                    query_start=0.0,
                    query_end=5.0,
                    video_id=7,
                    vod_start=100.0,
                    vod_end=105.0,
                    score=0.91,
                    ranking_score=8.0,
                    offset_seconds=100.0,
                    mean_similarity=0.94,
                    density=1.0,
                    supporting_fingerprints=10,
                    top_rank_fingerprints=8,
                ),
                SearchSegment(
                    query_start=10.0,
                    query_end=20.0,
                    video_id=8,
                    vod_start=500.0,
                    vod_end=510.0,
                    score=0.88,
                    ranking_score=12.0,
                    offset_seconds=490.0,
                    mean_similarity=0.90,
                    density=0.9,
                    supporting_fingerprints=18,
                    top_rank_fingerprints=12,
                ),
            ],
            unmatched_ranges=[UnmatchedRange(query_start=5.0, query_end=10.0)],
        ),
        execution_metadata=metadata,
        input_type="tiktok_url",
        total_duration_ms=2100,
    )

    store.complete_search_job(55, outcome)

    query, params = cursor.executed[0]
    assert "result_payload = %s::jsonb" in query
    assert "model_startup_duration_ms = %s" in query
    assert "fingerprint_duration_ms = %s" in query
    assert "query_fingerprint_count = %s" in query
    assert "candidate_count = %s" in query
    assert "segment_count = %s" in query
    payload = next(item for item in params if isinstance(item, str) and item.startswith("{"))
    assert json.loads(payload)["timestamp_seconds"] == 100
    assert [item["video_id"] for item in json.loads(payload)["segments"]] == [7, 8]
    assert json.loads(payload)["unmatched_ranges"] == [{"query_end": 10.0, "query_start": 5.0}]
    assert params[-11:] == (
        DEFAULT_NMFP_MODEL_VERSION,
        DEFAULT_NMFP_PREPROCESSING_VERSION,
        1200,
        None,
        None,
        None,
        185,
        40,
        400,
        2,
        55,
    )


def test_get_public_search_job_restores_nested_multi_segment_payload() -> None:
    payload = {
        "found": True,
        "streamer": "alice",
        "video_id": 7,
        "timestamp_seconds": 100,
        "score": 0.91,
        "segments": [
            {
                "query_start": 0.0,
                "query_end": 5.0,
                "video_id": 7,
                "vod_start": 100.0,
                "vod_end": 105.0,
                "score": 0.91,
                "ranking_score": 8.0,
                "offset_seconds": 100.0,
                "mean_similarity": 0.94,
                "density": 1.0,
                "supporting_fingerprints": 10,
                "top_rank_fingerprints": 8,
                "video_url_at_timestamp": "https://twitch.tv/videos/7?t=1m40s",
            }
        ],
        "unmatched_ranges": [{"query_start": 5.0, "query_end": 9.0}],
        "query_duration_seconds": 9.0,
    }
    cursor = FakeCursor(
        row=(
            55,
            "completed",
            None,
            datetime(2026, 8, 24, tzinfo=timezone.utc),
            datetime(2026, 8, 24, tzinfo=timezone.utc),
            datetime(2026, 8, 24, tzinfo=timezone.utc),
            True,
            "alice",
            7,
            100,
            0.91,
            None,
            None,
            None,
            payload,
        )
    )
    store = build_store(cursor)

    job = store.get_public_search_job(55)

    assert job is not None
    assert job.result is not None
    assert isinstance(job.result.segments[0], SearchSegment)
    assert job.result.segments[0].video_id == 7
    assert isinstance(job.result.unmatched_ranges[0], UnmatchedRange)
    assert job.result.unmatched_ranges[0].query_start == 5.0
