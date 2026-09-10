from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pytest

from search.models import (
    SearchExecutionMetadata,
    SearchRequestOutcome,
    SearchResult,
    SearchSegment,
    SearchSource,
)
from storage.vector_store import (
    DEFAULT_NMFP_MODEL_VERSION,
    DEFAULT_NMFP_PREPROCESSING_VERSION,
    NMFP_VECTOR_DIM,
)


def _create_creator(store, scope, *, suffix: str = "") -> int:
    name = f"{scope.streamer}{suffix}"
    creator_id = store.create_or_get_creator(
        name=name,
        url=f"https://www.twitch.tv/{name}",
        profile_image_url=f"https://cdn.example/{name}.png",
    )
    return scope.remember_creator(creator_id)


def _create_video(store, scope, creator_id: int, *, suffix: str = "") -> int:
    video_id = store.create_video(
        creator_id=creator_id,
        url=f"https://www.twitch.tv/videos/{scope.token}{suffix}",
        title=f"Integration VOD {suffix or 'primary'}",
        processed=True,
        streamed_at=datetime(2026, 9, 9, 12, 0, 0, tzinfo=timezone.utc),
        status="searchable",
    )
    return scope.remember_video(video_id)


@pytest.mark.integration
def test_migrated_schema_accepts_real_nmfp_vector_round_trip(store, database_scope) -> None:
    creator_id = _create_creator(store, database_scope)
    other_creator_id = _create_creator(store, database_scope, suffix="-other")
    video_id = _create_video(store, database_scope, creator_id)
    other_video_id = _create_video(store, database_scope, other_creator_id, suffix="-other")

    timestamps = np.array([10.0, 10.5], dtype=np.float32)
    embeddings = np.zeros((2, NMFP_VECTOR_DIM), dtype=np.float32)
    embeddings[:, 0] = 1.0
    fingerprint_ids = store.store_fingerprints(video_id, timestamps)
    store.append_vectors(embeddings, fingerprint_ids, creator_id=creator_id)

    other_fingerprint_ids = store.store_fingerprints(other_video_id, timestamps)
    store.append_vectors(embeddings, other_fingerprint_ids, creator_id=other_creator_id)

    candidates = store.query_fingerprint_candidates(
        query_embeddings=embeddings[:1],
        query_timestamps=np.array([0.0], dtype=np.float32),
        top_k=10,
        creator_id=creator_id,
        model_version=DEFAULT_NMFP_MODEL_VERSION,
        preprocessing_version=DEFAULT_NMFP_PREPROCESSING_VERSION,
    )

    assert len(candidates) == 2
    assert {candidate.video_id for candidate in candidates} == {video_id}
    assert {candidate.fingerprint_id for candidate in candidates} == set(fingerprint_ids)
    assert all(candidate.similarity > 0.99 for candidate in candidates)

    streamers = store.list_searchable_streamers()
    assert {item["name"] for item in streamers} >= {database_scope.streamer, f"{database_scope.streamer}-other"}


@pytest.mark.integration
def test_search_job_payload_survives_real_database_round_trip(store, database_scope) -> None:
    creator_id = _create_creator(store, database_scope)
    video_id = _create_video(store, database_scope, creator_id)
    search_id = database_scope.remember_search(
        store.create_public_search_job(
            tiktok_url="https://www.tiktok.com/@integration/video/123456789",
            streamer=database_scope.streamer,
            creator_id=creator_id,
        )
    )

    queued = store.get_public_search_job(search_id)
    assert queued is not None
    assert queued.status == "queued"
    assert queued.result is None

    store.update_search_job_status(
        search_id,
        status="running",
        stage="fingerprinting",
        started=True,
    )

    segment = SearchSegment(
        query_start=0.0,
        query_end=5.0,
        video_id=video_id,
        vod_start=100.0,
        vod_end=105.0,
        score=0.92,
        ranking_score=8.0,
        offset_seconds=100.0,
        mean_similarity=0.94,
        density=1.0,
        supporting_fingerprints=10,
        top_rank_fingerprints=8,
    )
    result = SearchResult(
        found=True,
        streamer=database_scope.streamer,
        video_id=video_id,
        timestamp_seconds=100,
        score=0.92,
        reason="Aligned NMFP evidence",
        segments=[segment],
        sources=[
            SearchSource(
                video_id=video_id,
                video_url=f"https://www.twitch.tv/videos/{database_scope.token}",
                title="Integration VOD primary",
                streamer=database_scope.streamer,
                segments=[segment],
            )
        ],
        query_duration_seconds=5.0,
    )
    outcome = SearchRequestOutcome(
        result=result,
        execution_metadata=SearchExecutionMetadata(
            found_match=True,
            matched_video_id=video_id,
            matched_timestamp_seconds=100,
            score=0.92,
            segment_count=1,
            model_version=DEFAULT_NMFP_MODEL_VERSION,
            preprocessing_version=DEFAULT_NMFP_PREPROCESSING_VERSION,
        ),
        input_type="tiktok_url",
        total_duration_ms=1234,
    )

    store.complete_search_job(search_id, outcome)

    completed = store.get_public_search_job(search_id)
    assert completed is not None
    assert completed.status == "completed"
    assert completed.stage is None
    assert completed.started_at is not None
    assert completed.finished_at is not None
    assert completed.result is not None
    assert completed.result.found is True
    assert completed.result.video_id == video_id
    assert completed.result.timestamp_seconds == 100
    assert completed.result.score == pytest.approx(0.92)
    assert completed.result.reason == "Aligned NMFP evidence"
    assert completed.result.segments[0].query_end == 5.0
    assert completed.result.sources[0].segments[0].vod_start == 100.0
