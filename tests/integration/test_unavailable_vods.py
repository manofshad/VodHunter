from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from services.vod_retention import purge_expired_vods
from storage.errors import VideoOwnerMismatchError
from storage.fingerprint_repository import NMFP_VECTOR_DIM
from storage.records import VideoStatus


pytestmark = pytest.mark.integration


def _create_partial_vod(store, scope, *, days_old=2):
    creator_id = scope.remember_creator(
        store.videos.create_or_get_creator(scope.streamer, scope.creator_url)
    )
    store.embedding_partitions.ensure_creator_partition(creator_id)
    vod_id = str(int(scope.token, 16))
    video_id = scope.remember_video(store.videos.create_video(
        creator_id=creator_id, url=f"https://www.twitch.tv/videos/{vod_id}",
        title="Partial archive", processed=False, status="indexing",
        streamed_at=datetime.now(timezone.utc) - timedelta(days=days_old),
    ))
    store.ingest_states.upsert(
        vod_platform_id=vod_id, video_id=video_id, streamer=scope.streamer,
        last_ingested_seconds=22648, last_seen_duration_seconds=22768,
    )
    with store.database.connect() as connection:
        connection.execute(
            "UPDATE vod_ingest_state SET updated_at = %s WHERE vod_platform_id = %s",
            (datetime.now(timezone.utc) - timedelta(hours=1), vod_id),
        )
    return creator_id, video_id, store.ingest_states.get(vod_id)


def test_unavailable_transition_preserves_coverage_and_index_but_removes_search(store, database_scope):
    creator_id, video_id, expected = _create_partial_vod(store, database_scope)
    embeddings = np.zeros((2, NMFP_VECTOR_DIM), dtype=np.float32)
    embeddings[:, 0] = 1.0
    fingerprint_ids = store.fingerprints.store_fingerprints(
        video_id, np.array([10.0, 10.5], dtype=np.float32),
    )
    store.fingerprints.append_vectors(embeddings, fingerprint_ids, creator_id=creator_id)

    def candidates():
        return store.fingerprints.query_fingerprint_candidates(
            query_embeddings=embeddings[:1], query_timestamps=np.array([0.0], dtype=np.float32),
            top_k=10, creator_id=creator_id,
        )

    assert {item.fingerprint_id for item in candidates()} == set(fingerprint_ids)
    assert database_scope.streamer in {item.name for item in store.videos.list_searchable_streamers()}
    assert store.ingest_states.list_pending(database_scope.streamer) == [expected]

    assert store.videos.mark_source_unavailable(
        expected, stale_before=datetime.now(timezone.utc) - timedelta(minutes=10),
    ) is True
    assert store.videos.mark_source_unavailable(
        expected, stale_before=datetime.now(timezone.utc) - timedelta(minutes=10),
    ) is False

    assert store.ingest_states.get(expected.vod_platform_id) == expected
    assert store.ingest_states.list_pending(database_scope.streamer) == []
    assert store.videos.get_video_status(video_id) is VideoStatus.UNAVAILABLE
    assert candidates() == []
    assert database_scope.streamer not in {item.name for item in store.videos.list_searchable_streamers()}

    with store.database.connect() as connection:
        assert connection.execute(
            "SELECT id, timestamp_seconds FROM fingerprints WHERE video_id = %s ORDER BY id", (video_id,),
        ).fetchall() == [(fingerprint_ids[0], 10.0), (fingerprint_ids[1], 10.5)]
        assert connection.execute(
            "SELECT COUNT(*) FROM fingerprint_embeddings WHERE creator_id = %s", (creator_id,),
        ).fetchone() == (2,)
        status, progress, cursor, duration, updated_at = connection.execute(
            """
            SELECT operational_status, progress_percent, last_ingested_seconds,
                   last_seen_duration_seconds, last_ingest_update
            FROM grafana_vod_inventory WHERE video_id = %s
            """, (video_id,),
        ).fetchone()
        assert status == "source_unavailable"
        assert float(progress) == pytest.approx(99.47294448)
        assert (cursor, duration, updated_at) == (22648, 22768, expected.updated_at)
        assert connection.execute(
            """
            SELECT total_vods, searchable_vods, active_vods, stalled_vods
            FROM grafana_streamer_summary WHERE creator_id = %s
            """, (creator_id,),
        ).fetchone() == (1, 0, 0, 0)
        assert connection.execute(
            "SELECT has_active_cursor FROM grafana_retention_inventory WHERE video_id = %s", (video_id,),
        ).fetchone() == (False,)

    with pytest.raises(VideoOwnerMismatchError):
        store.videos.delete_video_index(video_id, creator_id + 1000000)
    assert store.videos.get_video_status(video_id) is VideoStatus.UNAVAILABLE
    assert store.ingest_states.get(expected.vod_platform_id) == expected
    assert store.videos.delete_video_index(video_id, creator_id) is VideoStatus.DELETED
    assert store.ingest_states.get(expected.vod_platform_id) is None
    with store.database.connect() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM fingerprints WHERE video_id = %s", (video_id,),
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT COUNT(*) FROM fingerprint_embeddings WHERE creator_id = %s", (creator_id,),
        ).fetchone() == (0,)
    assert store.videos.request_video_reindex(video_id, creator_id) is VideoStatus.REINDEX_REQUESTED


@pytest.mark.parametrize("change", ["recent", "boundary", "cursor", "duration", "timestamp", "status", "url", "owner", "missing"])
def test_unavailable_transition_rejects_recent_or_changed_snapshot(store, database_scope, change):
    creator_id, video_id, expected = _create_partial_vod(store, database_scope)
    with store.database.connect() as connection:
        if change == "recent":
            connection.execute("UPDATE vod_ingest_state SET updated_at = NOW() WHERE video_id = %s", (video_id,))
        elif change == "cursor":
            connection.execute("UPDATE vod_ingest_state SET last_ingested_seconds = 22708 WHERE video_id = %s", (video_id,))
        elif change == "duration":
            connection.execute("UPDATE vod_ingest_state SET last_seen_duration_seconds = 23000 WHERE video_id = %s", (video_id,))
        elif change == "timestamp":
            connection.execute("UPDATE vod_ingest_state SET updated_at = updated_at + INTERVAL '1 second' WHERE video_id = %s", (video_id,))
        elif change == "status":
            connection.execute("UPDATE videos SET status = 'searchable', processed = TRUE WHERE id = %s", (video_id,))
        elif change == "url":
            connection.execute("UPDATE videos SET url = url || '-changed' WHERE id = %s", (video_id,))
        elif change == "owner":
            connection.execute("UPDATE creators SET name = name || '-changed' WHERE id = %s", (creator_id,))
        elif change == "missing":
            connection.execute("DELETE FROM vod_ingest_state WHERE video_id = %s", (video_id,))

    current_state = store.ingest_states.get(expected.vod_platform_id)
    current_video = store.videos.get_video_with_creator(video_id)
    if change == "recent":
        expected = current_state
    assert store.videos.mark_source_unavailable(
        expected, stale_before=expected.updated_at if change == "boundary" else datetime.now(timezone.utc) - timedelta(minutes=10),
    ) is False
    assert store.ingest_states.get(expected.vod_platform_id) == current_state
    assert store.videos.get_video_with_creator(video_id) == current_video


def test_expired_unavailable_cursor_is_purgeable_while_active_and_unexpired_rows_survive(store, database_scope):
    creator_id, video_id, expected = _create_partial_vod(store, database_scope, days_old=9001)
    assert store.videos.mark_source_unavailable(expected, stale_before=datetime.now(timezone.utc)) is True
    fingerprint_ids = store.fingerprints.store_fingerprints(video_id, np.array([10.0], dtype=np.float32))
    embeddings = np.zeros((1, NMFP_VECTOR_DIM), dtype=np.float32)
    embeddings[:, 0] = 1.0
    store.fingerprints.append_vectors(embeddings, fingerprint_ids, creator_id=creator_id)
    protected = []
    for suffix, status, days_old in [
        ("active", "indexing", 9001), ("cursor", "searchable", 9001),
        ("recent", "unavailable", 2), ("undated", "unavailable", None),
    ]:
        url = f"https://www.twitch.tv/videos/{database_scope.token}-{suffix}"
        row_id = database_scope.remember_video(store.videos.create_video(
            creator_id=creator_id, url=url, title=suffix, status=status, processed=status != "indexing",
            streamed_at=datetime.now(timezone.utc) - timedelta(days=days_old) if days_old else None,
        ))
        protected.append(row_id)
        store.ingest_states.upsert(
            vod_platform_id=f"{database_scope.token}-{suffix}", video_id=row_id,
            streamer=database_scope.streamer, last_ingested_seconds=60, last_seen_duration_seconds=120,
        )

    result = purge_expired_vods(database_scope.database_url, 9000)

    assert video_id in result.deleted_video_ids
    assert set(protected).isdisjoint(result.deleted_video_ids)
    assert store.videos.get_video_with_creator(video_id) is None
    assert store.ingest_states.get(expected.vod_platform_id) is None
    assert [store.videos.get_video_status(row_id) for row_id in protected] == [
        VideoStatus.INDEXING, VideoStatus.SEARCHABLE, VideoStatus.UNAVAILABLE, VideoStatus.UNAVAILABLE,
    ]
    with store.database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM fingerprints WHERE video_id = %s", (video_id,)).fetchone() == (0,)
        assert connection.execute("SELECT COUNT(*) FROM fingerprint_embeddings WHERE creator_id = %s", (creator_id,)).fetchone() == (0,)
