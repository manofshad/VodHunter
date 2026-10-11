from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest


@pytest.mark.integration
def test_grafana_inventory_views_report_active_ingest_and_partition(
    store,
    database_scope,
) -> None:
    creator_id = database_scope.remember_creator(
        store.videos.create_or_get_creator(
            database_scope.streamer,
            database_scope.creator_url,
            profile_image_url="https://cdn.example/avatar.png",
        )
    )
    partition_name = store.embedding_partitions.ensure_creator_partition(creator_id)
    video_id = database_scope.remember_video(
        store.videos.create_video(
            creator_id=creator_id,
            url="https://www.twitch.tv/videos/987654321",
            title="Reporting view integration VOD",
            thumbnail_url="https://cdn.example/vod.png",
            processed=False,
            streamed_at=datetime.now(timezone.utc) - timedelta(days=2),
            status="indexing",
        )
    )
    store.ingest_states.upsert(
        vod_platform_id="987654321",
        video_id=video_id,
        streamer=database_scope.streamer,
        last_ingested_seconds=30,
        last_seen_duration_seconds=120,
    )

    with psycopg.connect(database_scope.database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT operational_status, progress_percent, vod_platform_id
                FROM grafana_vod_inventory
                WHERE video_id = %s
                """,
                (video_id,),
            )
            operational_status, progress_percent, vod_platform_id = cursor.fetchone()

            cursor.execute(
                """
                SELECT total_vods, active_vods, stalled_vods
                FROM grafana_streamer_summary
                WHERE creator_id = %s
                """,
                (creator_id,),
            )
            total_vods, active_vods, stalled_vods = cursor.fetchone()

            cursor.execute(
                """
                SELECT partition_name, has_hnsw_index
                FROM grafana_index_partitions
                WHERE creator_id = %s
                """,
                (creator_id,),
            )
            reported_partition, has_hnsw_index = cursor.fetchone()

    assert operational_status == "in_progress"
    assert float(progress_percent) == 25.0
    assert vod_platform_id == "987654321"
    assert (total_vods, active_vods, stalled_vods) == (1, 1, 0)
    assert reported_partition == partition_name
    assert has_hnsw_index is True


@pytest.mark.integration
def test_unavailable_migration_downgrade_restores_views_without_erasing_cursor(store, database_scope, monkeypatch):
    creator_id = database_scope.remember_creator(
        store.videos.create_or_get_creator(database_scope.streamer, database_scope.creator_url)
    )
    vod_id = str(int(database_scope.token, 16))
    video_id = database_scope.remember_video(store.videos.create_video(
        creator_id=creator_id, url=f"https://www.twitch.tv/videos/{vod_id}",
        title="Unavailable migration", processed=True, status="unavailable",
        streamed_at=datetime.now(timezone.utc) - timedelta(days=2),
    ))
    store.ingest_states.upsert(
        vod_platform_id=vod_id, video_id=video_id, streamer=database_scope.streamer,
        last_ingested_seconds=22648, last_seen_duration_seconds=22768,
    )
    expected = store.ingest_states.get(vod_id)
    migration_path = Path(__file__).resolve().parents[2] / "alembic/versions/20261011_0017_add_unavailable_video_status.py"
    spec = importlib.util.spec_from_file_location("unavailable_migration", migration_path)
    revision = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(revision)

    with psycopg.connect(database_scope.database_url) as connection:
        try:
            monkeypatch.setattr(revision, "op", SimpleNamespace(execute=connection.execute))
            revision.downgrade()
            assert connection.execute(
                "SELECT status, processed FROM videos WHERE id = %s", (video_id,),
            ).fetchone() == ("indexing", False)
            assert connection.execute(
                "SELECT last_ingested_seconds, last_seen_duration_seconds, updated_at FROM vod_ingest_state WHERE video_id = %s",
                (video_id,),
            ).fetchone() == (22648, 22768, expected.updated_at)
            assert connection.execute(
                "SELECT operational_status FROM grafana_vod_inventory WHERE video_id = %s", (video_id,),
            ).fetchone() == ("in_progress",)
            assert connection.execute(
                "SELECT has_active_cursor FROM grafana_retention_inventory WHERE video_id = %s", (video_id,),
            ).fetchone() == (True,)
            with pytest.raises(psycopg.errors.CheckViolation):
                with connection.transaction():
                    connection.execute("UPDATE videos SET status = 'unavailable' WHERE id = %s", (video_id,))

            revision.upgrade()
            connection.execute("UPDATE videos SET status = 'unavailable', processed = TRUE WHERE id = %s", (video_id,))
            assert connection.execute(
                "SELECT operational_status FROM grafana_vod_inventory WHERE video_id = %s", (video_id,),
            ).fetchone() == ("source_unavailable",)
        finally:
            connection.rollback()
