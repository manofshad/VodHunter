from __future__ import annotations

from datetime import datetime, timedelta, timezone

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
