from alembic import op


revision = "20261011_0017"
down_revision = "20261005_0016"
branch_labels = None
depends_on = None


def _replace_inventory_views(*, include_unavailable: bool) -> None:
    unavailable_status = (
        "WHEN video.status = 'unavailable' THEN 'source_unavailable'"
        if include_unavailable else ""
    )
    active_cursor_condition = "video.status <> 'unavailable' AND " if include_unavailable else ""
    op.execute(
        f"""
        CREATE OR REPLACE VIEW grafana_vod_inventory AS
        SELECT
            video.id AS video_id,
            creator.id AS creator_id,
            LOWER(creator.name) AS streamer,
            creator.url AS streamer_url,
            creator.profile_image_url,
            COALESCE(
                ingest_state.vod_platform_id,
                SUBSTRING(video.url FROM '/videos/([^?]+)')
            ) AS vod_platform_id,
            video.url AS vod_url,
            video.title,
            video.thumbnail_url,
            video.streamed_at,
            video.status,
            CASE
                WHEN video.status = 'searchable' THEN 'complete'
                WHEN video.status = 'reindex_requested' THEN 'queued_reindex'
                WHEN video.status = 'deleted' THEN 'deleted'
                {unavailable_status}
                WHEN video.status = 'indexing'
                     AND ingest_state.updated_at >= NOW() - INTERVAL '10 minutes'
                    THEN 'in_progress'
                WHEN video.status = 'indexing' THEN 'stalled'
                ELSE 'unknown'
            END AS operational_status,
            ingest_state.last_ingested_seconds,
            ingest_state.last_seen_duration_seconds,
            CASE
                WHEN video.status = 'searchable' THEN 100.0
                WHEN ingest_state.last_seen_duration_seconds > 0 THEN
                    LEAST(
                        100.0,
                        100.0 * ingest_state.last_ingested_seconds
                            / ingest_state.last_seen_duration_seconds
                    )
                ELSE 0.0
            END AS progress_percent,
            ingest_state.updated_at AS last_ingest_update,
            CASE
                WHEN ingest_state.updated_at IS NULL THEN NULL
                ELSE EXTRACT(EPOCH FROM (NOW() - ingest_state.updated_at))::BIGINT
            END AS cursor_age_seconds
        FROM videos AS video
        JOIN creators AS creator ON creator.id = video.creator_id
        LEFT JOIN vod_ingest_state AS ingest_state
          ON ingest_state.video_id = video.id
        """
    )
    op.execute(
        f"""
        CREATE OR REPLACE VIEW grafana_retention_inventory AS
        SELECT
            video.id AS video_id,
            LOWER(creator.name) AS streamer,
            video.title,
            video.url AS vod_url,
            video.streamed_at,
            video.status,
            EXTRACT(EPOCH FROM (NOW() - video.streamed_at)) / 86400.0
                AS age_days,
            {active_cursor_condition}EXISTS (
                SELECT 1
                FROM vod_ingest_state AS ingest_state
                WHERE ingest_state.video_id = video.id
            ) AS has_active_cursor
        FROM videos AS video
        JOIN creators AS creator ON creator.id = video.creator_id
        WHERE video.streamed_at IS NOT NULL
        """
    )


def upgrade() -> None:
    op.execute("ALTER TABLE videos DROP CONSTRAINT videos_status_check")
    op.execute(
        """
        ALTER TABLE videos ADD CONSTRAINT videos_status_check
        CHECK (status IN ('indexing', 'searchable', 'deleted', 'reindex_requested', 'unavailable'))
        """
    )
    _replace_inventory_views(include_unavailable=True)


def downgrade() -> None:
    op.execute("UPDATE videos SET status = 'indexing', processed = FALSE WHERE status = 'unavailable'")
    _replace_inventory_views(include_unavailable=False)
    op.execute("ALTER TABLE videos DROP CONSTRAINT videos_status_check")
    op.execute(
        """
        ALTER TABLE videos ADD CONSTRAINT videos_status_check
        CHECK (status IN ('indexing', 'searchable', 'deleted', 'reindex_requested'))
        """
    )
