"""Add stable, read-only reporting views for Grafana dashboards.

Revision ID: 20260913_0014
Revises: 20260913_0013
Create Date: 2026-09-13 00:14:00
"""

from __future__ import annotations

from alembic import op


revision = "20260913_0014"
down_revision = "20260913_0013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
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
        """
        CREATE OR REPLACE VIEW grafana_streamer_summary AS
        SELECT
            creator.id AS creator_id,
            LOWER(creator.name) AS streamer,
            creator.url AS streamer_url,
            creator.profile_image_url,
            COUNT(video.video_id)::BIGINT AS total_vods,
            COUNT(*) FILTER (WHERE video.status = 'searchable')::BIGINT
                AS searchable_vods,
            COUNT(*) FILTER (WHERE video.operational_status = 'in_progress')::BIGINT
                AS active_vods,
            COUNT(*) FILTER (WHERE video.operational_status = 'stalled')::BIGINT
                AS stalled_vods,
            COUNT(*) FILTER (WHERE video.status = 'reindex_requested')::BIGINT
                AS reindex_requested_vods,
            COUNT(*) FILTER (WHERE video.status = 'deleted')::BIGINT
                AS deleted_vods,
            MAX(video.streamed_at) AS newest_vod_at,
            MAX(video.last_ingest_update) AS last_ingest_update
        FROM creators AS creator
        LEFT JOIN grafana_vod_inventory AS video
          ON video.creator_id = creator.id
        GROUP BY
            creator.id,
            creator.name,
            creator.url,
            creator.profile_image_url
        """
    )
    op.execute(
        """
        CREATE OR REPLACE VIEW grafana_search_quality AS
        SELECT
            search_request.id AS search_id,
            search_request.created_at,
            search_request.started_at,
            search_request.finished_at,
            LOWER(search_request.streamer) AS streamer,
            search_request.job_status,
            search_request.job_stage,
            CASE
                WHEN search_request.job_status = 'failed' THEN 'error'
                WHEN search_request.job_status = 'completed'
                     AND search_request.found_match IS TRUE THEN 'match'
                WHEN search_request.job_status = 'completed' THEN 'no_match'
                ELSE search_request.job_status
            END AS outcome,
            search_request.found_match,
            search_request.result_reason,
            search_request.score,
            search_request.error_code,
            search_request.total_duration_ms,
            search_request.preprocess_duration_ms,
            search_request.embed_duration_ms,
            search_request.vector_query_duration_ms,
            search_request.alignment_duration_ms,
            search_request.model_startup_duration_ms,
            search_request.model_cold_start,
            search_request.fingerprint_preprocessing_duration_ms,
            search_request.fingerprint_inference_duration_ms,
            search_request.fingerprint_duration_ms,
            search_request.query_fingerprint_count,
            search_request.candidate_count,
            search_request.segment_count,
            search_request.model_version,
            search_request.preprocessing_version,
            search_request.streamed_from,
            search_request.streamed_to,
            matched_video.id AS matched_video_id,
            matched_video.title AS matched_video_title,
            matched_video.url AS matched_video_url,
            matched_video.streamed_at AS matched_video_streamed_at
        FROM search_requests AS search_request
        LEFT JOIN videos AS matched_video
          ON matched_video.id = search_request.matched_video_id
        WHERE search_request.source_app = 'public'
        """
    )
    op.execute(
        """
        CREATE OR REPLACE VIEW grafana_index_partitions AS
        SELECT
            creator.id AS creator_id,
            LOWER(creator.name) AS streamer,
            partition_relation.relname AS partition_name,
            COALESCE(GREATEST(partition_relation.reltuples, 0), 0)::BIGINT
                AS estimated_embeddings,
            COALESCE(pg_total_relation_size(partition_relation.oid), 0)::BIGINT
                AS total_bytes,
            EXISTS (
                SELECT 1
                FROM pg_indexes AS partition_index
                WHERE partition_index.schemaname = CURRENT_SCHEMA()
                  AND partition_index.tablename = partition_relation.relname
                  AND partition_index.indexdef ILIKE '%USING hnsw%'
            ) AS has_hnsw_index,
            index_metadata.model_version,
            index_metadata.preprocessing_version,
            index_metadata.embedding_dim,
            index_metadata.updated_at AS metadata_updated_at
        FROM creators AS creator
        CROSS JOIN fingerprint_index_metadata AS index_metadata
        LEFT JOIN pg_class AS partition_relation
          ON partition_relation.relname =
             'fingerprint_embeddings_creator_' || creator.id::TEXT
         AND partition_relation.relnamespace = (
             SELECT namespace.oid
             FROM pg_namespace AS namespace
             WHERE namespace.nspname = CURRENT_SCHEMA()
         )
        WHERE index_metadata.singleton = TRUE
        """
    )
    op.execute(
        """
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
            EXISTS (
                SELECT 1
                FROM vod_ingest_state AS ingest_state
                WHERE ingest_state.video_id = video.id
            ) AS has_active_cursor
        FROM videos AS video
        JOIN creators AS creator ON creator.id = video.creator_id
        WHERE video.streamed_at IS NOT NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS grafana_retention_inventory")
    op.execute("DROP VIEW IF EXISTS grafana_index_partitions")
    op.execute("DROP VIEW IF EXISTS grafana_search_quality")
    op.execute("DROP VIEW IF EXISTS grafana_streamer_summary")
    op.execute("DROP VIEW IF EXISTS grafana_vod_inventory")
