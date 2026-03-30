"""Add search request logging table.

Revision ID: 20260330_0005
Revises: 20260317_0004
Create Date: 2026-03-30 00:05:00
"""

from __future__ import annotations

from alembic import op


revision = "20260330_0005"
down_revision = "20260317_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS search_requests (
            id BIGSERIAL PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            source_app TEXT NOT NULL,
            route TEXT NOT NULL,
            input_type TEXT,
            streamer TEXT,
            success BOOLEAN NOT NULL DEFAULT FALSE,
            http_status INTEGER,
            error_code TEXT,
            error_message TEXT,
            result_reason TEXT,
            found_match BOOLEAN,
            matched_video_id BIGINT REFERENCES videos(id),
            matched_timestamp_seconds INTEGER,
            score DOUBLE PRECISION,
            clip_filename TEXT,
            download_source TEXT,
            download_host TEXT,
            input_duration_seconds DOUBLE PRECISION,
            total_duration_ms INTEGER,
            preprocess_duration_ms INTEGER,
            embed_duration_ms INTEGER,
            vector_query_duration_ms INTEGER,
            alignment_duration_ms INTEGER
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_search_requests_created_at
        ON search_requests(created_at DESC)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_search_requests_source_app_created_at
        ON search_requests(source_app, created_at DESC)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_search_requests_source_app_created_at")
    op.execute("DROP INDEX IF EXISTS idx_search_requests_created_at")
    op.execute("DROP TABLE IF EXISTS search_requests")
