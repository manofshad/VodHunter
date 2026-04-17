"""Add async job fields to search requests.

Revision ID: 20260415_0008
Revises: 20260408_0007
Create Date: 2026-04-15 00:08:00
"""

from __future__ import annotations

from alembic import op


revision = "20260415_0008"
down_revision = "20260408_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE search_requests
        ADD COLUMN IF NOT EXISTS job_status TEXT NOT NULL DEFAULT 'completed',
        ADD COLUMN IF NOT EXISTS job_stage TEXT,
        ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS tiktok_url TEXT
        """
    )
    op.execute(
        """
        UPDATE search_requests
        SET job_status = CASE WHEN success THEN 'completed' ELSE 'failed' END
        """
    )
    op.execute(
        """
        UPDATE search_requests
        SET finished_at = COALESCE(finished_at, created_at)
        WHERE job_status IN ('completed', 'failed')
          AND finished_at IS NULL
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_search_requests_source_app_job_status_created_at
        ON search_requests(source_app, job_status, created_at DESC)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_search_requests_source_app_job_status_created_at")
    op.execute(
        """
        ALTER TABLE search_requests
        DROP COLUMN IF EXISTS tiktok_url,
        DROP COLUMN IF EXISTS finished_at,
        DROP COLUMN IF EXISTS started_at,
        DROP COLUMN IF EXISTS job_stage,
        DROP COLUMN IF EXISTS job_status
        """
    )
