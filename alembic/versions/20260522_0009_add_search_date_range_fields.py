"""Add search date range fields and streamed_at index.

Revision ID: 20260522_0009
Revises: 20260419_0008
Create Date: 2026-05-22 00:09:00
"""

from __future__ import annotations

from alembic import op


revision = "20260522_0009"
down_revision = "20260419_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE search_requests
        ADD COLUMN IF NOT EXISTS streamed_from TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS streamed_to TIMESTAMPTZ
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_videos_creator_streamed_at
        ON videos(creator_id, streamed_at)
        WHERE streamed_at IS NOT NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_videos_creator_streamed_at")
    op.execute(
        """
        ALTER TABLE search_requests
        DROP COLUMN IF EXISTS streamed_to,
        DROP COLUMN IF EXISTS streamed_from
        """
    )
