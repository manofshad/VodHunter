"""Add video status lifecycle fields.

Revision ID: 20260419_0008
Revises: 20260415_0008
Create Date: 2026-04-19 00:08:00
"""

from __future__ import annotations

from alembic import op


revision = "20260419_0008"
down_revision = "20260415_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE videos ADD COLUMN IF NOT EXISTS status TEXT")
    op.execute(
        """
        UPDATE videos
        SET status = CASE
            WHEN processed THEN 'searchable'
            ELSE 'indexing'
        END
        WHERE status IS NULL
        """
    )
    op.execute(
        """
        ALTER TABLE videos
        ALTER COLUMN status SET NOT NULL
        """
    )
    op.execute("ALTER TABLE videos DROP CONSTRAINT IF EXISTS videos_status_check")
    op.execute(
        """
        ALTER TABLE videos
        ADD CONSTRAINT videos_status_check
        CHECK (status IN ('indexing', 'searchable', 'deleted', 'reindex_requested'))
        """
    )


def downgrade() -> None:
    op.execute("ALTER TABLE videos DROP CONSTRAINT IF EXISTS videos_status_check")
    op.execute("ALTER TABLE videos DROP COLUMN IF EXISTS status")
