"""Add creator_id to search request logs.

Revision ID: 20260408_0007
Revises: 20260406_0006
Create Date: 2026-04-08 00:07:00
"""

from __future__ import annotations

from alembic import op


revision = "20260408_0007"
down_revision = "20260406_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE search_requests
        ADD COLUMN IF NOT EXISTS creator_id BIGINT REFERENCES creators(id)
        """
    )
    op.execute(
        """
        UPDATE search_requests AS sr
        SET creator_id = c.id
        FROM creators AS c
        WHERE sr.creator_id IS NULL
          AND sr.streamer IS NOT NULL
          AND LOWER(sr.streamer) = LOWER(c.name)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_search_requests_creator_id_created_at
        ON search_requests(creator_id, created_at DESC)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_search_requests_creator_id_created_at")
    op.execute("ALTER TABLE search_requests DROP COLUMN IF EXISTS creator_id")
