"""Add creator profile image metadata.

Revision ID: 20260317_0004
Revises: 20260310_0003
Create Date: 2026-03-17 00:04:00
"""

from __future__ import annotations

from alembic import op


revision = "20260317_0004"
down_revision = "20260310_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE creators
        ADD COLUMN IF NOT EXISTS profile_image_url TEXT
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE creators
        DROP COLUMN IF EXISTS profile_image_url
        """
    )
