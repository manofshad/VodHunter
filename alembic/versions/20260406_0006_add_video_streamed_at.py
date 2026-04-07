"""Add streamed_at to videos.

Revision ID: 20260406_0006
Revises: 20260330_0005
Create Date: 2026-04-06 00:00:00
"""

from __future__ import annotations

from alembic import op


revision = "20260406_0006"
down_revision = "20260330_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE videos ADD COLUMN IF NOT EXISTS streamed_at TIMESTAMPTZ")


def downgrade() -> None:
    op.execute("ALTER TABLE videos DROP COLUMN IF EXISTS streamed_at")
