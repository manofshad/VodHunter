"""Enable the PostgreSQL cache-prewarming extension."""

from __future__ import annotations

from alembic import op


revision = "20260912_0012"
down_revision = "20260903_0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_prewarm")


def downgrade() -> None:
    op.execute("DROP EXTENSION IF EXISTS pg_prewarm")
