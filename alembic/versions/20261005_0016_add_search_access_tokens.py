"""Require unguessable capabilities for public search reads.

Revision ID: 20261005_0016
Revises: 20260923_0015

Existing searches deliberately receive no token: their numbered URLs must not
remain an access path. Their records remain available for internal reporting.
"""

from alembic import op


revision = "20261005_0016"
down_revision = "20260923_0015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE search_requests ADD COLUMN IF NOT EXISTS access_token_hash TEXT")
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_search_requests_access_token_hash
        ON search_requests(access_token_hash)
        WHERE access_token_hash IS NOT NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_search_requests_access_token_hash")
    op.execute("ALTER TABLE search_requests DROP COLUMN IF EXISTS access_token_hash")
