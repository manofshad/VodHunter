"""Drop legacy IVFFLAT pgvector index.

Revision ID: 20260310_0003
Revises: 20260310_0002
Create Date: 2026-03-10 00:03:00
"""

from __future__ import annotations

from alembic import op


revision = "20260310_0003"
down_revision = "20260310_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_fingerprint_embeddings_ivfflat_cos")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fingerprint_embeddings_ivfflat_cos
            ON fingerprint_embeddings USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = 100)
            """
        )
