"""Remove the obsolete unpartitioned embedding rollback copy.

Revision ID: 20260923_0015
Revises: 20260913_0014

The active creator partitions contain the serving data. Fingerprints can be
recreated from source VODs if the old layout is ever needed again.
"""

from __future__ import annotations

from alembic import op


revision = "20260923_0015"
down_revision = "20260913_0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_class
                WHERE oid = 'fingerprint_embeddings'::regclass
                  AND relkind = 'p'
            ) THEN
                RAISE EXCEPTION 'Active fingerprint embeddings are not partitioned';
            END IF;
        END $$
        """
    )
    # RESTRICT is the default: fail if anything unexpectedly depends on the copy.
    op.execute("DROP TABLE IF EXISTS fingerprint_embeddings_unpartitioned_backup")


def downgrade() -> None:
    raise RuntimeError(
        "The unpartitioned backup was removed; restore a database backup or "
        "rebuild fingerprints to return to the old layout"
    )
