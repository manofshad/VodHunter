"""Prepare embedding writes for creator-partitioned storage.

The next schema migration will replace the global embedding table with a
LIST-partitioned parent keyed by ``creator_id``.  Keep this migration small and
online: repository writes can start using the future composite conflict key
before the physical table swap happens.
"""

from __future__ import annotations

from alembic import op


revision = "20260913_0013"
down_revision = "20260912_0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # fingerprint_id is currently globally unique, so every existing row is
    # already unique on (creator_id, fingerprint_id).  A concurrently-created
    # unique index makes the repository's future composite ON CONFLICT clause
    # valid while the table is still the current unpartitioned table.
    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS
                idx_fingerprint_embeddings_creator_fingerprint
            ON fingerprint_embeddings (creator_id, fingerprint_id)
            """
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS "
            "idx_fingerprint_embeddings_creator_fingerprint"
        )
