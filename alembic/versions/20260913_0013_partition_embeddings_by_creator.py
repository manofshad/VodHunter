"""Partition fingerprint embeddings by creator.

Revision ID: 20260913_0013
Revises: 20260912_0012
Create Date: 2026-09-13 00:13:00

The legacy table is retained as a rollback copy. Production operators may
remove it in a later maintenance revision after the partitioned layout soaks.
"""

from __future__ import annotations

from alembic import op


revision = "20260913_0013"
down_revision = "20260912_0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("LOCK TABLE fingerprint_embeddings IN ACCESS EXCLUSIVE MODE")
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM fingerprint_embeddings WHERE creator_id IS NULL
            ) THEN
                RAISE EXCEPTION
                    'Cannot partition fingerprint_embeddings: creator_id contains NULL';
            END IF;

            IF EXISTS (
                SELECT 1
                FROM fingerprint_embeddings AS embedding
                JOIN fingerprints AS fingerprint
                  ON fingerprint.id = embedding.fingerprint_id
                JOIN videos AS video
                  ON video.id = fingerprint.video_id
                WHERE embedding.creator_id <> video.creator_id
            ) THEN
                RAISE EXCEPTION
                    'Cannot partition fingerprint_embeddings: creator ownership mismatch';
            END IF;
        END $$
        """
    )

    op.execute(
        "ALTER TABLE fingerprint_embeddings "
        "RENAME TO fingerprint_embeddings_unpartitioned_backup"
    )
    op.execute(
        "ALTER TABLE fingerprint_embeddings_unpartitioned_backup "
        "RENAME CONSTRAINT fingerprint_embeddings_pkey "
        "TO fingerprint_embeddings_unpartitioned_backup_pkey"
    )
    for old_name, backup_name in (
        ("idx_fingerprint_embeddings_creator_id", "idx_fingerprint_embeddings_creator_id_backup"),
        ("idx_fingerprint_embeddings_model_versions", "idx_fingerprint_embeddings_model_versions_backup"),
        ("idx_fingerprint_embeddings_hnsw_cos", "idx_fingerprint_embeddings_hnsw_cos_backup"),
    ):
        op.execute(f"ALTER INDEX IF EXISTS {old_name} RENAME TO {backup_name}")

    op.execute(
        """
        CREATE TABLE fingerprint_embeddings (
            fingerprint_id BIGINT NOT NULL
                REFERENCES fingerprints(id) ON DELETE CASCADE,
            embedding vector(128) NOT NULL,
            creator_id BIGINT NOT NULL REFERENCES creators(id),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            model_version TEXT NOT NULL,
            preprocessing_version TEXT NOT NULL,
            PRIMARY KEY (creator_id, fingerprint_id)
        ) PARTITION BY LIST (creator_id)
        """
    )

    op.execute(
        """
        DO $$
        DECLARE
            creator_record RECORD;
            partition_name TEXT;
        BEGIN
            FOR creator_record IN
                SELECT DISTINCT creator_id
                FROM fingerprint_embeddings_unpartitioned_backup
                ORDER BY creator_id
            LOOP
                partition_name := format(
                    'fingerprint_embeddings_creator_%s',
                    creator_record.creator_id
                );
                EXECUTE format(
                    'CREATE TABLE %I PARTITION OF fingerprint_embeddings '
                    'FOR VALUES IN (%L)',
                    partition_name,
                    creator_record.creator_id
                );
            END LOOP;
        END $$
        """
    )

    op.execute(
        """
        INSERT INTO fingerprint_embeddings (
            fingerprint_id,
            embedding,
            creator_id,
            created_at,
            model_version,
            preprocessing_version
        )
        SELECT
            fingerprint_id,
            embedding,
            creator_id,
            created_at,
            model_version,
            preprocessing_version
        FROM fingerprint_embeddings_unpartitioned_backup
        """
    )

    op.execute(
        """
        DO $$
        DECLARE
            creator_record RECORD;
            partition_name TEXT;
            index_name TEXT;
        BEGIN
            FOR creator_record IN
                SELECT DISTINCT creator_id
                FROM fingerprint_embeddings_unpartitioned_backup
                ORDER BY creator_id
            LOOP
                partition_name := format(
                    'fingerprint_embeddings_creator_%s',
                    creator_record.creator_id
                );
                index_name := format('%s_hnsw_cos', partition_name);
                EXECUTE format(
                    'CREATE INDEX %I ON %I USING hnsw '
                    '(embedding vector_cosine_ops)',
                    index_name,
                    partition_name
                );
            END LOOP;
        END $$
        """
    )
    op.execute(
        """
        CREATE INDEX idx_fingerprint_embeddings_creator_id
        ON fingerprint_embeddings (creator_id)
        """
    )
    op.execute(
        """
        CREATE INDEX idx_fingerprint_embeddings_model_versions
        ON fingerprint_embeddings (
            creator_id,
            model_version,
            preprocessing_version
        )
        """
    )
    op.execute("ANALYZE fingerprint_embeddings")
    op.execute(
        """
        DO $$
        DECLARE
            source_count BIGINT;
            target_count BIGINT;
        BEGIN
            SELECT COUNT(*) INTO source_count
            FROM fingerprint_embeddings_unpartitioned_backup;
            SELECT COUNT(*) INTO target_count
            FROM fingerprint_embeddings;

            IF source_count <> target_count THEN
                RAISE EXCEPTION
                    'Partitioned embedding copy mismatch: source %, target %',
                    source_count,
                    target_count;
            END IF;
        END $$
        """
    )


def downgrade() -> None:
    op.execute("LOCK TABLE fingerprint_embeddings IN ACCESS EXCLUSIVE MODE")
    op.execute(
        """
        INSERT INTO fingerprint_embeddings_unpartitioned_backup (
            fingerprint_id,
            embedding,
            creator_id,
            created_at,
            model_version,
            preprocessing_version
        )
        SELECT
            fingerprint_id,
            embedding,
            creator_id,
            created_at,
            model_version,
            preprocessing_version
        FROM fingerprint_embeddings
        ON CONFLICT (fingerprint_id) DO UPDATE
        SET embedding = excluded.embedding,
            creator_id = excluded.creator_id,
            created_at = excluded.created_at,
            model_version = excluded.model_version,
            preprocessing_version = excluded.preprocessing_version
        """
    )
    op.execute("DROP TABLE fingerprint_embeddings CASCADE")
    op.execute(
        "ALTER TABLE fingerprint_embeddings_unpartitioned_backup "
        "RENAME TO fingerprint_embeddings"
    )
    op.execute(
        "ALTER TABLE fingerprint_embeddings "
        "RENAME CONSTRAINT fingerprint_embeddings_unpartitioned_backup_pkey "
        "TO fingerprint_embeddings_pkey"
    )
    for backup_name, restored_name in (
        ("idx_fingerprint_embeddings_creator_id_backup", "idx_fingerprint_embeddings_creator_id"),
        ("idx_fingerprint_embeddings_model_versions_backup", "idx_fingerprint_embeddings_model_versions"),
        ("idx_fingerprint_embeddings_hnsw_cos_backup", "idx_fingerprint_embeddings_hnsw_cos"),
    ):
        op.execute(f"ALTER INDEX IF EXISTS {backup_name} RENAME TO {restored_name}")
