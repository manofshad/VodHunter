"""Replace the AST vector index with the production NMFP index schema.

Revision ID: 20260824_0010
Revises: 20260522_0009
Create Date: 2026-08-24 00:10:00

The previous production database is gone, so there is no mixed-model serving
window to preserve.  If this revision is applied to a development database,
it deliberately clears incompatible AST fingerprints and marks their videos
for a fresh NMFP reindex.
"""

from __future__ import annotations

from alembic import op


revision = "20260824_0010"
down_revision = "20260522_0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_fingerprint_embeddings_hnsw_cos")
    op.execute("DROP INDEX IF EXISTS idx_fingerprint_embeddings_model_versions")

    # AST vectors cannot be converted into NMFP fingerprints. Remove both the
    # vectors and their timestamp rows so a backfill cannot accidentally treat
    # the old timeline as NMFP-compatible.
    op.execute("DELETE FROM fingerprint_embeddings")
    op.execute("DELETE FROM fingerprints")
    op.execute("DELETE FROM vod_ingest_state")
    op.execute(
        """
        UPDATE videos
        SET status = 'reindex_requested',
            processed = TRUE
        WHERE status <> 'deleted'
        """
    )

    op.execute(
        """
        ALTER TABLE fingerprint_embeddings
        ALTER COLUMN embedding TYPE vector(128)
        """
    )
    op.execute(
        """
        ALTER TABLE fingerprint_embeddings
        ADD COLUMN IF NOT EXISTS model_version TEXT,
        ADD COLUMN IF NOT EXISTS preprocessing_version TEXT
        """
    )
    op.execute(
        """
        ALTER TABLE fingerprint_embeddings
        ALTER COLUMN model_version SET NOT NULL,
        ALTER COLUMN preprocessing_version SET NOT NULL,
        DROP COLUMN IF EXISTS model_name
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fingerprint_embeddings_model_versions
        ON fingerprint_embeddings(creator_id, model_version, preprocessing_version)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fingerprint_embeddings_hnsw_cos
        ON fingerprint_embeddings USING hnsw (embedding vector_cosine_ops)
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS fingerprint_index_metadata (
            singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
            model_version TEXT NOT NULL,
            preprocessing_version TEXT NOT NULL,
            embedding_dim INTEGER NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute(
        """
        INSERT INTO fingerprint_index_metadata (
            singleton,
            model_version,
            preprocessing_version,
            embedding_dim,
            updated_at
        )
        VALUES (
            TRUE,
            'nmfp-triplet@15c6f3bcdf6a6da1daddfe47a1ffa5a0d22deadc+zenodo-15719945+ckpt-100',
            'nmfp-8khz-mono-1s-hop0.5-mel-v1',
            128,
            NOW()
        )
        ON CONFLICT (singleton) DO UPDATE SET
            model_version = excluded.model_version,
            preprocessing_version = excluded.preprocessing_version,
            embedding_dim = excluded.embedding_dim,
            updated_at = excluded.updated_at
        """
    )

    op.execute(
        """
        ALTER TABLE search_requests
        ADD COLUMN IF NOT EXISTS result_payload JSONB,
        ADD COLUMN IF NOT EXISTS model_version TEXT,
        ADD COLUMN IF NOT EXISTS preprocessing_version TEXT,
        ADD COLUMN IF NOT EXISTS model_startup_duration_ms INTEGER,
        ADD COLUMN IF NOT EXISTS model_cold_start BOOLEAN,
        ADD COLUMN IF NOT EXISTS fingerprint_preprocessing_duration_ms INTEGER,
        ADD COLUMN IF NOT EXISTS fingerprint_inference_duration_ms INTEGER,
        ADD COLUMN IF NOT EXISTS fingerprint_duration_ms INTEGER,
        ADD COLUMN IF NOT EXISTS query_fingerprint_count INTEGER,
        ADD COLUMN IF NOT EXISTS candidate_count INTEGER,
        ADD COLUMN IF NOT EXISTS segment_count INTEGER
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_fingerprint_embeddings_hnsw_cos")
    op.execute("DROP INDEX IF EXISTS idx_fingerprint_embeddings_model_versions")
    op.execute("DROP TABLE IF EXISTS fingerprint_index_metadata")

    # The two embedding spaces are not convertible in either direction.
    op.execute("DELETE FROM fingerprint_embeddings")
    op.execute("DELETE FROM fingerprints")
    op.execute("DELETE FROM vod_ingest_state")
    op.execute(
        """
        UPDATE videos
        SET status = 'reindex_requested',
            processed = TRUE
        WHERE status <> 'deleted'
        """
    )

    op.execute("ALTER TABLE fingerprint_embeddings ADD COLUMN IF NOT EXISTS model_name TEXT")
    op.execute(
        """
        ALTER TABLE fingerprint_embeddings
        DROP COLUMN IF EXISTS preprocessing_version,
        DROP COLUMN IF EXISTS model_version,
        ALTER COLUMN embedding TYPE vector(768)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fingerprint_embeddings_hnsw_cos
        ON fingerprint_embeddings USING hnsw (embedding vector_cosine_ops)
        """
    )

    op.execute(
        """
        ALTER TABLE search_requests
        DROP COLUMN IF EXISTS segment_count,
        DROP COLUMN IF EXISTS candidate_count,
        DROP COLUMN IF EXISTS query_fingerprint_count,
        DROP COLUMN IF EXISTS fingerprint_duration_ms,
        DROP COLUMN IF EXISTS fingerprint_inference_duration_ms,
        DROP COLUMN IF EXISTS fingerprint_preprocessing_duration_ms,
        DROP COLUMN IF EXISTS model_cold_start,
        DROP COLUMN IF EXISTS model_startup_duration_ms,
        DROP COLUMN IF EXISTS preprocessing_version,
        DROP COLUMN IF EXISTS model_version,
        DROP COLUMN IF EXISTS result_payload
        """
    )
