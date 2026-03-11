"""Create baseline pgvector schema.

Revision ID: 20260310_0001
Revises:
Create Date: 2026-03-10 00:01:00
"""

from __future__ import annotations

from alembic import op


revision = "20260310_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS creators (
            id BIGSERIAL PRIMARY KEY,
            name TEXT,
            url TEXT UNIQUE
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS videos (
            id BIGSERIAL PRIMARY KEY,
            creator_id BIGINT REFERENCES creators(id),
            url TEXT,
            title TEXT,
            thumbnail_url TEXT,
            processed BOOLEAN DEFAULT FALSE
        )
        """
    )
    op.execute(
        """
        ALTER TABLE videos
        ADD COLUMN IF NOT EXISTS thumbnail_url TEXT
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS fingerprints (
            id BIGSERIAL PRIMARY KEY,
            video_id BIGINT REFERENCES videos(id),
            timestamp_seconds DOUBLE PRECISION
        )
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_fingerprints_video_timestamp
        ON fingerprints(video_id, timestamp_seconds)
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS vod_ingest_state (
            vod_platform_id TEXT PRIMARY KEY,
            video_id BIGINT NOT NULL REFERENCES videos(id),
            streamer TEXT NOT NULL,
            last_ingested_seconds INTEGER NOT NULL,
            last_seen_duration_seconds INTEGER NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        )
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = current_schema()
                  AND table_name = 'live_ingest_state'
            ) THEN
                INSERT INTO vod_ingest_state (
                    vod_platform_id,
                    video_id,
                    streamer,
                    last_ingested_seconds,
                    last_seen_duration_seconds,
                    updated_at
                )
                SELECT
                    vod_platform_id,
                    video_id,
                    streamer,
                    last_ingested_seconds,
                    last_seen_duration_seconds,
                    updated_at
                FROM live_ingest_state
                ON CONFLICT (vod_platform_id) DO NOTHING;
            END IF;
        END $$;
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS fingerprint_embeddings (
            fingerprint_id BIGINT PRIMARY KEY REFERENCES fingerprints(id) ON DELETE CASCADE,
            embedding vector(768) NOT NULL,
            creator_id BIGINT REFERENCES creators(id),
            model_name TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute(
        """
        ALTER TABLE fingerprint_embeddings
        ADD COLUMN IF NOT EXISTS creator_id BIGINT REFERENCES creators(id)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fingerprint_embeddings_ivfflat_cos
        ON fingerprint_embeddings USING ivfflat (embedding vector_cosine_ops)
        WITH (lists = 100)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fingerprint_embeddings_creator_id
        ON fingerprint_embeddings(creator_id)
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_videos_creator_id ON videos(creator_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_creators_lower_name ON creators((LOWER(name)))")
    op.execute("CREATE INDEX IF NOT EXISTS idx_videos_url ON videos(url)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_fingerprints_video_id ON fingerprints(video_id)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_fingerprints_video_id")
    op.execute("DROP INDEX IF EXISTS idx_videos_url")
    op.execute("DROP INDEX IF EXISTS idx_creators_lower_name")
    op.execute("DROP INDEX IF EXISTS idx_videos_creator_id")
    op.execute("DROP INDEX IF EXISTS idx_fingerprint_embeddings_creator_id")
    op.execute("DROP INDEX IF EXISTS idx_fingerprint_embeddings_ivfflat_cos")
    op.execute("DROP INDEX IF EXISTS idx_fingerprints_video_timestamp")
    op.execute("DROP TABLE IF EXISTS fingerprint_embeddings")
    op.execute("DROP TABLE IF EXISTS vod_ingest_state")
    op.execute("DROP TABLE IF EXISTS fingerprints")
    op.execute("DROP TABLE IF EXISTS videos")
    op.execute("DROP TABLE IF EXISTS creators")
    op.execute("DROP EXTENSION IF EXISTS vector")
