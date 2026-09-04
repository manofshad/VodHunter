"""Add local VOD retention safeguards."""

from __future__ import annotations

from alembic import op


revision = "20260903_0011"
down_revision = "20260824_0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_videos_streamed_at
        ON videos(streamed_at)
        WHERE streamed_at IS NOT NULL
        """
    )
    op.execute(
        """
        ALTER TABLE search_requests
        DROP CONSTRAINT IF EXISTS search_requests_matched_video_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE search_requests
        ADD CONSTRAINT search_requests_matched_video_id_fkey
        FOREIGN KEY (matched_video_id)
        REFERENCES videos(id)
        ON DELETE SET NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_videos_streamed_at")
    op.execute(
        """
        ALTER TABLE search_requests
        DROP CONSTRAINT IF EXISTS search_requests_matched_video_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE search_requests
        ADD CONSTRAINT search_requests_matched_video_id_fkey
        FOREIGN KEY (matched_video_id)
        REFERENCES videos(id)
        """
    )
