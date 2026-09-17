"""Add Web Push installations, Shortcut pairings, and search delivery state.

Revision ID: 20260915_0015
Revises: 20260913_0014
Create Date: 2026-09-15 00:15:00
"""

from __future__ import annotations

from alembic import op


revision = "20260915_0015"
down_revision = "20260913_0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS notification_installations (
            id BIGSERIAL PRIMARY KEY,
            browser_token_hash TEXT NOT NULL UNIQUE,
            shortcut_token_hash TEXT UNIQUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            revoked_at TIMESTAMPTZ
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS push_subscriptions (
            id BIGSERIAL PRIMARY KEY,
            installation_id BIGINT NOT NULL
                REFERENCES notification_installations(id) ON DELETE CASCADE,
            endpoint TEXT NOT NULL UNIQUE,
            p256dh TEXT NOT NULL,
            auth TEXT NOT NULL,
            expiration_time DOUBLE PRECISION,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            revoked_at TIMESTAMPTZ
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_push_subscriptions_installation_active
        ON push_subscriptions(installation_id)
        WHERE revoked_at IS NULL
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS notification_pairings (
            id UUID PRIMARY KEY,
            installation_id BIGINT NOT NULL
                REFERENCES notification_installations(id) ON DELETE CASCADE,
            pairing_code_hash TEXT NOT NULL UNIQUE,
            expires_at TIMESTAMPTZ NOT NULL,
            claimed_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_notification_pairings_installation_created
        ON notification_pairings(installation_id, created_at DESC)
        """
    )
    op.execute(
        """
        ALTER TABLE search_requests
        ADD COLUMN IF NOT EXISTS notification_installation_id BIGINT
            REFERENCES notification_installations(id) ON DELETE SET NULL,
        ADD COLUMN IF NOT EXISTS notification_sent_at TIMESTAMPTZ
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_search_requests_notification_installation
        ON search_requests(notification_installation_id, created_at DESC)
        WHERE notification_installation_id IS NOT NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_search_requests_notification_installation")
    op.execute(
        """
        ALTER TABLE search_requests
        DROP COLUMN IF EXISTS notification_sent_at,
        DROP COLUMN IF EXISTS notification_installation_id
        """
    )
    op.execute("DROP TABLE IF EXISTS notification_pairings")
    op.execute("DROP TABLE IF EXISTS push_subscriptions")
    op.execute("DROP TABLE IF EXISTS notification_installations")
