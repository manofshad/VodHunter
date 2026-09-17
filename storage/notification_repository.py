"""Persistence for Web Push subscriptions and iOS Shortcut pairing tokens."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import secrets
import uuid

from storage.database import PostgresDatabase


class InvalidNotificationTokenError(ValueError):
    """Raised when a browser or Shortcut installation token is invalid."""


class InvalidPairingCodeError(ValueError):
    """Raised when a pairing code is missing, expired, or already claimed."""


@dataclass(frozen=True)
class PairingRecord:
    id: str
    code: str
    expires_at: str


@dataclass(frozen=True)
class PushSubscriptionRecord:
    id: int
    endpoint: str
    p256dh: str
    auth: str


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _isoformat(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


class NotificationRepository:
    def __init__(self, database: PostgresDatabase) -> None:
        self.database = database

    def register_subscription(
        self,
        *,
        endpoint: str,
        p256dh: str,
        auth: str,
        expiration_time: float | None,
        browser_token: str | None,
    ) -> tuple[int, str | None]:
        new_browser_token: str | None = None
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                if browser_token:
                    cur.execute(
                        """
                        SELECT id
                        FROM notification_installations
                        WHERE browser_token_hash = %s AND revoked_at IS NULL
                        LIMIT 1
                        """,
                        (_token_hash(browser_token),),
                    )
                    row = cur.fetchone()
                    if row is None:
                        raise InvalidNotificationTokenError("Notification installation token is invalid")
                    installation_id = int(row[0])
                else:
                    new_browser_token = secrets.token_urlsafe(32)
                    cur.execute(
                        """
                        INSERT INTO notification_installations (browser_token_hash)
                        VALUES (%s)
                        RETURNING id
                        """,
                        (_token_hash(new_browser_token),),
                    )
                    row = cur.fetchone()
                    if row is None:
                        raise RuntimeError("Failed to create notification installation")
                    installation_id = int(row[0])

                cur.execute(
                    """
                    UPDATE push_subscriptions
                    SET revoked_at = NOW(), updated_at = NOW()
                    WHERE installation_id = %s
                      AND endpoint <> %s
                      AND revoked_at IS NULL
                    """,
                    (installation_id, endpoint),
                )
                cur.execute(
                    """
                    INSERT INTO push_subscriptions (
                        installation_id, endpoint, p256dh, auth, expiration_time
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (endpoint) DO UPDATE
                    SET installation_id = EXCLUDED.installation_id,
                        p256dh = EXCLUDED.p256dh,
                        auth = EXCLUDED.auth,
                        expiration_time = EXCLUDED.expiration_time,
                        updated_at = NOW(),
                        revoked_at = NULL
                    """,
                    (installation_id, endpoint, p256dh, auth, expiration_time),
                )
                cur.execute(
                    """
                    UPDATE notification_installations
                    SET last_seen_at = NOW(), updated_at = NOW()
                    WHERE id = %s
                    """,
                    (installation_id,),
                )
        return installation_id, new_browser_token

    def create_pairing(self, browser_token: str, *, ttl_seconds: int = 300) -> PairingRecord:
        pairing_id = str(uuid.uuid4())
        pairing_code = secrets.token_urlsafe(24)
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=max(int(ttl_seconds), 1))
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                installation_id = self._get_browser_installation_id(cur, browser_token)
                cur.execute(
                    """
                    DELETE FROM notification_pairings
                    WHERE installation_id = %s AND claimed_at IS NULL
                    """,
                    (installation_id,),
                )
                cur.execute(
                    """
                    INSERT INTO notification_pairings (
                        id, installation_id, pairing_code_hash, expires_at
                    )
                    VALUES (%s, %s, %s, %s)
                    """,
                    (pairing_id, installation_id, _token_hash(pairing_code), expires_at),
                )
        return PairingRecord(
            id=pairing_id,
            code=pairing_code,
            expires_at=_isoformat(expires_at),
        )

    def get_pairing_status(self, pairing_id: str, browser_token: str) -> str | None:
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                installation_id = self._get_browser_installation_id(cur, browser_token)
                cur.execute(
                    """
                    SELECT claimed_at, expires_at
                    FROM notification_pairings
                    WHERE id = %s AND installation_id = %s
                    LIMIT 1
                    """,
                    (pairing_id, installation_id),
                )
                row = cur.fetchone()
        if row is None:
            return None
        claimed_at, expires_at = row
        if claimed_at is not None:
            return "connected"
        now = datetime.now(timezone.utc)
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        return "expired" if expires_at <= now else "pending"

    def claim_pairing(self, pairing_code: str) -> str:
        shortcut_token = secrets.token_urlsafe(32)
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT pairing.id, pairing.installation_id
                    FROM notification_pairings AS pairing
                    JOIN notification_installations AS installation
                      ON installation.id = pairing.installation_id
                    WHERE pairing.pairing_code_hash = %s
                      AND pairing.claimed_at IS NULL
                      AND pairing.expires_at > NOW()
                      AND installation.revoked_at IS NULL
                    FOR UPDATE OF pairing
                    LIMIT 1
                    """,
                    (_token_hash(pairing_code),),
                )
                row = cur.fetchone()
                if row is None:
                    raise InvalidPairingCodeError("Pairing code is invalid, expired, or already used")
                pairing_id, installation_id = row
                cur.execute(
                    """
                    UPDATE notification_installations
                    SET shortcut_token_hash = %s,
                        updated_at = NOW(),
                        last_seen_at = NOW()
                    WHERE id = %s
                    """,
                    (_token_hash(shortcut_token), int(installation_id)),
                )
                cur.execute(
                    """
                    UPDATE notification_pairings
                    SET claimed_at = NOW()
                    WHERE id = %s AND claimed_at IS NULL
                    """,
                    (pairing_id,),
                )
        return shortcut_token

    def resolve_shortcut_token(self, shortcut_token: str | None) -> int | None:
        if not shortcut_token:
            return None
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT installation.id
                    FROM notification_installations AS installation
                    WHERE installation.shortcut_token_hash = %s
                      AND installation.revoked_at IS NULL
                      AND EXISTS (
                          SELECT 1
                          FROM push_subscriptions AS subscription
                          WHERE subscription.installation_id = installation.id
                            AND subscription.revoked_at IS NULL
                      )
                    LIMIT 1
                    """,
                    (_token_hash(shortcut_token),),
                )
                row = cur.fetchone()
                if row is not None:
                    cur.execute(
                        """
                        UPDATE notification_installations
                        SET last_seen_at = NOW()
                        WHERE id = %s
                        """,
                        (int(row[0]),),
                    )
        return int(row[0]) if row is not None else None

    def list_search_subscriptions(self, search_id: int) -> list[PushSubscriptionRecord]:
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT subscription.id, subscription.endpoint,
                           subscription.p256dh, subscription.auth
                    FROM search_requests AS search_request
                    JOIN notification_installations AS installation
                      ON installation.id = search_request.notification_installation_id
                     AND installation.revoked_at IS NULL
                    JOIN push_subscriptions AS subscription
                      ON subscription.installation_id = installation.id
                     AND subscription.revoked_at IS NULL
                    WHERE search_request.id = %s
                      AND search_request.notification_sent_at IS NULL
                    """,
                    (int(search_id),),
                )
                rows = cur.fetchall()
        return [
            PushSubscriptionRecord(
                id=int(row[0]),
                endpoint=str(row[1]),
                p256dh=str(row[2]),
                auth=str(row[3]),
            )
            for row in rows
        ]

    def mark_subscription_revoked(self, subscription_id: int) -> None:
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE push_subscriptions
                    SET revoked_at = NOW(), updated_at = NOW()
                    WHERE id = %s
                    """,
                    (int(subscription_id),),
                )

    def mark_search_notification_sent(self, search_id: int) -> None:
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE search_requests
                    SET notification_sent_at = COALESCE(notification_sent_at, NOW())
                    WHERE id = %s
                    """,
                    (int(search_id),),
                )

    @staticmethod
    def _get_browser_installation_id(cur, browser_token: str) -> int:
        cur.execute(
            """
            SELECT id
            FROM notification_installations
            WHERE browser_token_hash = %s AND revoked_at IS NULL
            LIMIT 1
            """,
            (_token_hash(browser_token),),
        )
        row = cur.fetchone()
        if row is None:
            raise InvalidNotificationTokenError("Notification installation token is invalid")
        return int(row[0])
