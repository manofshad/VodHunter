from datetime import datetime, timezone
import hashlib
import uuid
from unittest.mock import patch

from storage.notification_repository import NotificationRepository


class FakeCursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def execute(self, sql, params=()):
        self.executed.append((sql, tuple(params)))

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def fetchall(self):
        return self.rows.pop(0) if self.rows else []


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def cursor(self):
        return self._cursor


class FakeDatabase:
    def __init__(self, rows):
        self.cursor_value = FakeCursor(rows)

    def connect(self):
        return FakeConnection(self.cursor_value)


def digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def test_pairing_persists_only_a_hash_of_the_browser_token_and_one_time_code() -> None:
    database = FakeDatabase([(17,)])
    repository = NotificationRepository(database)
    pairing_id = uuid.UUID("9f7cf239-5b35-4e56-96e8-1f61a4f7c3c0")

    with patch("storage.notification_repository.secrets.token_urlsafe", return_value="pair-code"), patch(
        "storage.notification_repository.uuid.uuid4", return_value=pairing_id
    ):
        pairing = repository.create_pairing("browser-secret", ttl_seconds=300)

    assert pairing.id == str(pairing_id)
    assert pairing.code == "pair-code"
    all_params = [item for _, params in database.cursor_value.executed for item in params]
    assert "browser-secret" not in all_params
    assert "pair-code" not in all_params
    assert digest("browser-secret") in all_params
    assert digest("pair-code") in all_params


def test_subscription_refresh_revokes_older_endpoints_for_the_installation() -> None:
    database = FakeDatabase([(17,)])
    repository = NotificationRepository(database)

    installation_id, new_token = repository.register_subscription(
        endpoint="https://web.push.apple.com/new-endpoint",
        p256dh="public-key",
        auth="auth-secret",
        expiration_time=None,
        browser_token="browser-secret",
    )

    assert installation_id == 17
    assert new_token is None
    sql_statements = [sql for sql, _ in database.cursor_value.executed]
    revoke_index = next(index for index, sql in enumerate(sql_statements) if "UPDATE push_subscriptions" in sql)
    upsert_index = next(index for index, sql in enumerate(sql_statements) if "INSERT INTO push_subscriptions" in sql)
    assert revoke_index < upsert_index


def test_claiming_pairing_rotates_and_returns_a_raw_shortcut_token_once() -> None:
    pairing_id = uuid.UUID("9f7cf239-5b35-4e56-96e8-1f61a4f7c3c0")
    database = FakeDatabase([(pairing_id, 17)])
    repository = NotificationRepository(database)

    with patch("storage.notification_repository.secrets.token_urlsafe", return_value="shortcut-secret"):
        shortcut_token = repository.claim_pairing("pair-code")

    assert shortcut_token == "shortcut-secret"
    all_params = [item for _, params in database.cursor_value.executed for item in params]
    assert "pair-code" not in all_params
    assert "shortcut-secret" not in all_params
    assert digest("pair-code") in all_params
    assert digest("shortcut-secret") in all_params


def test_pairing_status_reports_expiration_from_server_time() -> None:
    database = FakeDatabase([(21,), (None, datetime(2020, 1, 1, tzinfo=timezone.utc))])
    repository = NotificationRepository(database)

    assert repository.get_pairing_status(
        "9f7cf239-5b35-4e56-96e8-1f61a4f7c3c0",
        "browser-secret",
    ) == "expired"
