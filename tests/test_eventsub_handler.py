import hashlib
import hmac
import json
from datetime import datetime, timedelta, timezone

import pytest

from backend.services.eventsub_handler import EventSubAuthError, EventSubHandler


class FakeMonitorManager:
    def __init__(self):
        self.online_calls: list[str] = []
        self.offline_calls: list[str] = []
        self.degraded_calls: list[str] = []
        self.healthy_calls = 0
        self.event_calls = 0

    def on_stream_online(self, streamer: str, event_payload=None):
        self.online_calls.append(streamer)

    def on_stream_offline(self, streamer: str, event_payload=None):
        self.offline_calls.append(streamer)

    def mark_eventsub_degraded(self, reason: str):
        self.degraded_calls.append(reason)

    def mark_eventsub_healthy(self):
        self.healthy_calls += 1

    def note_eventsub_event(self):
        self.event_calls += 1


def make_headers(secret: str, body: bytes, message_type: str, message_id: str, timestamp: str) -> dict[str, str]:
    payload = (message_id + timestamp).encode("utf-8") + body
    signature = "sha256=" + hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()
    return {
        "twitch-eventsub-message-id": message_id,
        "twitch-eventsub-message-timestamp": timestamp,
        "twitch-eventsub-message-signature": signature,
        "twitch-eventsub-message-type": message_type,
    }


@pytest.fixture
def handler_state():
    monitor = FakeMonitorManager()
    secret = "test-secret"
    handler = EventSubHandler(
        monitor_manager=monitor,  # type: ignore[arg-type]
        secret=secret,
        message_ttl_seconds=600,
        max_clock_skew_seconds=600,
    )
    return handler, monitor, secret


def test_verification_challenge_returns_plain_text(handler_state) -> None:
    handler, _monitor, secret = handler_state
    body = json.dumps({"challenge": "abc123"}).encode("utf-8")
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    headers = make_headers(secret, body, "webhook_callback_verification", "mid-1", ts)

    result = handler.process(headers=headers, raw_body=body)

    assert result.status_code == 200
    assert result.body == "abc123"
    assert result.media_type == "text/plain"


def test_notification_calls_online_offline_handlers(handler_state) -> None:
    handler, monitor, secret = handler_state
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    body_online = json.dumps(
        {
            "subscription": {"type": "stream.online"},
            "event": {"broadcaster_user_login": "alice"},
        }
    ).encode("utf-8")
    headers_online = make_headers(secret, body_online, "notification", "mid-online", ts)

    result_online = handler.process(headers=headers_online, raw_body=body_online)

    assert result_online.status_code == 204
    assert monitor.online_calls == ["alice"]

    body_offline = json.dumps(
        {
            "subscription": {"type": "stream.offline"},
            "event": {"broadcaster_user_login": "alice"},
        }
    ).encode("utf-8")
    headers_offline = make_headers(secret, body_offline, "notification", "mid-offline", ts)

    result_offline = handler.process(headers=headers_offline, raw_body=body_offline)

    assert result_offline.status_code == 204
    assert monitor.offline_calls == ["alice"]


def test_invalid_signature_rejected(handler_state) -> None:
    handler, _monitor, _secret = handler_state
    body = b"{}"
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    headers = {
        "twitch-eventsub-message-id": "mid-2",
        "twitch-eventsub-message-timestamp": ts,
        "twitch-eventsub-message-signature": "sha256=bad",
        "twitch-eventsub-message-type": "notification",
    }

    with pytest.raises(EventSubAuthError):
        handler.process(headers=headers, raw_body=body)


def test_old_timestamp_rejected(handler_state) -> None:
    handler, _monitor, secret = handler_state
    body = b"{}"
    old_ts = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    headers = make_headers(secret, body, "notification", "mid-3", old_ts)

    with pytest.raises(EventSubAuthError):
        handler.process(headers=headers, raw_body=body)


def test_duplicate_message_returns_204_without_reprocessing(handler_state) -> None:
    handler, monitor, secret = handler_state
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    body = json.dumps(
        {
            "subscription": {"type": "stream.online"},
            "event": {"broadcaster_user_login": "alice"},
        }
    ).encode("utf-8")
    headers = make_headers(secret, body, "notification", "mid-dupe", ts)

    first = handler.process(headers=headers, raw_body=body)
    second = handler.process(headers=headers, raw_body=body)

    assert first.status_code == 204
    assert second.status_code == 204
    assert monitor.online_calls == ["alice"]


def test_revocation_marks_degraded(handler_state) -> None:
    handler, monitor, secret = handler_state
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    body = json.dumps({"subscription": {"status": "authorization_revoked"}}).encode("utf-8")
    headers = make_headers(secret, body, "revocation", "mid-revoke", ts)

    result = handler.process(headers=headers, raw_body=body)

    assert result.status_code == 204
    assert monitor.degraded_calls
