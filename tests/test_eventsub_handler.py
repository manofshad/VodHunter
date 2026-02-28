import hashlib
import hmac
import json
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

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


class TestEventSubHandler(unittest.TestCase):
    def setUp(self) -> None:
        self.monitor = FakeMonitorManager()
        self.secret = "test-secret"
        self.handler = EventSubHandler(
            monitor_manager=self.monitor,  # type: ignore[arg-type]
            secret=self.secret,
            message_ttl_seconds=600,
            max_clock_skew_seconds=600,
        )

    def test_verification_challenge_returns_plain_text(self) -> None:
        body = json.dumps({"challenge": "abc123"}).encode("utf-8")
        ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        headers = make_headers(self.secret, body, "webhook_callback_verification", "mid-1", ts)
        result = self.handler.process(headers=headers, raw_body=body)
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.body, "abc123")
        self.assertEqual(result.media_type, "text/plain")

    def test_notification_calls_online_offline_handlers(self) -> None:
        ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        body_online = json.dumps(
            {
                "subscription": {"type": "stream.online"},
                "event": {"broadcaster_user_login": "alice"},
            }
        ).encode("utf-8")
        headers_online = make_headers(self.secret, body_online, "notification", "mid-online", ts)
        result_online = self.handler.process(headers=headers_online, raw_body=body_online)
        self.assertEqual(result_online.status_code, 204)
        self.assertEqual(self.monitor.online_calls, ["alice"])

        body_offline = json.dumps(
            {
                "subscription": {"type": "stream.offline"},
                "event": {"broadcaster_user_login": "alice"},
            }
        ).encode("utf-8")
        headers_offline = make_headers(self.secret, body_offline, "notification", "mid-offline", ts)
        result_offline = self.handler.process(headers=headers_offline, raw_body=body_offline)
        self.assertEqual(result_offline.status_code, 204)
        self.assertEqual(self.monitor.offline_calls, ["alice"])

    def test_invalid_signature_rejected(self) -> None:
        body = b"{}"
        ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        headers = {
            "twitch-eventsub-message-id": "mid-2",
            "twitch-eventsub-message-timestamp": ts,
            "twitch-eventsub-message-signature": "sha256=bad",
            "twitch-eventsub-message-type": "notification",
        }
        with self.assertRaises(EventSubAuthError):
            self.handler.process(headers=headers, raw_body=body)

    def test_old_timestamp_rejected(self) -> None:
        body = b"{}"
        old_ts = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat().replace("+00:00", "Z")
        headers = make_headers(self.secret, body, "notification", "mid-3", old_ts)
        with self.assertRaises(EventSubAuthError):
            self.handler.process(headers=headers, raw_body=body)

    def test_duplicate_message_returns_204_without_reprocessing(self) -> None:
        ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        body = json.dumps(
            {
                "subscription": {"type": "stream.online"},
                "event": {"broadcaster_user_login": "alice"},
            }
        ).encode("utf-8")
        headers = make_headers(self.secret, body, "notification", "mid-dupe", ts)
        first = self.handler.process(headers=headers, raw_body=body)
        second = self.handler.process(headers=headers, raw_body=body)
        self.assertEqual(first.status_code, 204)
        self.assertEqual(second.status_code, 204)
        self.assertEqual(self.monitor.online_calls, ["alice"])

    def test_revocation_marks_degraded(self) -> None:
        ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        body = json.dumps({"subscription": {"status": "authorization_revoked"}}).encode("utf-8")
        headers = make_headers(self.secret, body, "revocation", "mid-revoke", ts)
        result = self.handler.process(headers=headers, raw_body=body)
        self.assertEqual(result.status_code, 204)
        self.assertTrue(self.monitor.degraded_calls)


if __name__ == "__main__":
    unittest.main()
