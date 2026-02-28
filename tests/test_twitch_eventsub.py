import unittest
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from services.twitch_eventsub import EventSubClient


class FakeHelixMonitor:
    def __init__(self, subscriptions: list[dict]):
        self.subscriptions = list(subscriptions)
        self.next_id = 100

    def _helix_request(self, path: str, method: str = "GET", params=None, body=None):
        if path != "eventsub/subscriptions":
            raise AssertionError(path)

        if method == "GET":
            return {"data": [dict(item) for item in self.subscriptions]}

        if method == "POST":
            self.next_id += 1
            created = {
                "id": str(self.next_id),
                "type": body["type"],
                "status": "webhook_callback_verification_pending",
                "condition": {"broadcaster_user_id": body["condition"]["broadcaster_user_id"]},
                "transport": {
                    "method": "webhook",
                    "callback": body["transport"]["callback"],
                },
            }
            self.subscriptions.append(created)
            return {"data": [created]}

        if method == "DELETE":
            target_id = str((params or {}).get("id", ""))
            self.subscriptions = [s for s in self.subscriptions if str(s.get("id")) != target_id]
            return {}

        raise AssertionError(method)


class TestTwitchEventSub(unittest.TestCase):
    def test_ensure_stream_subscriptions_reconciles_duplicates(self) -> None:
        monitor = FakeHelixMonitor(
            subscriptions=[
                {
                    "id": "1",
                    "type": "stream.online",
                    "status": "enabled",
                    "condition": {"broadcaster_user_id": "user-1"},
                    "transport": {"method": "webhook", "callback": "https://cb.example/api/twitch/eventsub"},
                },
                {
                    "id": "2",
                    "type": "stream.online",
                    "status": "enabled",
                    "condition": {"broadcaster_user_id": "user-1"},
                    "transport": {"method": "webhook", "callback": "https://cb.example/api/twitch/eventsub"},
                },
                {
                    "id": "3",
                    "type": "stream.offline",
                    "status": "enabled",
                    "condition": {"broadcaster_user_id": "user-1"},
                    "transport": {"method": "webhook", "callback": "https://wrong.example/cb"},
                },
                {
                    "id": "4",
                    "type": "stream.offline",
                    "status": "enabled",
                    "condition": {"broadcaster_user_id": "other-user"},
                    "transport": {"method": "webhook", "callback": "https://cb.example/api/twitch/eventsub"},
                },
            ]
        )
        client = EventSubClient(twitch_monitor=monitor)  # type: ignore[arg-type]

        ids = client.ensure_stream_subscriptions(
            user_id="user-1",
            callback_url="https://cb.example/api/twitch/eventsub",
            secret="secret-value",
        )

        self.assertIn("stream.online", ids)
        self.assertIn("stream.offline", ids)

        user1_subs = [
            row
            for row in monitor.subscriptions
            if row.get("condition", {}).get("broadcaster_user_id") == "user-1"
        ]
        online = [row for row in user1_subs if row.get("type") == "stream.online"]
        offline = [row for row in user1_subs if row.get("type") == "stream.offline"]
        self.assertEqual(len(online), 1)
        self.assertEqual(len(offline), 1)
        self.assertEqual(offline[0]["transport"]["callback"], "https://cb.example/api/twitch/eventsub")

    def test_cleanup_for_broadcaster_deletes_online_offline(self) -> None:
        monitor = FakeHelixMonitor(
            subscriptions=[
                {
                    "id": "1",
                    "type": "stream.online",
                    "status": "enabled",
                    "condition": {"broadcaster_user_id": "user-1"},
                    "transport": {"method": "webhook", "callback": "https://cb.example"},
                },
                {
                    "id": "2",
                    "type": "stream.offline",
                    "status": "enabled",
                    "condition": {"broadcaster_user_id": "user-1"},
                    "transport": {"method": "webhook", "callback": "https://cb.example"},
                },
                {
                    "id": "3",
                    "type": "channel.follow",
                    "status": "enabled",
                    "condition": {"broadcaster_user_id": "user-1"},
                    "transport": {"method": "webhook", "callback": "https://cb.example"},
                },
            ]
        )
        client = EventSubClient(twitch_monitor=monitor)  # type: ignore[arg-type]
        client.cleanup_for_broadcaster("user-1")

        remaining_types = [row["type"] for row in monitor.subscriptions]
        self.assertEqual(remaining_types, ["channel.follow"])


if __name__ == "__main__":
    unittest.main()
