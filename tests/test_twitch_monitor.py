import unittest
from unittest.mock import patch
from datetime import datetime, timezone
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from services.twitch_monitor import TwitchMonitor


class TestTwitchMonitor(unittest.TestCase):
    def test_parse_duration_to_seconds(self) -> None:
        self.assertEqual(TwitchMonitor.parse_duration_to_seconds("1h2m3s"), 3723)
        self.assertEqual(TwitchMonitor.parse_duration_to_seconds("45m"), 2700)
        self.assertEqual(TwitchMonitor.parse_duration_to_seconds("59s"), 59)
        self.assertEqual(TwitchMonitor.parse_duration_to_seconds(""), 0)

    def test_selects_latest_archive_vod(self) -> None:
        monitor = TwitchMonitor(client_id="x", client_secret="y")

        payload = {
            "data": [
                {
                    "id": "111",
                    "title": "Older",
                    "duration": "1h0m0s",
                    "created_at": "2026-02-15T10:00:00Z",
                    "thumbnail_url": "https://static-cdn.jtvnw.net/cf_vods/d2/vod-thumb-%{width}x%{height}.jpg",
                },
                {
                    "id": "222",
                    "title": "Newest",
                    "duration": "2h3m4s",
                    "created_at": "2026-02-15T12:00:00Z",
                    "thumbnail_url": "https://static-cdn.jtvnw.net/cf_vods/d2/new-thumb-%{width}x%{height}.jpg",
                },
            ]
        }

        with patch.object(monitor, "_helix_get", return_value=payload):
            vod = monitor.get_latest_archive_vod("user-1")

        self.assertIsNotNone(vod)
        assert vod is not None
        self.assertEqual(vod["id"], "222")
        self.assertEqual(vod["url"], "https://www.twitch.tv/videos/222")
        self.assertEqual(vod["duration_seconds"], 7384)
        self.assertEqual(
            vod["thumbnail_url"],
            "https://static-cdn.jtvnw.net/cf_vods/d2/new-thumb-320x180.jpg",
        )

    def test_list_archive_vods_since_paginates_filters_and_sorts(self) -> None:
        monitor = TwitchMonitor(client_id="x", client_secret="y")
        first_page = {
            "data": [
                {
                    "id": "300",
                    "title": "Newest",
                    "duration": "2h0m0s",
                    "created_at": "2026-03-09T13:00:00Z",
                    "thumbnail_url": "https://cdn/%{width}x%{height}.jpg",
                },
                {
                    "id": "200",
                    "title": "Middle",
                    "duration": "1h0m0s",
                    "created_at": "2026-03-08T13:00:00Z",
                    "thumbnail_url": "https://cdn/%{width}x%{height}.jpg",
                },
            ],
            "pagination": {"cursor": "next-page"},
        }
        second_page = {
            "data": [
                {
                    "id": "100",
                    "title": "Too old",
                    "duration": "30m",
                    "created_at": "2026-03-01T13:00:00Z",
                    "thumbnail_url": "https://cdn/%{width}x%{height}.jpg",
                }
            ],
            "pagination": {},
        }

        with patch.object(monitor, "_helix_get", side_effect=[first_page, second_page]) as helix_get:
            vods = monitor.list_archive_vods_since(
                "user-1",
                datetime(2026, 3, 5, 0, 0, tzinfo=timezone.utc),
            )

        self.assertEqual([vod["id"] for vod in vods], ["300", "200"])
        self.assertEqual(helix_get.call_args_list[0].args[1]["first"], "100")
        self.assertEqual(helix_get.call_args_list[1].args[1]["after"], "next-page")


if __name__ == "__main__":
    unittest.main()
