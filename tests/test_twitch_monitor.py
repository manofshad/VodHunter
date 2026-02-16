import unittest
from unittest.mock import patch
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
                },
                {
                    "id": "222",
                    "title": "Newest",
                    "duration": "2h3m4s",
                    "created_at": "2026-02-15T12:00:00Z",
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


if __name__ == "__main__":
    unittest.main()
