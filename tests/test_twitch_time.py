import unittest
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from search.twitch_time import build_twitch_timestamp_url, format_twitch_offset


class TestTwitchTime(unittest.TestCase):
    def test_format_twitch_offset_examples(self) -> None:
        self.assertEqual(format_twitch_offset(1368), "22m48s")
        self.assertEqual(format_twitch_offset(59), "59s")
        self.assertEqual(format_twitch_offset(60), "1m0s")
        self.assertEqual(format_twitch_offset(3723), "1h2m3s")

    def test_build_twitch_timestamp_url_handles_invalid_seconds(self) -> None:
        self.assertIsNone(build_twitch_timestamp_url("https://www.twitch.tv/videos/1", None))
        self.assertIsNone(build_twitch_timestamp_url("https://www.twitch.tv/videos/1", -1))

    def test_build_twitch_timestamp_url_appends_or_replaces_t(self) -> None:
        self.assertEqual(
            build_twitch_timestamp_url("https://www.twitch.tv/videos/2699020769", 1368),
            "https://www.twitch.tv/videos/2699020769?t=22m48s",
        )
        self.assertEqual(
            build_twitch_timestamp_url("https://www.twitch.tv/videos/1?foo=bar&t=1s", 60),
            "https://www.twitch.tv/videos/1?foo=bar&t=1m0s",
        )


if __name__ == "__main__":
    unittest.main()
