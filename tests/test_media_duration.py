import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from backend.services.media_duration import MediaDurationError, probe_media_duration_seconds


class TestMediaDuration(unittest.TestCase):
    def test_probe_returns_duration_seconds(self) -> None:
        ok = subprocess.CompletedProcess(args=["ffprobe"], returncode=0, stdout="123.45\n", stderr="")
        with patch("backend.services.media_duration.subprocess.run", return_value=ok):
            duration = probe_media_duration_seconds("/tmp/input.mp4")
        self.assertEqual(duration, 123.45)

    def test_probe_raises_when_ffprobe_fails(self) -> None:
        failed = subprocess.CompletedProcess(args=["ffprobe"], returncode=1, stdout="", stderr="err")
        with patch("backend.services.media_duration.subprocess.run", return_value=failed):
            with self.assertRaises(MediaDurationError):
                probe_media_duration_seconds("/tmp/input.mp4")

    def test_probe_raises_when_duration_missing(self) -> None:
        ok = subprocess.CompletedProcess(args=["ffprobe"], returncode=0, stdout="", stderr="")
        with patch("backend.services.media_duration.subprocess.run", return_value=ok):
            with self.assertRaises(MediaDurationError):
                probe_media_duration_seconds("/tmp/input.mp4")


if __name__ == "__main__":
    unittest.main()
