import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from backend.services.remote_clip_downloader import DownloadError, InvalidTikTokUrlError, RemoteClipDownloader


class TestRemoteClipDownloader(unittest.TestCase):
    def test_validate_accepts_allowed_tiktok_hosts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)

            accepted = downloader.validate_tiktok_url("https://www.tiktok.com/@demo/video/1")
            self.assertEqual(accepted, "https://www.tiktok.com/@demo/video/1")

    def test_validate_rejects_non_tiktok_host(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)
            with self.assertRaises(InvalidTikTokUrlError):
                downloader.validate_tiktok_url("https://example.com/video")

    def test_download_raises_when_ytdlp_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)
            failed = subprocess.CompletedProcess(args=["yt-dlp"], returncode=1, stdout="", stderr="boom")
            with patch("backend.services.remote_clip_downloader.subprocess.run", return_value=failed):
                with self.assertRaises(DownloadError):
                    downloader.download_tiktok("https://www.tiktok.com/@demo/video/1")

    def test_download_raises_when_output_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)
            ok = subprocess.CompletedProcess(args=["yt-dlp"], returncode=0, stdout="", stderr="")
            with patch("backend.services.remote_clip_downloader.subprocess.run", return_value=ok):
                with self.assertRaises(DownloadError):
                    downloader.download_tiktok("https://www.tiktok.com/@demo/video/1")

    def test_download_raises_on_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)
            with patch(
                "backend.services.remote_clip_downloader.subprocess.run",
                side_effect=subprocess.TimeoutExpired(cmd=["yt-dlp"], timeout=5),
            ):
                with self.assertRaises(DownloadError):
                    downloader.download_tiktok("https://www.tiktok.com/@demo/video/1")

    def test_download_returns_created_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)

            def fake_run(*args, **kwargs):
                out = os.path.join(tmp, "tiktok_abc.mp4")
                with open(out, "wb") as f:
                    f.write(b"video-bytes")
                return subprocess.CompletedProcess(args=["yt-dlp"], returncode=0, stdout="", stderr="")

            with patch("backend.services.remote_clip_downloader.uuid.uuid4", return_value=SimpleNamespace(hex="abc")):
                with patch("backend.services.remote_clip_downloader.subprocess.run", side_effect=fake_run):
                    result = downloader.download_tiktok("https://www.tiktok.com/@demo/video/1")

            self.assertTrue(os.path.exists(result.path))
            downloader.cleanup(result.path)
            self.assertFalse(os.path.exists(result.path))


if __name__ == "__main__":
    unittest.main()
