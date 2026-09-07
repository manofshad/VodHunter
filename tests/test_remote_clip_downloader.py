import json
import os
import pytest
import subprocess
import tempfile
from types import SimpleNamespace
from unittest.mock import patch
from backend.services.remote_clip_downloader import (
    DownloadError,
    InvalidTikTokUrlError,
    RemoteClipDownloader,
    validate_tiktok_url,
)

class TestRemoteClipDownloader:

    @pytest.mark.parametrize(
        "url",
        [
            "https://www.tiktok.com/@demo/video/1",
            "https://www.tiktok.com/@/video/1",
            "https://www.tiktok.com/share/video/1",
            "https://www.tiktok.com/embed/1",
            "https://www.tiktokv.com/@demo/video/1",
            "https://www.tiktok.com/t/ZP8ctwC2V/",
            "https://vm.tiktok.com/ZTR45GpSF/",
            "https://vt.tiktok.com/ZSe4FqkKd",
        ],
    )
    def test_validate_accepts_supported_tiktok_video_links(self, url: str) -> None:
        assert validate_tiktok_url(url) == url

    def test_validate_normalizes_bare_tiktok_host(self) -> None:
        assert validate_tiktok_url("https://tiktok.com/@demo/video/1") == "https://www.tiktok.com/@demo/video/1"

    @pytest.mark.parametrize(
        "url",
        [
            "https://www.tiktok.com/@demo",
            "https://www.tiktok.com/@demo/live",
            "https://www.tiktok.com/music/song-123",
            "https://www.tiktok.com/@demo/photo/1",
            "https://www.tiktok.com/sticker/1",
            "https://www.tiktok.com/tag/gaming",
            "https://www.tiktok.com/@demo/collection/1",
            "https://www.tiktok.com/share/live/1",
            "https://m.tiktok.com/@demo/video/1",
        ],
    )
    def test_validate_rejects_non_video_tiktok_pages(self, url: str) -> None:
        with pytest.raises(InvalidTikTokUrlError):
            validate_tiktok_url(url)

    def test_validate_rejects_non_tiktok_host(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)
            with pytest.raises(InvalidTikTokUrlError):
                downloader.validate_tiktok_url('https://example.com/video')

    def test_download_rejects_profile_before_invoking_ytdlp(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)
            with patch("backend.services.remote_clip_downloader.subprocess.run") as run:
                with pytest.raises(InvalidTikTokUrlError):
                    downloader.download_tiktok("https://www.tiktok.com/@jasontheween")

            run.assert_not_called()

    def test_download_raises_when_ytdlp_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)
            failed = subprocess.CompletedProcess(args=['yt-dlp'], returncode=1, stdout='', stderr='boom')
            with patch('backend.services.remote_clip_downloader.subprocess.run', return_value=failed):
                with pytest.raises(DownloadError):
                    downloader.download_tiktok('https://www.tiktok.com/@demo/video/1')

    def test_download_raises_when_output_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)
            ok = subprocess.CompletedProcess(args=['yt-dlp'], returncode=0, stdout='', stderr='')
            with patch('backend.services.remote_clip_downloader.subprocess.run', return_value=ok):
                with pytest.raises(DownloadError):
                    downloader.download_tiktok('https://www.tiktok.com/@demo/video/1')

    def test_download_raises_on_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)
            with patch('backend.services.remote_clip_downloader.subprocess.run', side_effect=subprocess.TimeoutExpired(cmd=['yt-dlp'], timeout=5)):
                with pytest.raises(DownloadError):
                    downloader.download_tiktok('https://www.tiktok.com/@demo/video/1')

    def test_download_returns_created_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)

            def fake_run(*args, **kwargs):
                out = os.path.join(tmp, 'tiktok_abc.mp4')
                with open(out, 'wb') as f:
                    f.write(b'video-bytes')
                return subprocess.CompletedProcess(args=['yt-dlp'], returncode=0, stdout='', stderr='')
            with patch('backend.services.remote_clip_downloader.uuid.uuid4', return_value=SimpleNamespace(hex='abc')):
                with patch('backend.services.remote_clip_downloader.subprocess.run', side_effect=fake_run):
                    result = downloader.download_tiktok('https://www.tiktok.com/@demo/video/1')
            assert os.path.exists(result.path)
            downloader.cleanup(result.path)
            assert not os.path.exists(result.path)

    def test_download_resolves_short_link_to_single_video_before_download(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)
            metadata = subprocess.CompletedProcess(
                args=["yt-dlp"],
                returncode=0,
                stdout=json.dumps(
                    {
                        "extractor_key": "TikTok",
                        "id": "1234567890",
                        "webpage_url": "https://www.tiktok.com/@demo/video/1234567890",
                    }
                ),
                stderr="",
            )
            calls = []

            def fake_run(cmd, **kwargs):
                calls.append((cmd, kwargs))
                if len(calls) == 2:
                    with open(os.path.join(tmp, "tiktok_abc.mp4"), "wb") as file:
                        file.write(b"video-bytes")
                    return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")
                return metadata

            with patch("backend.services.remote_clip_downloader.uuid.uuid4", return_value=SimpleNamespace(hex="abc")):
                with patch("backend.services.remote_clip_downloader.subprocess.run", side_effect=fake_run):
                    result = downloader.download_tiktok("https://www.tiktok.com/t/ZP8ctwC2V/")

            assert len(calls) == 2
            assert "--dump-single-json" in calls[0][0]
            assert calls[0][1]["timeout"] == 20
            assert "--dump-single-json" not in calls[1][0]
            downloader.cleanup(result.path)

    def test_download_rejects_short_link_that_resolves_to_playlist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)
            metadata = subprocess.CompletedProcess(
                args=["yt-dlp"],
                returncode=0,
                stdout=json.dumps(
                    {
                        "_type": "playlist",
                        "extractor_key": "TikTokUser",
                        "id": "demo",
                        "entries": [],
                    }
                ),
                stderr="",
            )
            with patch("backend.services.remote_clip_downloader.subprocess.run", return_value=metadata) as run:
                with pytest.raises(InvalidTikTokUrlError):
                    downloader.download_tiktok("https://www.tiktok.com/t/profile_code")

            run.assert_called_once()
