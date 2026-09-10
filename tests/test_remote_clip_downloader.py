import http.client
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

    def test_download_resolves_short_link_before_one_download(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)
            canonical_url = "https://www.tiktok.com/@demo/video/1234567890?_r=1&_t=share"
            calls = []

            def fake_run(cmd, **kwargs):
                calls.append((cmd, kwargs))
                with open(os.path.join(tmp, "tiktok_abc.mp4"), "wb") as file:
                    file.write(b"video-bytes")
                return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

            with patch("backend.services.remote_clip_downloader.uuid.uuid4", return_value=SimpleNamespace(hex="abc")):
                with patch.object(
                    downloader,
                    "_request_tiktok_redirect",
                    return_value=(301, canonical_url),
                ) as resolve:
                    with patch("backend.services.remote_clip_downloader.subprocess.run", side_effect=fake_run):
                        result = downloader.download_tiktok("https://www.tiktok.com/t/ZP8ctwC2V/")

            resolve.assert_called_once_with("https://www.tiktok.com/t/ZP8ctwC2V/")
            assert len(calls) == 1
            assert "--ignore-config" in calls[0][0]
            assert "--" in calls[0][0]
            assert canonical_url in calls[0][0]
            downloader.cleanup(result.path)

    def test_download_resolves_multiple_short_redirects(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)
            responses = [
                (302, "https://vm.tiktok.com/ZTR45GpSF/"),
                (301, "https://www.tiktok.com/@demo/video/1234567890"),
            ]

            with patch.object(downloader, "_request_tiktok_redirect", side_effect=responses) as resolve:
                with patch("backend.services.remote_clip_downloader.subprocess.run") as run:
                    run.return_value = subprocess.CompletedProcess(args=["yt-dlp"], returncode=1, stdout="", stderr="boom")
                    with pytest.raises(DownloadError):
                        downloader.download_tiktok("https://www.tiktok.com/t/ZP8ctwC2V/")

            assert resolve.call_count == 2
            run.assert_called_once()
            assert run.call_args.args[0][-1] == "https://www.tiktok.com/@demo/video/1234567890"

    def test_download_rejects_short_link_redirect_to_external_host(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)
            with patch.object(
                downloader,
                "_request_tiktok_redirect",
                return_value=(301, "https://example.com/@demo/video/1234567890"),
            ) as resolve:
                with patch("backend.services.remote_clip_downloader.subprocess.run") as run:
                    with pytest.raises(InvalidTikTokUrlError):
                        downloader.download_tiktok("https://www.tiktok.com/t/ZP8ctwC2V/")

            resolve.assert_called_once()
            run.assert_not_called()

    def test_download_rejects_short_link_redirect_to_non_video(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)
            with patch.object(
                downloader,
                "_request_tiktok_redirect",
                return_value=(301, "https://www.tiktok.com/@demo"),
            ) as resolve:
                with patch("backend.services.remote_clip_downloader.subprocess.run") as run:
                    with pytest.raises(InvalidTikTokUrlError):
                        downloader.download_tiktok("https://www.tiktok.com/t/profile_code")

            resolve.assert_called_once()
            run.assert_not_called()

    def test_short_resolver_retries_remote_disconnect(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(
                temp_dir=tmp,
                resolve_retry_delay_seconds=0,
            )
            canonical_url = "https://www.tiktok.com/@demo/video/1234567890"
            with patch.object(
                downloader,
                "_request_tiktok_redirect",
                side_effect=[http.client.RemoteDisconnected("closed"), (301, canonical_url)],
            ) as resolve:
                with patch("backend.services.remote_clip_downloader.time.sleep") as sleep:
                    assert downloader._resolve_short_tiktok_url("https://www.tiktok.com/t/ZP8ctwC2V/") == canonical_url

            assert resolve.call_count == 2
            sleep.assert_called_once_with(0)

    def test_short_resolver_retries_temporary_http_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(
                temp_dir=tmp,
                resolve_retry_delay_seconds=0,
            )
            canonical_url = "https://www.tiktok.com/@demo/video/1234567890"
            with patch.object(
                downloader,
                "_request_tiktok_redirect",
                side_effect=[(503, None), (301, canonical_url)],
            ) as resolve:
                with patch("backend.services.remote_clip_downloader.time.sleep"):
                    assert downloader._resolve_short_tiktok_url("https://www.tiktok.com/t/ZP8ctwC2V/") == canonical_url

            assert resolve.call_count == 2

    def test_short_resolver_fails_after_three_transient_attempts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(
                temp_dir=tmp,
                resolve_retry_delay_seconds=0,
            )
            with patch.object(
                downloader,
                "_request_tiktok_redirect",
                side_effect=http.client.RemoteDisconnected("closed"),
            ) as resolve:
                with patch("backend.services.remote_clip_downloader.time.sleep") as sleep:
                    with pytest.raises(DownloadError, match="could not be resolved"):
                        downloader._resolve_short_tiktok_url("https://www.tiktok.com/t/ZP8ctwC2V/")

            assert resolve.call_count == 3
            assert sleep.call_count == 2

    def test_short_resolver_rejects_redirect_loop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)
            with patch.object(
                downloader,
                "_request_tiktok_redirect",
                return_value=(301, "https://www.tiktok.com/t/ZP8ctwC2V/"),
            ) as resolve:
                with pytest.raises(InvalidTikTokUrlError, match="redirect loop"):
                    downloader._resolve_short_tiktok_url("https://www.tiktok.com/t/ZP8ctwC2V/")

            resolve.assert_called_once()

    def test_short_resolver_rejects_more_than_maximum_redirects(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp, max_resolve_redirects=2)
            responses = [
                (301, "https://www.tiktok.com/t/second"),
                (301, "https://www.tiktok.com/t/third"),
            ]
            with patch.object(downloader, "_request_tiktok_redirect", side_effect=responses) as resolve:
                with pytest.raises(InvalidTikTokUrlError, match="too many"):
                    downloader._resolve_short_tiktok_url("https://www.tiktok.com/t/first")

            assert resolve.call_count == 2

    def test_short_resolver_accepts_relative_location(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)
            canonical_url = "https://www.tiktok.com/@demo/video/1234567890"
            with patch.object(
                downloader,
                "_request_tiktok_redirect",
                return_value=(301, "/@demo/video/1234567890"),
            ):
                assert downloader._resolve_short_tiktok_url("https://www.tiktok.com/t/ZP8ctwC2V/") == canonical_url

    def test_request_tiktok_redirect_uses_get_without_following_redirect(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)

            class FakeResponse:
                status = 301

                def getheader(self, name):
                    assert name == "Location"
                    return "https://www.tiktok.com/@demo/video/1234567890"

                def close(self):
                    pass

            class FakeConnection:
                def __init__(self):
                    self.request_args = None
                    self.closed = False

                def request(self, method, target, headers):
                    self.request_args = (method, target, headers)

                def getresponse(self):
                    return FakeResponse()

                def close(self):
                    self.closed = True

            connection = FakeConnection()
            with patch(
                "backend.services.remote_clip_downloader.http.client.HTTPSConnection",
                return_value=connection,
            ) as connection_factory:
                status, location = downloader._request_tiktok_redirect(
                    "https://www.tiktok.com/t/ZP8ctwC2V/?x=1"
                )

            connection_factory.assert_called_once_with("www.tiktok.com", timeout=10.0)
            assert status == 301
            assert location == "https://www.tiktok.com/@demo/video/1234567890"
            assert connection.request_args[0] == "GET"
            assert connection.request_args[1] == "/t/ZP8ctwC2V/?x=1"
            assert connection.request_args[2]["Connection"] == "close"
            assert connection.request_args[2]["User-Agent"]
            assert connection.closed
