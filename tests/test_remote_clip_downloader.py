import http.client
import os
import pytest
import subprocess
import tempfile
from types import SimpleNamespace
from unittest.mock import patch
from backend.services.remote_clip_downloader import (
    DownloadError,
    InvalidClipUrlError,
    RemoteClipDownloader,
    parse_clip_url,
    validate_clip_url,
)

class TestRemoteClipDownloader:

    @pytest.mark.parametrize(
        "url, platform, normalized",
        [
            (" https://instagram.com/reel/Cx_123-abc/?igsh=tracking#fragment ", "instagram", "https://www.instagram.com/reel/Cx_123-abc/"),
            ("https://www.instagram.com/reels/Cx_123-abc", "instagram", "https://www.instagram.com/reels/Cx_123-abc"),
            ("http://m.youtube.com/shorts/BaW_jenozKc?si=share", "youtube", "https://www.youtube.com/shorts/BaW_jenozKc"),
            ("https://youtube.com/shorts/BaW_jenozKc/?list=playlist&t=12", "youtube", "https://www.youtube.com/shorts/BaW_jenozKc/"),
        ],
    )
    def test_detects_and_normalizes_reels_and_shorts(self, url, platform, normalized) -> None:
        clip = parse_clip_url(url)
        assert (clip.url, clip.platform, clip.kind) == (normalized, platform, "direct")

    @pytest.mark.parametrize(
        "url",
        [
            "https://www.instagram.com/demo/",
            "https://www.instagram.com/p/Cx123/",
            "https://www.instagram.com/stories/demo/123/",
            "https://www.instagram.com/reels/audio/123/",
            "https://www.instagram.com/reel/",
            "https://www.youtube.com/watch?v=BaW_jenozKc",
            "https://www.youtube.com/playlist?list=123",
            "https://www.youtube.com/@demo/shorts",
            "https://www.youtube.com/shorts/too-short",
            "https://youtu.be/BaW_jenozKc",
            "https://www.youtube.com.evil.test/shorts/BaW_jenozKc",
            "https://instagram.com.evil.test/reel/Cx123/",
            "https://user@www.instagram.com/reel/Cx123/",
            "https://www.youtube.com:443/shorts/BaW_jenozKc",
            "https://www.instagram.com:bad/reel/Cx123/",
            "https://www.instagram.com/reel/Cx123/;params",
            "file://www.youtube.com/shorts/BaW_jenozKc",
        ],
    )
    def test_rejects_unsupported_pages_without_downloading(self, url: str) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch("backend.services.remote_clip_downloader.subprocess.run") as run:
                with pytest.raises(InvalidClipUrlError):
                    RemoteClipDownloader(temp_dir=tmp).download_clip(url)
            run.assert_not_called()

    @pytest.mark.parametrize(
        "url, platform",
        [
            ("https://www.instagram.com/reel/Cx123/", "instagram"),
            ("https://www.youtube.com/shorts/BaW_jenozKc", "youtube"),
        ],
    )
    def test_downloads_new_platforms_without_tiktok_resolution(self, url, platform) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)

            def fake_run(cmd, **kwargs):
                output_template = cmd[cmd.index("-o") + 1]
                with open(output_template.replace("%(ext)s", "m4a"), "wb") as file:
                    file.write(b"audio-bytes")
                return subprocess.CompletedProcess(args=cmd, returncode=0)

            with patch.object(downloader, "_request_tiktok_redirect") as resolve:
                with patch("backend.services.remote_clip_downloader.subprocess.run", side_effect=fake_run) as run:
                    result = downloader.download_clip(url)
            resolve.assert_not_called()
            cmd = run.call_args.args[0]
            assert cmd[-2:] == ["--", url]
            assert "--ignore-config" in cmd and "--no-playlist" in cmd
            if platform == "youtube":
                assert cmd[cmd.index("--format") + 1] == "bestaudio/best"
                assert cmd[cmd.index("--js-runtimes") + 1] == "node"
            with open(result.path, "rb") as file:
                assert file.read() == b"audio-bytes"
            downloader.cleanup(result.path)
            assert os.listdir(tmp) == []

    @pytest.mark.parametrize("failure", ["error", "timeout", "oversize"])
    def test_cleans_partial_downloads_on_failure(self, failure) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp, max_file_mb=0)

            def fake_run(cmd, **kwargs):
                template = cmd[cmd.index("-o") + 1]
                with open(template.replace("%(ext)s", "m4a.part"), "wb") as file:
                    file.write(b"partial")
                if failure == "timeout":
                    raise subprocess.TimeoutExpired(cmd, 1)
                if failure == "oversize":
                    with open(template.replace("%(ext)s", "m4a"), "wb") as file:
                        file.write(b"audio")
                    return subprocess.CompletedProcess(cmd, 0)
                return subprocess.CompletedProcess(cmd, 1, stderr="provider unavailable")

            with patch("backend.services.remote_clip_downloader.subprocess.run", side_effect=fake_run):
                with pytest.raises(DownloadError):
                    downloader.download_clip("https://www.youtube.com/shorts/BaW_jenozKc")
            assert os.listdir(tmp) == []

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
        assert validate_clip_url(url) == url

    def test_validate_normalizes_bare_tiktok_host(self) -> None:
        assert validate_clip_url("https://tiktok.com/@demo/video/1") == "https://www.tiktok.com/@demo/video/1"

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
        with pytest.raises(InvalidClipUrlError):
            validate_clip_url(url)

    def test_validate_rejects_non_tiktok_host(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)
            with pytest.raises(InvalidClipUrlError):
                validate_clip_url('https://example.com/video')

    def test_download_rejects_profile_before_invoking_ytdlp(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)
            with patch("backend.services.remote_clip_downloader.subprocess.run") as run:
                with pytest.raises(InvalidClipUrlError):
                    downloader.download_clip("https://www.tiktok.com/@jasontheween")

            run.assert_not_called()

    def test_download_raises_when_ytdlp_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)
            failed = subprocess.CompletedProcess(args=['yt-dlp'], returncode=1, stdout='', stderr='boom')
            with patch('backend.services.remote_clip_downloader.subprocess.run', return_value=failed):
                with pytest.raises(DownloadError, match="^boom$"):
                    downloader.download_clip('https://www.tiktok.com/@demo/video/1')

    def test_download_reports_login_required_without_ytdlp_instructions(self, caplog) -> None:
        stderr = (
            "WARNING: [TikTok] The extractor is attempting impersonation, but no "
            "impersonate target is available. If you encounter errors, then see "
            "https://github.com/yt-dlp/yt-dlp#impersonation for information on "
            "installing the required dependencies\n"
            "ERROR: [TikTok] 7693712809221246222: This post may not be comfortable "
            "for some audiences. Log in for access. Use --cookies-from-browser or "
            "--cookies for the authentication. See "
            "https://github.com/yt-dlp/yt-dlp/wiki/FAQ#how-do-i-pass-cookies-to-yt-dlp "
            "for how to manually pass cookies"
        )
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)
            failed = subprocess.CompletedProcess(
                args=["yt-dlp"], returncode=1, stdout="", stderr=stderr,
            )
            with patch("backend.services.remote_clip_downloader.subprocess.run", return_value=failed):
                with pytest.raises(DownloadError) as error:
                    downloader.download_clip(
                        "https://www.tiktok.com/@realstableronaldo/video/7693712809221246222"
                    )

        assert str(error.value) == (
            "This TikTok requires login, so VodHunter cannot download it. "
            "Try a publicly accessible TikTok video."
        )
        assert stderr in caplog.text

    def test_download_raises_when_output_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)
            ok = subprocess.CompletedProcess(args=['yt-dlp'], returncode=0, stdout='', stderr='')
            with patch('backend.services.remote_clip_downloader.subprocess.run', return_value=ok):
                with pytest.raises(DownloadError):
                    downloader.download_clip('https://www.tiktok.com/@demo/video/1')

    def test_download_raises_on_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            downloader = RemoteClipDownloader(temp_dir=tmp)
            with patch('backend.services.remote_clip_downloader.subprocess.run', side_effect=subprocess.TimeoutExpired(cmd=['yt-dlp'], timeout=5)):
                with pytest.raises(DownloadError):
                    downloader.download_clip('https://www.tiktok.com/@demo/video/1')

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
                    result = downloader.download_clip('https://www.tiktok.com/@demo/video/1')
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
                        result = downloader.download_clip("https://www.tiktok.com/t/ZP8ctwC2V/")

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
                        downloader.download_clip("https://www.tiktok.com/t/ZP8ctwC2V/")

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
                    with pytest.raises(InvalidClipUrlError):
                        downloader.download_clip("https://www.tiktok.com/t/ZP8ctwC2V/")

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
                    with pytest.raises(InvalidClipUrlError):
                        downloader.download_clip("https://www.tiktok.com/t/profile_code")

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
                with pytest.raises(InvalidClipUrlError, match="redirect loop"):
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
                with pytest.raises(InvalidClipUrlError, match="too many"):
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
