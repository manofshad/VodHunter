import hashlib
import json
import shutil
import subprocess
import wave
from pathlib import Path
from unittest.mock import patch

import httpx
import numpy as np
import pytest

from backend.services.remote_clip_downloader import (
    CobaltDownloadError,
    RemoteClipDownloader,
)
from search.query_preprocessor import QueryPreprocessor


VIDEO_URL = "https://www.tiktok.com/@demo/video/123456789"
API_URL = "http://cobalt:9000/"
TUNNEL_URL = "http://cobalt:9000/tunnel?id=test"
MP4 = b"\x00\x00\x00\x18ftypisom" + b"mixed-soundtrack" * 20


def make_downloader(tmp_path: Path, handler, **kwargs) -> RemoteClipDownloader:
    return RemoteClipDownloader(
        temp_dir=str(tmp_path),
        downloader="cobalt",
        cobalt_api_url=API_URL,
        cobalt_client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
        **kwargs,
    )


def resolver_response(status="tunnel", url=TUNNEL_URL, filename="clip.mp4"):
    return httpx.Response(
        200,
        headers={"content-type": "application/json"},
        json={"status": status, "url": url, "filename": filename},
    )


@pytest.mark.parametrize("status,url", [
    ("tunnel", TUNNEL_URL),
    ("redirect", "https://v16.tiktokcdn.com/media/clip.mp4"),
])
def test_streams_normal_mp4_and_records_timings(tmp_path, status, url):
    requests = []

    def handler(request):
        requests.append(request)
        if request.method == "POST":
            assert request.url == API_URL
            assert request.headers["accept"] == "application/json"
            assert json.loads(request.content) == {
                "url": VIDEO_URL,
                "downloadMode": "auto",
                "alwaysProxy": True,
            }
            return resolver_response(status, url)
        assert str(request.url) == url
        return httpx.Response(200, headers={"content-type": "video/mp4", "content-length": str(len(MP4))}, content=MP4)

    downloader = make_downloader(tmp_path, handler)
    result = downloader.download_tiktok(VIDEO_URL)
    assert Path(result.path).read_bytes() == MP4
    assert result.path.endswith(".mp4")
    assert result.provider == "cobalt"
    assert result.size_bytes == len(MP4)
    assert result.cobalt_resolution_ms is not None
    assert result.media_transfer_ms is not None
    assert result.download_duration_ms is not None
    assert len(requests) == 2
    assert not list(tmp_path.glob("*.part"))
    downloader.cleanup(result.path)
    assert not list(tmp_path.iterdir())


def test_short_share_url_is_resolved_before_cobalt(tmp_path):
    calls = []

    def handler(request):
        if request.method == "POST":
            calls.append(json.loads(request.content)["url"])
            return resolver_response()
        return httpx.Response(200, headers={"content-type": "video/mp4"}, content=MP4)

    downloader = make_downloader(tmp_path, handler)
    downloader._request_tiktok_redirect = lambda _: (301, VIDEO_URL)
    result = downloader.download_tiktok("https://vm.tiktok.com/Z123/")
    assert calls == [VIDEO_URL]
    downloader.cleanup(result.path)


@pytest.mark.parametrize("resolver", [
    resolver_response("error"),
    resolver_response("picker"),
    resolver_response("local-processing"),
    resolver_response("unknown"),
    resolver_response("tunnel", "http://other:9000/tunnel?id=bad"),
    resolver_response("redirect", "http://localhost:9000/video.mp4"),
    resolver_response("tunnel", TUNNEL_URL, "audio.mp3"),
    httpx.Response(200, headers={"content-type": "application/json"}, content=b""),
    httpx.Response(200, headers={"content-type": "text/html"}, content=b"<html>bad</html>"),
    httpx.Response(503, headers={"content-type": "application/json"}, json={"status": "error"}),
])
def test_rejects_bad_resolver_response_and_leaves_no_file(tmp_path, resolver):
    downloader = make_downloader(tmp_path, lambda _: resolver)
    with pytest.raises(CobaltDownloadError):
        downloader.download_tiktok(VIDEO_URL)
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("media_response,category", [
    (httpx.Response(200, headers={"content-type": "video/mp4"}, content=b""), "empty"),
    (httpx.Response(200, headers={"content-type": "text/html"}, content=b"<html>"), "invalid_mime"),
    (httpx.Response(200, headers={"content-type": "video/mp4", "content-length": "9999"}, content=MP4), "truncated"),
    (httpx.Response(200, headers={"content-type": "video/mp4"}, content=MP4 * 5000), "oversized"),
    (httpx.Response(404, content=b"missing"), "media_http"),
])
def test_rejects_bad_media_and_cleans_partial_file(tmp_path, media_response, category):
    def handler(request):
        return resolver_response() if request.method == "POST" else media_response

    downloader = make_downloader(tmp_path, handler, max_file_mb=1 if category == "oversized" else 200)
    with pytest.raises(CobaltDownloadError) as error:
        downloader.download_tiktok(VIDEO_URL)
    assert error.value.category == category
    assert not list(tmp_path.iterdir())


def test_rejects_oversized_declared_length_before_read(tmp_path):
    def handler(request):
        if request.method == "POST":
            return resolver_response()
        return httpx.Response(200, headers={"content-type": "video/mp4", "content-length": str(201 * 1024 * 1024)}, content=b"small")

    downloader = make_downloader(tmp_path, handler)
    with pytest.raises(CobaltDownloadError, match="size limit"):
        downloader.download_tiktok(VIDEO_URL)
    assert not list(tmp_path.iterdir())


def test_interrupted_declared_transfer_is_truncated_and_cleaned(tmp_path):
    class BrokenStream(httpx.SyncByteStream):
        def __iter__(self):
            yield MP4[:32]
            raise httpx.RemoteProtocolError("connection closed mid-body")

    def handler(request):
        if request.method == "POST":
            return resolver_response()
        return httpx.Response(
            200,
            headers={"content-type": "video/mp4", "content-length": str(len(MP4))},
            stream=BrokenStream(),
        )

    downloader = make_downloader(tmp_path, handler)
    with pytest.raises(CobaltDownloadError) as error:
        downloader.download_tiktok(VIDEO_URL)
    assert error.value.category == "truncated"
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("stage", ["POST", "GET"])
@pytest.mark.parametrize("failure,category", [
    (httpx.ConnectError("offline"), "connection"),
    (httpx.ReadTimeout("timeout"), "timeout"),
])
def test_connection_and_timeout_failures(tmp_path, stage, failure, category):
    def handler(request):
        if request.method == stage:
            raise failure
        return resolver_response()

    downloader = make_downloader(tmp_path, handler)
    with pytest.raises(CobaltDownloadError) as error:
        downloader.download_tiktok(VIDEO_URL)
    assert error.value.category == category
    assert not list(tmp_path.iterdir())


def test_media_http_redirect_is_bounded_and_validated(tmp_path):
    def handler(request):
        if request.method == "POST":
            return resolver_response()
        if request.url.host == "cobalt":
            return httpx.Response(302, headers={"location": "https://v16.tiktokcdn.com/clip.mp4"})
        return httpx.Response(200, headers={"content-type": "application/octet-stream"}, content=MP4)

    downloader = make_downloader(tmp_path, handler)
    result = downloader.download_tiktok(VIDEO_URL)
    assert Path(result.path).read_bytes() == MP4
    downloader.cleanup(result.path)


def test_cobalt_failure_does_not_call_ytdlp(tmp_path):
    downloader = make_downloader(tmp_path, lambda _: httpx.Response(503))
    with patch("backend.services.remote_clip_downloader.subprocess.run") as run:
        with pytest.raises(CobaltDownloadError):
            downloader.download_tiktok(VIDEO_URL)
    run.assert_not_called()


@pytest.mark.parametrize("api_url", ["", "ftp://cobalt:9000/", "http://cobalt:bad/", "http://cobalt:9000/other"])
def test_cobalt_api_url_must_be_service_root(tmp_path, api_url):
    with pytest.raises(ValueError, match="COBALT_API_URL"):
        RemoteClipDownloader(temp_dir=str(tmp_path), downloader="cobalt", cobalt_api_url=api_url)


@pytest.mark.skipif(shutil.which("ffmpeg") is None, reason="ffmpeg required")
def test_complete_mp4_reaches_existing_ffmpeg_mixed_audio_path(tmp_path):
    source = tmp_path / "fixture.mp4"
    subprocess.run([
        "ffmpeg", "-loglevel", "error", "-f", "lavfi", "-i", "color=c=black:s=64x64:r=10:d=3",
        "-f", "lavfi", "-i", "sine=frequency=440:duration=3",
        "-f", "lavfi", "-i", "sine=frequency=660:duration=3",
        "-filter_complex", "[1:a][2:a]amix=inputs=2:duration=longest[a]",
        "-map", "0:v", "-map", "[a]", "-c:v", "mpeg4", "-c:a", "aac", "-y", str(source),
    ], check=True, capture_output=True)
    media = source.read_bytes()

    def handler(request):
        return resolver_response() if request.method == "POST" else httpx.Response(
            200, headers={"content-type": "video/mp4", "content-length": str(len(media))}, content=media
        )

    downloader = make_downloader(tmp_path / "downloads", handler)
    result = downloader.download_tiktok(VIDEO_URL)
    preprocessor = QueryPreprocessor(str(tmp_path / "normalized"))
    baseline = preprocessor.prepare(str(source))
    actual = preprocessor.prepare(result.path)
    with wave.open(actual, "rb") as audio:
        assert audio.getframerate() == 8000
        assert audio.getnchannels() == 1
        assert audio.getsampwidth() == 2
        assert audio.getnframes() / audio.getframerate() == pytest.approx(3, abs=0.1)
        samples = np.frombuffer(audio.readframes(audio.getnframes()), dtype="<i2")
    window = samples[:8000]
    spectrum = np.abs(np.fft.rfft(window))
    assert spectrum[440] > 100_000
    assert spectrum[660] > 100_000
    assert hashlib.sha256(Path(actual).read_bytes()).digest() == hashlib.sha256(Path(baseline).read_bytes()).digest()
    downloader.cleanup(result.path)
    preprocessor.cleanup(actual)
    preprocessor.cleanup(baseline)
    assert not list((tmp_path / "downloads").iterdir())
