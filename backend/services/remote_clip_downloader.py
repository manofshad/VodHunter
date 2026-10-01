import glob
import http.client
import ipaddress
import json
import logging
import os
import re
import subprocess
import time
import uuid
from dataclasses import dataclass
from typing import Callable, Literal
from urllib.parse import urljoin, urlparse, urlunparse

import httpx

from backend.observability import observe_tiktok_download

logger = logging.getLogger("uvicorn.error")


class InvalidTikTokUrlError(Exception):
    pass


class DownloadError(Exception):
    pass


class CobaltDownloadError(DownloadError):
    def __init__(self, message: str, category: str):
        super().__init__(message)
        self.category = category


class _TransientTikTokResolveError(Exception):
    pass


@dataclass(frozen=True)
class DownloadResult:
    path: str
    provider: str = "yt-dlp"
    size_bytes: int | None = None
    cobalt_resolution_ms: int | None = None
    media_transfer_ms: int | None = None
    download_duration_ms: int | None = None


@dataclass(frozen=True)
class TikTokUrl:
    url: str
    kind: Literal["direct", "short"]


_DIRECT_TIKTOK_HOSTS = {
    "tiktok.com",
    "www.tiktok.com",
    "www.tiktokv.com",
}
_SHORT_TIKTOK_HOSTS = {
    "tiktok.com",
    "www.tiktok.com",
    "vm.tiktok.com",
    "vt.tiktok.com",
}
_DIRECT_VIDEO_PATHS = (
    re.compile(r"/@(?:[A-Za-z0-9_.-]+)?/video/[0-9]+/?"),
    re.compile(r"/share/video/[0-9]+/?"),
    re.compile(r"/embed/[0-9]+/?"),
)
_CANONICAL_VIDEO_PATH = re.compile(
    r"/@(?P<username>[A-Za-z0-9_.-]+)/video/(?P<video_id>[0-9]+)/?"
)
_TIKTOK_SHARE_PATH = re.compile(r"/t/[A-Za-z0-9_]+/?")
_TIKTOK_VM_SHARE_PATH = re.compile(r"/[A-Za-z0-9_]+/?")
_TIKTOK_REDIRECT_STATUSES = {301, 302, 303, 307, 308}
_TIKTOK_TRANSIENT_STATUS_CODES = {408, 429, *range(500, 600)}
_TIKTOK_RESOLVER_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)
_SHORT_LINK_TARGET_ERROR = "TikTok short link must resolve to a single TikTok video"
_TRANSIENT_RESOLVE_EXCEPTIONS = (
    _TransientTikTokResolveError,
    http.client.HTTPException,
    OSError,
    TimeoutError,
)
_COBALT_MAX_JSON_BYTES = 64 * 1024
_COBALT_MEDIA_REDIRECTS = 3


def _elapsed_ms(started_at: float) -> int:
    return max(0, round((time.perf_counter() - started_at) * 1000))


def _same_origin(left: str, right: str) -> bool:
    try:
        a, b = urlparse(left), urlparse(right)
        return (a.scheme, a.hostname, a.port) == (b.scheme, b.hostname, b.port)
    except ValueError:
        return False


def _validate_cobalt_media_url(url: object, status: str, api_url: str) -> str:
    if not isinstance(url, str):
        raise CobaltDownloadError("Cobalt returned no media URL", "invalid_response")
    try:
        parsed = urlparse(url)
        host, port = parsed.hostname, parsed.port
    except ValueError as exc:
        raise CobaltDownloadError("Cobalt returned an invalid media URL", "invalid_response") from exc
    if not host or parsed.username or parsed.password or parsed.fragment:
        raise CobaltDownloadError("Cobalt returned an invalid media URL", "invalid_response")
    if status == "tunnel":
        if not _same_origin(url, api_url) or parsed.path != "/tunnel":
            raise CobaltDownloadError("Cobalt returned an invalid tunnel URL", "invalid_response")
    else:
        if parsed.scheme != "https" or port not in (None, 443):
            raise CobaltDownloadError("Cobalt returned an unsafe redirect URL", "invalid_response")
        if host == "localhost" or host.endswith((".localhost", ".local", ".internal")):
            raise CobaltDownloadError("Cobalt returned an unsafe redirect URL", "invalid_response")
        try:
            if not ipaddress.ip_address(host).is_global:
                raise CobaltDownloadError("Cobalt returned an unsafe redirect URL", "invalid_response")
        except ValueError:
            pass
    return url


class CobaltTikTokDownloader:
    """Download Cobalt's normal MP4 response into the search temporary directory."""

    def __init__(
        self,
        *,
        api_url: str,
        temp_dir: str,
        timeout_seconds: int,
        max_file_mb: int | None,
        resolve_timeout_seconds: float,
        client_factory: Callable[[], httpx.Client] | None = None,
    ):
        try:
            parsed = urlparse(api_url)
            parsed.port
        except ValueError as exc:
            raise ValueError("COBALT_API_URL must be an HTTP(S) service root") from exc
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.path not in {"", "/"}
            or parsed.params
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("COBALT_API_URL must be an HTTP(S) service root")
        self.api_url = api_url.rstrip("/") + "/"
        self.temp_dir = temp_dir
        self.timeout_seconds = timeout_seconds
        self.max_bytes = None if max_file_mb is None else int(max_file_mb) * 1024 * 1024
        self.resolve_timeout_seconds = resolve_timeout_seconds
        self.client_factory = client_factory or (lambda: httpx.Client(follow_redirects=False, trust_env=False))

    @staticmethod
    def _timeout(deadline: float, cap: float) -> httpx.Timeout:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise CobaltDownloadError("Cobalt download timed out", "timeout")
        bounded = min(remaining, cap)
        return httpx.Timeout(connect=min(bounded, 5.0), read=bounded, write=bounded, pool=bounded)

    def _resolve(self, client: httpx.Client, url: str, deadline: float) -> tuple[str, str]:
        try:
            with client.stream(
                "POST",
                self.api_url,
                json={"url": url, "downloadMode": "auto", "alwaysProxy": True},
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                timeout=self._timeout(deadline, self.resolve_timeout_seconds),
            ) as response:
                if response.status_code != 200:
                    logger.warning("cobalt_resolver_http_status status=%d", response.status_code)
                    raise CobaltDownloadError("Cobalt could not resolve the TikTok clip", "resolver_http")
                if response.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
                    raise CobaltDownloadError("Cobalt returned an invalid resolver response", "invalid_response")
                body = bytearray()
                for chunk in response.iter_bytes():
                    if time.monotonic() >= deadline:
                        raise CobaltDownloadError("Cobalt download timed out", "timeout")
                    body.extend(chunk)
                    if len(body) > _COBALT_MAX_JSON_BYTES:
                        raise CobaltDownloadError("Cobalt resolver response was too large", "invalid_response")
            payload = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as exc:
            raise CobaltDownloadError("Cobalt returned invalid JSON", "invalid_response") from exc
        except httpx.TimeoutException as exc:
            raise CobaltDownloadError("Cobalt resolver timed out", "timeout") from exc
        except httpx.RequestError as exc:
            raise CobaltDownloadError("Cobalt resolver connection failed", "connection") from exc
        if not isinstance(payload, dict):
            raise CobaltDownloadError("Cobalt returned an invalid resolver response", "invalid_response")
        status = payload.get("status")
        if status == "error":
            raise CobaltDownloadError("Cobalt could not resolve the TikTok clip", "cobalt_error")
        if status not in {"tunnel", "redirect"}:
            raise CobaltDownloadError("Cobalt returned an unsupported media status", "unsupported_status")
        filename = payload.get("filename")
        if not isinstance(filename, str) or not filename.lower().endswith(".mp4"):
            raise CobaltDownloadError("Cobalt did not return an MP4", "invalid_response")
        return status, _validate_cobalt_media_url(payload.get("url"), status, self.api_url)

    def _transfer(self, client: httpx.Client, media_url: str, token: str, deadline: float) -> tuple[str, int]:
        partial_path = os.path.join(self.temp_dir, f"tiktok_{token}.mp4.part")
        final_path = os.path.join(self.temp_dir, f"tiktok_{token}.mp4")
        current_url = media_url
        try:
            for redirect_count in range(_COBALT_MEDIA_REDIRECTS + 1):
                try:
                    with client.stream(
                        "GET",
                        current_url,
                        headers={"Accept": "video/mp4,application/octet-stream", "Accept-Encoding": "identity"},
                        timeout=self._timeout(deadline, min(5.0, float(self.timeout_seconds))),
                    ) as response:
                        if response.status_code in _TIKTOK_REDIRECT_STATUSES:
                            if redirect_count == _COBALT_MEDIA_REDIRECTS or not response.headers.get("location"):
                                raise CobaltDownloadError("Cobalt media redirected too many times", "invalid_response")
                            next_url = urljoin(current_url, response.headers["location"])
                            next_status = "tunnel" if _same_origin(next_url, self.api_url) else "redirect"
                            current_url = _validate_cobalt_media_url(next_url, next_status, self.api_url)
                            continue
                        if response.status_code != 200:
                            logger.warning("cobalt_media_http_status status=%d", response.status_code)
                            raise CobaltDownloadError("Cobalt media transfer failed", "media_http")
                        mime = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
                        if mime not in {"video/mp4", "application/mp4", "application/octet-stream"}:
                            raise CobaltDownloadError("Cobalt returned an invalid media type", "invalid_mime")
                        declared_length = response.headers.get("content-length")
                        try:
                            expected_bytes = int(declared_length) if declared_length is not None else None
                        except ValueError as exc:
                            raise CobaltDownloadError("Cobalt returned an invalid media length", "invalid_response") from exc
                        if expected_bytes is not None and expected_bytes < 0:
                            raise CobaltDownloadError("Cobalt returned an invalid media length", "invalid_response")
                        if self.max_bytes is not None and expected_bytes is not None and expected_bytes > self.max_bytes:
                            raise CobaltDownloadError("Downloaded file exceeds size limit", "oversized")
                        size_bytes = 0
                        with open(partial_path, "xb") as output:
                            try:
                                for chunk in response.iter_bytes():
                                    if time.monotonic() >= deadline:
                                        raise CobaltDownloadError("Cobalt download timed out", "timeout")
                                    size_bytes += len(chunk)
                                    if self.max_bytes is not None and size_bytes > self.max_bytes:
                                        raise CobaltDownloadError("Downloaded file exceeds size limit", "oversized")
                                    output.write(chunk)
                            except httpx.RemoteProtocolError as exc:
                                if expected_bytes is not None and size_bytes < expected_bytes:
                                    raise CobaltDownloadError("Cobalt media transfer was truncated", "truncated") from exc
                                raise
                        if size_bytes == 0:
                            raise CobaltDownloadError("Downloaded file is empty", "empty")
                        if expected_bytes is not None and size_bytes != expected_bytes:
                            raise CobaltDownloadError("Cobalt media transfer was truncated", "truncated")
                        os.replace(partial_path, final_path)
                        return final_path, size_bytes
                except httpx.TimeoutException as exc:
                    raise CobaltDownloadError("Cobalt media transfer timed out", "timeout") from exc
                except httpx.RequestError as exc:
                    raise CobaltDownloadError("Cobalt media connection failed", "connection") from exc
                except OSError as exc:
                    raise CobaltDownloadError("Could not save the downloaded MP4", "file_error") from exc
            raise CobaltDownloadError("Cobalt media redirected too many times", "invalid_response")
        finally:
            if os.path.exists(partial_path):
                os.remove(partial_path)

    def download(self, url: str, started_at: float) -> DownloadResult:
        deadline = time.monotonic() + self.timeout_seconds
        token = uuid.uuid4().hex
        with self.client_factory() as client:
            resolution_started_at = time.perf_counter()
            _, media_url = self._resolve(client, url, deadline)
            resolution_ms = _elapsed_ms(resolution_started_at)
            transfer_started_at = time.perf_counter()
            path, size_bytes = self._transfer(client, media_url, token, deadline)
            transfer_ms = _elapsed_ms(transfer_started_at)
        return DownloadResult(
            path=path,
            provider="cobalt",
            size_bytes=size_bytes,
            cobalt_resolution_ms=resolution_ms,
            media_transfer_ms=transfer_ms,
            download_duration_ms=_elapsed_ms(started_at),
        )


def _invalid_path_error() -> InvalidTikTokUrlError:
    return InvalidTikTokUrlError(
        "TikTok URL must point to a single video; profile, live, music, photo, "
        "effect, hashtag, and collection links are not supported"
    )


def parse_tiktok_url(raw_url: str) -> TikTokUrl:
    url = (raw_url or "").strip()
    if not url:
        raise InvalidTikTokUrlError("TikTok URL is required")

    try:
        parsed = urlparse(url)
        port = parsed.port
    except ValueError as exc:
        raise InvalidTikTokUrlError("TikTok URL is malformed") from exc

    if parsed.scheme.lower() not in {"http", "https"}:
        raise InvalidTikTokUrlError("TikTok URL must use http or https")

    host = (parsed.hostname or "").lower()
    if host not in _DIRECT_TIKTOK_HOSTS and host not in _SHORT_TIKTOK_HOSTS:
        raise InvalidTikTokUrlError("Only TikTok URLs are supported")
    if parsed.username or parsed.password or "@" in parsed.netloc or port is not None or parsed.params:
        raise InvalidTikTokUrlError("TikTok URL is malformed")

    is_direct_video = host in _DIRECT_TIKTOK_HOSTS and any(
        pattern.fullmatch(parsed.path) for pattern in _DIRECT_VIDEO_PATHS
    )
    is_short_share = (
        host in {"tiktok.com", "www.tiktok.com"}
        and _TIKTOK_SHARE_PATH.fullmatch(parsed.path)
    ) or (
        host in {"vm.tiktok.com", "vt.tiktok.com"}
        and _TIKTOK_VM_SHARE_PATH.fullmatch(parsed.path)
    )

    if not is_direct_video and not is_short_share:
        raise _invalid_path_error()

    normalized_host = "www.tiktok.com" if host == "tiktok.com" else host
    normalized_url = urlunparse(
        (
            parsed.scheme.lower(),
            normalized_host,
            parsed.path,
            parsed.params,
            parsed.query,
            parsed.fragment,
        )
    )
    return TikTokUrl(url=normalized_url, kind="direct" if is_direct_video else "short")


def validate_tiktok_url(raw_url: str) -> str:
    return parse_tiktok_url(raw_url).url


class RemoteClipDownloader:

    def __init__(
        self,
        temp_dir: str,
        timeout_seconds: int = 90,
        max_file_mb: int | None = 200,
        resolve_timeout_seconds: int = 10,
        resolve_attempts: int = 3,
        max_resolve_redirects: int = 3,
        resolve_retry_delay_seconds: float = 0.25,
        downloader: Literal["yt-dlp", "cobalt"] = "yt-dlp",
        cobalt_api_url: str | None = None,
        cobalt_client_factory: Callable[[], httpx.Client] | None = None,
    ):
        if downloader not in {"yt-dlp", "cobalt"}:
            raise ValueError("TIKTOK_DOWNLOADER must be yt-dlp or cobalt")
        if (
            timeout_seconds <= 0
            or resolve_timeout_seconds <= 0
            or (max_file_mb is not None and max_file_mb <= 0)
        ):
            raise ValueError("Download timeout and size limit must be positive")
        self.temp_dir = temp_dir
        self.downloader = downloader
        self.timeout_seconds = int(timeout_seconds)
        self.max_file_mb = max_file_mb
        self.resolve_timeout_seconds = float(resolve_timeout_seconds)
        self.resolve_attempts = max(1, int(resolve_attempts))
        self.max_resolve_redirects = max(1, int(max_resolve_redirects))
        self.resolve_retry_delay_seconds = max(0.0, float(resolve_retry_delay_seconds))
        os.makedirs(self.temp_dir, exist_ok=True)
        self.cobalt = (
            CobaltTikTokDownloader(
                api_url=cobalt_api_url or "",
                temp_dir=temp_dir,
                timeout_seconds=self.timeout_seconds,
                max_file_mb=max_file_mb,
                resolve_timeout_seconds=self.resolve_timeout_seconds,
                client_factory=cobalt_client_factory,
            )
            if downloader == "cobalt"
            else None
        )

    def validate_tiktok_url(self, raw_url: str) -> str:
        return validate_tiktok_url(raw_url)

    def _request_tiktok_redirect(self, url: str) -> tuple[int, str | None]:
        parsed = urlparse(url)
        connection_type = (
            http.client.HTTPSConnection
            if parsed.scheme.lower() == "https"
            else http.client.HTTPConnection
        )
        connection = connection_type(parsed.hostname, timeout=self.resolve_timeout_seconds)
        response = None

        request_target = parsed.path or "/"
        if parsed.query:
            request_target = f"{request_target}?{parsed.query}"

        try:
            connection.request(
                "GET",
                request_target,
                headers={
                    "Accept": "text/html,*/*",
                    "Accept-Encoding": "identity",
                    "Connection": "close",
                    "User-Agent": _TIKTOK_RESOLVER_USER_AGENT,
                },
            )
            response = connection.getresponse()
            return response.status, response.getheader("Location")
        finally:
            if response is not None:
                response.close()
            connection.close()

    def _resolve_short_tiktok_url_once(self, url: str) -> tuple[str, int]:
        current_url = url
        visited_urls = {current_url}

        for redirect_count in range(1, self.max_resolve_redirects + 1):
            status, location = self._request_tiktok_redirect(current_url)

            if status in _TIKTOK_TRANSIENT_STATUS_CODES:
                raise _TransientTikTokResolveError(
                    f"TikTok short-link resolver returned HTTP {status}"
                )
            if status not in _TIKTOK_REDIRECT_STATUSES:
                raise InvalidTikTokUrlError(_SHORT_LINK_TARGET_ERROR)
            if not location:
                raise InvalidTikTokUrlError(
                    "TikTok short link returned a redirect without a destination"
                )

            candidate_url = urljoin(current_url, location)
            try:
                candidate = parse_tiktok_url(candidate_url)
            except InvalidTikTokUrlError as exc:
                raise InvalidTikTokUrlError(_SHORT_LINK_TARGET_ERROR) from exc

            candidate_parts = urlparse(candidate.url)
            canonical_match = _CANONICAL_VIDEO_PATH.fullmatch(candidate_parts.path)
            if (
                candidate_parts.scheme.lower() == "https"
                and candidate_parts.hostname == "www.tiktok.com"
                and canonical_match is not None
            ):
                return candidate.url, redirect_count

            if candidate.kind != "short":
                raise InvalidTikTokUrlError(_SHORT_LINK_TARGET_ERROR)
            if candidate_parts.scheme.lower() != "https":
                raise InvalidTikTokUrlError(_SHORT_LINK_TARGET_ERROR)
            if candidate.url in visited_urls:
                raise InvalidTikTokUrlError("TikTok short link redirect loop detected")

            visited_urls.add(candidate.url)
            current_url = candidate.url

        raise InvalidTikTokUrlError("TikTok short link redirected too many times")

    def _resolve_short_tiktok_url(self, url: str) -> str:
        started_at = time.perf_counter()

        for attempt in range(1, self.resolve_attempts + 1):
            try:
                resolved_url, redirect_count = self._resolve_short_tiktok_url_once(url)
                logger.info(
                    "timing event=tiktok_short_resolve seconds=%.2f attempts=%d redirects=%d",
                    time.perf_counter() - started_at,
                    attempt,
                    redirect_count,
                )
                return resolved_url
            except _TRANSIENT_RESOLVE_EXCEPTIONS as exc:
                if attempt >= self.resolve_attempts:
                    logger.warning(
                        "tiktok_short_resolve_failed attempts=%d error_type=%s",
                        attempt,
                        type(exc).__name__,
                        exc_info=True,
                    )
                    raise DownloadError(
                        "TikTok short link could not be resolved right now. Please try again."
                    ) from exc

                delay = self.resolve_retry_delay_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "tiktok_short_resolve_retry attempt=%d/%d error_type=%s delay_seconds=%.2f",
                    attempt,
                    self.resolve_attempts,
                    type(exc).__name__,
                    delay,
                )
                time.sleep(delay)

        raise AssertionError("TikTok short-link resolver completed without a result")

    def download_tiktok(self, raw_url: str) -> DownloadResult:
        download_started_at = time.perf_counter()
        parsed_url = parse_tiktok_url(raw_url)
        url = parsed_url.url
        try:
            if parsed_url.kind == "short":
                url = self._resolve_short_tiktok_url(url)

            if self.cobalt is not None:
                result = self.cobalt.download(url, download_started_at)
            else:
                result = self._download_with_yt_dlp(url, download_started_at)
        except DownloadError as exc:
            category = getattr(exc, "category", "download_error")
            observe_tiktok_download(
                provider=self.downloader,
                outcome="failure",
                category=category,
                total_ms=_elapsed_ms(download_started_at),
            )
            logger.warning(
                "tiktok_download_failed provider=%s category=%s total_ms=%d",
                self.downloader,
                category,
                _elapsed_ms(download_started_at),
            )
            raise
        observe_tiktok_download(
            provider=self.downloader,
            outcome="success",
            category="none",
            total_ms=result.download_duration_ms,
            resolve_ms=result.cobalt_resolution_ms,
            transfer_ms=result.media_transfer_ms,
            size_bytes=result.size_bytes,
        )
        logger.info(
            "timing event=tiktok_download provider=%s total_ms=%d cobalt_resolution_ms=%s "
            "media_transfer_ms=%s size_bytes=%s",
            self.downloader,
            result.download_duration_ms,
            result.cobalt_resolution_ms,
            result.media_transfer_ms,
            result.size_bytes,
        )
        return result

    def _download_with_yt_dlp(self, url: str, started_at: float) -> DownloadResult:

        token = uuid.uuid4().hex
        output_template = os.path.join(self.temp_dir, f"tiktok_{token}.%(ext)s")
        cmd = [
            "yt-dlp",
            "--ignore-config",
            "--no-playlist",
            "--no-progress",
            "-o",
            output_template,
            "--",
            url,
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            self._cleanup_token(token)
            raise DownloadError("yt-dlp timed out while downloading TikTok clip") from exc
        except OSError as exc:
            self._cleanup_token(token)
            raise DownloadError("yt-dlp could not start") from exc

        if result.returncode != 0:
            message = (result.stderr or result.stdout or "yt-dlp failed").strip()
            self._cleanup_token(token)
            raise DownloadError(message)

        pattern = os.path.join(self.temp_dir, f"tiktok_{token}.*")
        matches = [path for path in glob.glob(pattern) if os.path.isfile(path)]
        if not matches:
            raise DownloadError("Downloaded file was not created")

        # yt-dlp may create multiple side files in some cases; use the largest media file.
        downloaded_path = max(matches, key=lambda path: os.path.getsize(path))
        size_bytes = os.path.getsize(downloaded_path)
        if size_bytes <= 0:
            self._cleanup_token(token)
            raise DownloadError("Downloaded file is empty")

        if self.max_file_mb is not None:
            max_bytes = int(self.max_file_mb) * 1024 * 1024
            if size_bytes > max_bytes:
                self._cleanup_token(token)
                raise DownloadError(f"Downloaded file exceeds {self.max_file_mb}MB limit")

        return DownloadResult(
            path=downloaded_path,
            provider="yt-dlp",
            size_bytes=size_bytes,
            download_duration_ms=_elapsed_ms(started_at),
        )

    def _cleanup_token(self, token: str) -> None:
        for path in glob.glob(os.path.join(self.temp_dir, f"tiktok_{token}.*")):
            if os.path.isfile(path):
                self.cleanup(path)

    def cleanup(self, path: str) -> None:
        if os.path.exists(path):
            os.remove(path)
