import glob
import http.client
import logging
import os
import re
import subprocess
import time
import uuid
from dataclasses import dataclass
from typing import Literal
from urllib.parse import urljoin, urlparse, urlunparse

logger = logging.getLogger("uvicorn.error")


class InvalidClipUrlError(Exception):
    pass


class DownloadError(Exception):
    pass


class _TransientTikTokResolveError(Exception):
    pass


@dataclass(frozen=True)
class DownloadResult:
    path: str


@dataclass(frozen=True)
class ClipUrl:
    url: str
    platform: Literal["tiktok", "instagram", "youtube"]
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
_TIKTOK_LOGIN_REQUIRED_ERROR = re.compile(
    r"^ERROR: \[TikTok\] [0-9]+: [^\r\n]*\bLog in for access\.",
    re.MULTILINE,
)
_YOUTUBE_BOT_ERROR = re.compile(r"sign in to confirm you['’]re not a bot", re.IGNORECASE)
_YOUTUBE_DOWNLOAD_ERROR = "VodHunter could not download this YouTube Short. Please try again later."
_TRANSIENT_RESOLVE_EXCEPTIONS = (
    _TransientTikTokResolveError,
    http.client.HTTPException,
    OSError,
    TimeoutError,
)
_DIRECT_CLIP_RULES = (
    ("instagram", {"instagram.com", "www.instagram.com"}, re.compile(r"/reels?/[A-Za-z0-9_-]+/?")),
    ("youtube", {"youtube.com", "www.youtube.com", "m.youtube.com"}, re.compile(r"/shorts/[A-Za-z0-9_-]{11}/?")),
)


def _invalid_path_error() -> InvalidClipUrlError:
    return InvalidClipUrlError(
        "TikTok URL must point to a single video; profile, live, music, photo, "
        "effect, hashtag, and collection links are not supported"
    )


def parse_clip_url(raw_url: str) -> ClipUrl:
    url = (raw_url or "").strip()
    try:
        parsed = urlparse(url)
        port = parsed.port
    except ValueError as exc:
        raise InvalidClipUrlError("Clip URL is malformed") from exc

    if not url:
        raise InvalidClipUrlError("Clip URL is required")
    if parsed.scheme.lower() not in {"http", "https"}:
        raise InvalidClipUrlError("Clip URL must use http or https")
    if parsed.username or parsed.password or "@" in parsed.netloc or port is not None or parsed.params:
        raise InvalidClipUrlError("Clip URL is malformed")

    host = (parsed.hostname or "").lower()
    if host in _DIRECT_TIKTOK_HOSTS or host in _SHORT_TIKTOK_HOSTS:
        return _parse_tiktok_url(url)
    for platform, hosts, path in _DIRECT_CLIP_RULES:
        if host in hosts:
            if not path.fullmatch(parsed.path):
                raise InvalidClipUrlError("URL must point to a single Instagram Reel or YouTube Short")
            normalized_host = "www.instagram.com" if platform == "instagram" else "www.youtube.com"
            normalized_url = urlunparse(("https", normalized_host, parsed.path, "", "", ""))
            return ClipUrl(url=normalized_url, platform=platform, kind="direct")
    raise InvalidClipUrlError("Only TikTok videos, Instagram Reels, and YouTube Shorts are supported")


def _parse_tiktok_url(raw_url: str) -> ClipUrl:
    url = (raw_url or "").strip()
    if not url:
        raise InvalidClipUrlError("TikTok URL is required")

    try:
        parsed = urlparse(url)
        port = parsed.port
    except ValueError as exc:
        raise InvalidClipUrlError("TikTok URL is malformed") from exc

    if parsed.scheme.lower() not in {"http", "https"}:
        raise InvalidClipUrlError("TikTok URL must use http or https")

    host = (parsed.hostname or "").lower()
    if host not in _DIRECT_TIKTOK_HOSTS and host not in _SHORT_TIKTOK_HOSTS:
        raise InvalidClipUrlError("Only TikTok URLs are supported")
    if parsed.username or parsed.password or "@" in parsed.netloc or port is not None or parsed.params:
        raise InvalidClipUrlError("TikTok URL is malformed")

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
    return ClipUrl(url=normalized_url, platform="tiktok", kind="direct" if is_direct_video else "short")


def validate_clip_url(raw_url: str) -> str:
    return parse_clip_url(raw_url).url


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
        youtube_proxy_url: str | None = None,
    ):
        self.temp_dir = temp_dir
        self.timeout_seconds = int(timeout_seconds)
        self.max_file_mb = max_file_mb
        self.youtube_proxy_url = youtube_proxy_url
        self.resolve_timeout_seconds = float(resolve_timeout_seconds)
        self.resolve_attempts = max(1, int(resolve_attempts))
        self.max_resolve_redirects = max(1, int(max_resolve_redirects))
        self.resolve_retry_delay_seconds = max(0.0, float(resolve_retry_delay_seconds))
        os.makedirs(self.temp_dir, exist_ok=True)

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
                raise InvalidClipUrlError(_SHORT_LINK_TARGET_ERROR)
            if not location:
                raise InvalidClipUrlError(
                    "TikTok short link returned a redirect without a destination"
                )

            candidate_url = urljoin(current_url, location)
            try:
                candidate = _parse_tiktok_url(candidate_url)
            except InvalidClipUrlError as exc:
                raise InvalidClipUrlError(_SHORT_LINK_TARGET_ERROR) from exc

            candidate_parts = urlparse(candidate.url)
            canonical_match = _CANONICAL_VIDEO_PATH.fullmatch(candidate_parts.path)
            if (
                candidate_parts.scheme.lower() == "https"
                and candidate_parts.hostname == "www.tiktok.com"
                and canonical_match is not None
            ):
                return candidate.url, redirect_count

            if candidate.kind != "short":
                raise InvalidClipUrlError(_SHORT_LINK_TARGET_ERROR)
            if candidate_parts.scheme.lower() != "https":
                raise InvalidClipUrlError(_SHORT_LINK_TARGET_ERROR)
            if candidate.url in visited_urls:
                raise InvalidClipUrlError("TikTok short link redirect loop detected")

            visited_urls.add(candidate.url)
            current_url = candidate.url

        raise InvalidClipUrlError("TikTok short link redirected too many times")

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

    def download_clip(self, raw_url: str) -> DownloadResult:
        parsed_url = parse_clip_url(raw_url)
        url = parsed_url.url
        if parsed_url.kind == "short":
            url = self._resolve_short_tiktok_url(url)

        token = uuid.uuid4().hex
        output_prefix = os.path.join(self.temp_dir, f"{parsed_url.platform}_{token}")
        output_template = f"{output_prefix}.%(ext)s"
        download_started_at = time.perf_counter()

        cmd = [
            "yt-dlp",
            "--ignore-config",
            "--no-playlist",
            "--no-progress",
            "-o",
            output_template,
        ]
        if parsed_url.platform == "youtube":
            cmd.extend(["--js-runtimes", "node", "--format", "bestaudio/best"])
            if self.youtube_proxy_url:
                cmd.extend(["--proxy", self.youtube_proxy_url])
        cmd.extend(["--", url])

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            self._cleanup_download(output_prefix)
            if parsed_url.platform == "youtube":
                logger.warning("youtube_download_failed reason=timeout proxy_configured=%s", bool(self.youtube_proxy_url))
                raise DownloadError(_YOUTUBE_DOWNLOAD_ERROR) from None
            raise DownloadError(f"yt-dlp timed out while downloading {parsed_url.platform} clip") from exc
        except OSError:
            if parsed_url.platform != "youtube":
                raise
            self._cleanup_download(output_prefix)
            logger.warning("youtube_download_failed reason=process_error proxy_configured=%s", bool(self.youtube_proxy_url))
            raise DownloadError(_YOUTUBE_DOWNLOAD_ERROR) from None

        if result.returncode != 0:
            self._cleanup_download(output_prefix)
            if parsed_url.platform == "youtube":
                bot_challenge = bool(_YOUTUBE_BOT_ERROR.search(result.stderr or "") or _YOUTUBE_BOT_ERROR.search(result.stdout or ""))
                logger.warning(
                    "youtube_download_failed reason=%s returncode=%s proxy_configured=%s",
                    "bot_challenge" if bot_challenge else "provider_error",
                    result.returncode,
                    bool(self.youtube_proxy_url),
                )
                if bot_challenge:
                    raise DownloadError(
                        "YouTube blocked VodHunter from downloading this Short. Please try again later."
                    ) from None
                raise DownloadError(_YOUTUBE_DOWNLOAD_ERROR) from None
            message = (result.stderr or result.stdout or "yt-dlp failed").strip()
            if _TIKTOK_LOGIN_REQUIRED_ERROR.search(result.stderr or ""):
                logger.warning("tiktok_download_login_required error=%s", message)
                raise DownloadError(
                    "This TikTok requires login, so VodHunter cannot download it. "
                    "Try a publicly accessible TikTok video."
                )
            raise DownloadError(message)

        matches = [
            path for path in glob.glob(f"{output_prefix}.*")
            if os.path.isfile(path) and not path.endswith((".part", ".ytdl", ".json"))
        ]
        if not matches:
            raise DownloadError("Downloaded file was not created")

        # yt-dlp may create multiple side files in some cases; use the largest media file.
        downloaded_path = max(matches, key=lambda path: os.path.getsize(path))
        size_bytes = os.path.getsize(downloaded_path)
        if size_bytes <= 0:
            self._cleanup_download(output_prefix)
            raise DownloadError("Downloaded file is empty")

        if self.max_file_mb is not None:
            max_bytes = int(self.max_file_mb) * 1024 * 1024
            if size_bytes > max_bytes:
                self._cleanup_download(output_prefix)
                raise DownloadError(f"Downloaded file exceeds {self.max_file_mb}MB limit")

        logger.info(
            "timing event=clip_download platform=%s seconds=%.2f size_bytes=%d",
            parsed_url.platform,
            time.perf_counter() - download_started_at,
            size_bytes,
        )
        return DownloadResult(path=downloaded_path)

    def _cleanup_download(self, output_prefix: str) -> None:
        for path in glob.glob(f"{output_prefix}.*"):
            self.cleanup(path)

    def cleanup(self, path: str) -> None:
        if os.path.exists(path):
            os.remove(path)
