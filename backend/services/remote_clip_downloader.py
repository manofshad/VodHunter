import glob
import json
import logging
import os
import re
import subprocess
import time
import uuid
from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlparse, urlunparse

logger = logging.getLogger("uvicorn.error")


class InvalidTikTokUrlError(Exception):
    pass


class DownloadError(Exception):
    pass


@dataclass(frozen=True)
class DownloadResult:
    path: str


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
_TIKTOK_SHARE_PATH = re.compile(r"/t/[A-Za-z0-9_]+/?")
_TIKTOK_VM_SHARE_PATH = re.compile(r"/[A-Za-z0-9_]+/?")


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
        max_file_mb: int | None = None,
        resolve_timeout_seconds: int = 20,
    ):
        self.temp_dir = temp_dir
        self.timeout_seconds = int(timeout_seconds)
        self.max_file_mb = max_file_mb
        self.resolve_timeout_seconds = int(resolve_timeout_seconds)
        os.makedirs(self.temp_dir, exist_ok=True)

    def validate_tiktok_url(self, raw_url: str) -> str:
        return validate_tiktok_url(raw_url)

    def _validate_short_link_target(self, url: str) -> None:
        cmd = [
            "yt-dlp",
            "--no-playlist",
            "--no-progress",
            "--no-warnings",
            "--skip-download",
            "--dump-single-json",
            url,
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.resolve_timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise DownloadError("yt-dlp timed out while resolving TikTok link") from exc

        if result.returncode != 0:
            message = (result.stderr or result.stdout or "yt-dlp failed to resolve TikTok link").strip()
            raise DownloadError(message)

        try:
            info = json.loads(result.stdout)
        except (TypeError, json.JSONDecodeError) as exc:
            raise DownloadError("yt-dlp returned invalid TikTok link metadata") from exc

        if not isinstance(info, dict):
            raise DownloadError("yt-dlp returned invalid TikTok link metadata")

        extractor = str(info.get("extractor_key") or info.get("extractor") or "").lower()
        video_id = str(info.get("id") or "")
        if (
            info.get("_type") in {"playlist", "multi_video"}
            or "entries" in info
            or not extractor.startswith("tiktok")
            or not video_id.isdigit()
        ):
            raise InvalidTikTokUrlError(
                "TikTok URL must resolve to a single video; profile and playlist links are not supported"
            )

        resolved_url = info.get("webpage_url")
        if resolved_url and resolved_url != url:
            try:
                resolved = parse_tiktok_url(str(resolved_url))
            except InvalidTikTokUrlError as exc:
                raise InvalidTikTokUrlError(
                    "TikTok URL must resolve to a single video; profile and playlist links are not supported"
                ) from exc
            if resolved.kind != "direct":
                raise InvalidTikTokUrlError(
                    "TikTok URL must resolve to a single video; profile and playlist links are not supported"
                )

    def download_tiktok(self, raw_url: str) -> DownloadResult:
        parsed_url = parse_tiktok_url(raw_url)
        url = parsed_url.url
        if parsed_url.kind == "short":
            self._validate_short_link_target(url)

        token = uuid.uuid4().hex
        output_template = os.path.join(self.temp_dir, f"tiktok_{token}.%(ext)s")
        download_started_at = time.perf_counter()

        cmd = [
            "yt-dlp",
            "--no-playlist",
            "--no-progress",
            "-o",
            output_template,
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
            raise DownloadError("yt-dlp timed out while downloading TikTok clip") from exc

        if result.returncode != 0:
            message = (result.stderr or result.stdout or "yt-dlp failed").strip()
            raise DownloadError(message)

        pattern = os.path.join(self.temp_dir, f"tiktok_{token}.*")
        matches = [path for path in glob.glob(pattern) if os.path.isfile(path)]
        if not matches:
            raise DownloadError("Downloaded file was not created")

        # yt-dlp may create multiple side files in some cases; use the largest media file.
        downloaded_path = max(matches, key=lambda path: os.path.getsize(path))
        size_bytes = os.path.getsize(downloaded_path)
        if size_bytes <= 0:
            self.cleanup(downloaded_path)
            raise DownloadError("Downloaded file is empty")

        if self.max_file_mb is not None:
            max_bytes = int(self.max_file_mb) * 1024 * 1024
            if size_bytes > max_bytes:
                self.cleanup(downloaded_path)
                raise DownloadError(f"Downloaded file exceeds {self.max_file_mb}MB limit")

        logger.info(
            "timing event=tiktok_download seconds=%.2f size_bytes=%d",
            time.perf_counter() - download_started_at,
            size_bytes,
        )
        return DownloadResult(path=downloaded_path)

    def cleanup(self, path: str) -> None:
        if os.path.exists(path):
            os.remove(path)
