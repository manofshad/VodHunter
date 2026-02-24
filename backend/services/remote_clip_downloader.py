import glob
import os
import subprocess
import uuid
from dataclasses import dataclass
from urllib.parse import urlparse


class InvalidTikTokUrlError(Exception):
    pass


class DownloadError(Exception):
    pass


@dataclass(frozen=True)
class DownloadResult:
    path: str


class RemoteClipDownloader:
    ALLOWED_TIKTOK_HOSTS = {
        "tiktok.com",
        "www.tiktok.com",
        "m.tiktok.com",
        "vm.tiktok.com",
        "vt.tiktok.com",
    }

    def __init__(
        self,
        temp_dir: str,
        timeout_seconds: int = 90,
        max_file_mb: int | None = None,
    ):
        self.temp_dir = temp_dir
        self.timeout_seconds = int(timeout_seconds)
        self.max_file_mb = max_file_mb
        os.makedirs(self.temp_dir, exist_ok=True)

    def validate_tiktok_url(self, raw_url: str) -> str:
        url = (raw_url or "").strip()
        if not url:
            raise InvalidTikTokUrlError("TikTok URL is required")

        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            raise InvalidTikTokUrlError("TikTok URL must use http or https")

        host = (parsed.hostname or "").lower()
        if host not in self.ALLOWED_TIKTOK_HOSTS:
            raise InvalidTikTokUrlError("Only TikTok URLs are supported")

        return url

    def download_tiktok(self, raw_url: str) -> DownloadResult:
        url = self.validate_tiktok_url(raw_url)
        token = uuid.uuid4().hex
        output_template = os.path.join(self.temp_dir, f"tiktok_{token}.%(ext)s")

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

        return DownloadResult(path=downloaded_path)

    def cleanup(self, path: str) -> None:
        if os.path.exists(path):
            os.remove(path)
