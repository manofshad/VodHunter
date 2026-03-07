import os
import shutil
import uuid
import math
from typing import Callable

from fastapi import UploadFile

from backend.services.media_duration import MediaDurationError, probe_media_duration_seconds
from backend.services.remote_clip_downloader import RemoteClipDownloader
from search.search_service import SearchService


class SearchInputError(Exception):
    pass


class InputDurationExceededError(SearchInputError):
    def __init__(self, duration_seconds: float, max_duration_seconds: int):
        self.duration_seconds = duration_seconds
        self.max_duration_seconds = max_duration_seconds
        rounded_duration = int(math.ceil(duration_seconds))
        super().__init__(f"Input video is {rounded_duration}s; maximum allowed is {max_duration_seconds}s")


class SearchManager:
    def __init__(
        self,
        search_service: SearchService,
        upload_temp_dir: str,
        remote_downloader: RemoteClipDownloader,
        max_duration_seconds: int | None = None,
        duration_probe: Callable[[str], float] = probe_media_duration_seconds,
    ):
        self.search_service = search_service
        self.upload_temp_dir = upload_temp_dir
        self.remote_downloader = remote_downloader
        self.max_duration_seconds = max_duration_seconds
        self.duration_probe = duration_probe
        os.makedirs(self.upload_temp_dir, exist_ok=True)

    def search_upload(self, file: UploadFile, streamer: str):
        if not file.filename:
            raise SearchInputError("Uploaded file must have a filename")

        suffix = os.path.splitext(file.filename)[1] or ".bin"
        temp_path = os.path.join(self.upload_temp_dir, f"upload_{uuid.uuid4().hex}{suffix}")

        try:
            with open(temp_path, "wb") as out:
                shutil.copyfileobj(file.file, out)

            if os.path.getsize(temp_path) == 0:
                raise SearchInputError("Uploaded file is empty")

            self._validate_duration(temp_path)
            return self._search_local_file(temp_path, streamer)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def search_tiktok_url(self, url: str, streamer: str):
        downloaded_path = ""
        try:
            result = self.remote_downloader.download_tiktok(url)
            downloaded_path = result.path
            self._validate_duration(downloaded_path)
            return self._search_local_file(downloaded_path, streamer)
        finally:
            if downloaded_path:
                self.remote_downloader.cleanup(downloaded_path)

    def _validate_duration(self, path: str) -> None:
        if self.max_duration_seconds is None:
            return
        try:
            duration_seconds = self.duration_probe(path)
        except MediaDurationError as exc:
            raise SearchInputError(str(exc)) from exc

        if duration_seconds > self.max_duration_seconds:
            raise InputDurationExceededError(
                duration_seconds=duration_seconds,
                max_duration_seconds=self.max_duration_seconds,
            )

    def _search_local_file(self, path: str, streamer: str):
        normalized_streamer = streamer.strip().lower()
        if not normalized_streamer:
            raise SearchInputError("streamer is required")
        return self.search_service.search_file(path, normalized_streamer)
