import logging
import os
import math
import time
from typing import Callable

from backend.services.media_duration import MediaDurationError, probe_media_duration_seconds
from backend.services.remote_clip_downloader import RemoteClipDownloader
from search.search_service import SearchService

logger = logging.getLogger("uvicorn.error")


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
        remote_downloader: RemoteClipDownloader,
        max_duration_seconds: int | None = None,
        duration_probe: Callable[[str], float] = probe_media_duration_seconds,
    ):
        self.search_service = search_service
        self.remote_downloader = remote_downloader
        self.max_duration_seconds = max_duration_seconds
        self.duration_probe = duration_probe

    def search_tiktok_url(self, url: str, streamer: str):
        downloaded_path = ""
        request_started_at = time.perf_counter()
        try:
            result = self.remote_downloader.download_tiktok(url)
            downloaded_path = result.path
            self._validate_duration(downloaded_path)
            search_result = self._search_local_file(downloaded_path, streamer)
            logger.info(
                "timing event=search_tiktok_url seconds=%.2f streamer=%s",
                time.perf_counter() - request_started_at,
                streamer.strip().lower(),
            )
            return search_result
        finally:
            if downloaded_path:
                self.remote_downloader.cleanup(downloaded_path)

    def _validate_duration(self, path: str) -> None:
        if self.max_duration_seconds is None:
            return
        started_at = time.perf_counter()
        try:
            duration_seconds = self.duration_probe(path)
        except MediaDurationError as exc:
            raise SearchInputError(str(exc)) from exc
        logger.info(
            "timing event=duration_probe seconds=%.2f duration_seconds=%.2f path=%s",
            time.perf_counter() - started_at,
            duration_seconds,
            os.path.basename(path),
        )

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
