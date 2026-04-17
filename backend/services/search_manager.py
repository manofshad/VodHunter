import logging
import math
import os
import shutil
import time
from typing import Callable
from urllib.parse import urlparse

from fastapi import UploadFile

from backend.services.media_duration import MediaDurationError, probe_media_duration_seconds
from backend.services.remote_clip_downloader import RemoteClipDownloader
from search.models import SearchRequestOutcome
from search.search_service import SearchService

logger = logging.getLogger("uvicorn.error")


def _duration_ms(seconds: float) -> int:
    return max(int(round(seconds * 1000.0)), 0)


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
        upload_temp_dir: str | None = None,
        max_duration_seconds: int | None = None,
        duration_probe: Callable[[str], float] = probe_media_duration_seconds,
    ):
        self.search_service = search_service
        self.upload_temp_dir = upload_temp_dir
        self.remote_downloader = remote_downloader
        self.max_duration_seconds = max_duration_seconds
        self.duration_probe = duration_probe
        if self.upload_temp_dir is not None:
            os.makedirs(self.upload_temp_dir, exist_ok=True)

    def search_upload(
        self,
        file: UploadFile,
        streamer: str,
        on_stage_change: Callable[[str], None] | None = None,
    ) -> SearchRequestOutcome:
        if self.upload_temp_dir is None:
            raise SearchInputError("File uploads are not enabled")
        if not file.filename:
            raise SearchInputError("Uploaded file must have a filename")

        suffix = os.path.splitext(file.filename)[1] or ".bin"
        temp_path = os.path.join(self.upload_temp_dir, f"upload_{time.time_ns()}{suffix}")
        request_started_at = time.perf_counter()
        input_duration_seconds: float | None = None

        try:
            with open(temp_path, "wb") as out:
                shutil.copyfileobj(file.file, out)

            if os.path.getsize(temp_path) == 0:
                raise SearchInputError("Uploaded file is empty")

            input_duration_seconds = self._validate_duration(temp_path, on_stage_change=on_stage_change)
            execution_result = self._search_local_file(temp_path, streamer, on_stage_change=on_stage_change)
            logger.info(
                "timing event=search_upload seconds=%.2f streamer=%s",
                time.perf_counter() - request_started_at,
                streamer.strip().lower(),
            )
            return SearchRequestOutcome(
                result=execution_result.result,
                execution_metadata=execution_result.metadata,
                input_type="file",
                clip_filename=file.filename,
                input_duration_seconds=input_duration_seconds,
                total_duration_ms=_duration_ms(time.perf_counter() - request_started_at),
            )
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def search_tiktok_url(
        self,
        url: str,
        streamer: str,
        on_stage_change: Callable[[str], None] | None = None,
    ) -> SearchRequestOutcome:
        downloaded_path = ""
        request_started_at = time.perf_counter()
        input_duration_seconds: float | None = None
        parsed_url = urlparse((url or "").strip())
        try:
            if on_stage_change is not None:
                on_stage_change("downloading")
            result = self.remote_downloader.download_tiktok(url)
            downloaded_path = result.path
            input_duration_seconds = self._validate_duration(downloaded_path, on_stage_change=on_stage_change)
            execution_result = self._search_local_file(downloaded_path, streamer, on_stage_change=on_stage_change)
            logger.info(
                "timing event=search_tiktok_url seconds=%.2f streamer=%s",
                time.perf_counter() - request_started_at,
                streamer.strip().lower(),
            )
            return SearchRequestOutcome(
                result=execution_result.result,
                execution_metadata=execution_result.metadata,
                input_type="tiktok_url",
                download_source="tiktok",
                download_host=(parsed_url.hostname or "").lower() or None,
                input_duration_seconds=input_duration_seconds,
                total_duration_ms=_duration_ms(time.perf_counter() - request_started_at),
            )
        finally:
            if downloaded_path:
                self.remote_downloader.cleanup(downloaded_path)

    def _validate_duration(self, path: str, on_stage_change: Callable[[str], None] | None = None) -> float | None:
        if self.max_duration_seconds is None:
            return None
        if on_stage_change is not None:
            on_stage_change("probing")
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
        return duration_seconds

    def _search_local_file(
        self,
        path: str,
        streamer: str,
        on_stage_change: Callable[[str], None] | None = None,
    ):
        normalized_streamer = streamer.strip().lower()
        if not normalized_streamer:
            raise SearchInputError("streamer is required")
        return self.search_service.search_file(path, normalized_streamer, on_stage_change=on_stage_change)
