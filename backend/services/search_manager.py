import logging
import math
import os
import time
from typing import Callable
from urllib.parse import urlparse

from backend.services.media_duration import MediaDurationError, probe_media_duration_seconds
from backend.services.remote_clip_downloader import RemoteClipDownloader
from search.models import SearchDateRange, SearchRequestOutcome
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
        max_duration_seconds: int | None = None,
        duration_probe: Callable[[str], float] = probe_media_duration_seconds,
    ):
        self.search_service = search_service
        self.remote_downloader = remote_downloader
        self.max_duration_seconds = max_duration_seconds
        self.duration_probe = duration_probe

    def search_tiktok_url(
        self,
        url: str,
        streamer: str,
        date_range: SearchDateRange | None = None,
        on_stage_change: Callable[[str], None] | None = None,
        on_stage_timing: Callable[[str, int], None] | None = None,
    ) -> SearchRequestOutcome:
        downloaded_path = ""
        request_started_at = time.perf_counter()
        input_duration_seconds: float | None = None
        stage_durations_ms: dict[str, int] = {}
        parsed_url = urlparse((url or "").strip())
        try:
            if on_stage_change is not None:
                on_stage_change("downloading")
            started_at = time.perf_counter()
            try:
                result = self.remote_downloader.download_tiktok(url)
            finally:
                self._record_stage_timing(
                    "download",
                    started_at,
                    stage_durations_ms,
                    on_stage_timing,
                )
            downloaded_path = result.path
            input_duration_seconds = self._validate_duration(
                downloaded_path,
                on_stage_change=on_stage_change,
                on_stage_timing=on_stage_timing,
                stage_durations_ms=stage_durations_ms,
            )
            execution_result = self._search_local_file(
                downloaded_path,
                streamer,
                date_range=date_range,
                on_stage_change=on_stage_change,
                on_stage_timing=on_stage_timing,
                query_duration_seconds=input_duration_seconds,
            )
            for stage, duration_ms in stage_durations_ms.items():
                if stage == "download":
                    execution_result.metadata.download_duration_ms = duration_ms
                elif stage == "probe":
                    execution_result.metadata.probe_duration_ms = duration_ms
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
                date_range=date_range,
            )
        finally:
            if downloaded_path:
                self.remote_downloader.cleanup(downloaded_path)

    def _validate_duration(
        self,
        path: str,
        on_stage_change: Callable[[str], None] | None = None,
        on_stage_timing: Callable[[str, int], None] | None = None,
        stage_durations_ms: dict[str, int] | None = None,
    ) -> float | None:
        if on_stage_change is not None:
            on_stage_change("probing")
        started_at = time.perf_counter()
        try:
            duration_seconds = self.duration_probe(path)
        except MediaDurationError as exc:
            raise SearchInputError(str(exc)) from exc
        finally:
            self._record_stage_timing(
                "probe",
                started_at,
                stage_durations_ms,
                on_stage_timing,
            )
        logger.info(
            "timing event=duration_probe seconds=%.2f duration_seconds=%.2f path=%s",
            time.perf_counter() - started_at,
            duration_seconds,
            os.path.basename(path),
        )

        if (
            self.max_duration_seconds is not None
            and duration_seconds > self.max_duration_seconds
        ):
            raise InputDurationExceededError(
                duration_seconds=duration_seconds,
                max_duration_seconds=self.max_duration_seconds,
            )
        return duration_seconds

    def _search_local_file(
        self,
        path: str,
        streamer: str,
        date_range: SearchDateRange | None = None,
        on_stage_change: Callable[[str], None] | None = None,
        on_stage_timing: Callable[[str, int], None] | None = None,
        query_duration_seconds: float | None = None,
    ):
        normalized_streamer = streamer.strip().lower()
        if not normalized_streamer:
            raise SearchInputError("streamer is required")
        search_kwargs = {
            "date_range": date_range,
            "on_stage_change": on_stage_change,
            "query_duration_seconds": query_duration_seconds,
        }
        if on_stage_timing is not None:
            search_kwargs["on_stage_timing"] = on_stage_timing
        return self.search_service.search_file(path, normalized_streamer, **search_kwargs)

    @staticmethod
    def _record_stage_timing(
        stage: str,
        started_at: float,
        stage_durations_ms: dict[str, int] | None,
        on_stage_timing: Callable[[str, int], None] | None,
    ) -> None:
        duration_ms = _duration_ms(time.perf_counter() - started_at)
        if stage_durations_ms is not None:
            stage_durations_ms[stage] = duration_ms
        if on_stage_timing is not None:
            on_stage_timing(stage, duration_ms)
