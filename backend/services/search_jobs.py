from __future__ import annotations

from concurrent.futures import Executor
import logging
import time

from backend.observability import observe_terminal_search
from backend.services.remote_clip_downloader import DownloadError, InvalidTikTokUrlError, validate_tiktok_url
from backend.services.search_manager import InputDurationExceededError, SearchInputError
from search.models import SearchDateRange, SearchJobRecord
from storage.search_job_repository import SearchJobRepository


logger = logging.getLogger("uvicorn.error")


def _duration_ms(seconds: float) -> int:
    return max(int(round(seconds * 1000.0)), 0)


class SearchJobService:
    def __init__(self, jobs: SearchJobRepository, search_manager, executor: Executor):
        self.jobs = jobs
        self.search_manager = search_manager
        self.executor = executor

    def create_public_search_job(
        self,
        *,
        tiktok_url: str,
        streamer: str,
        creator_id: int | None,
        date_range: SearchDateRange | None = None,
    ) -> int:
        accepted_started_at = time.perf_counter()
        normalized_tiktok_url = validate_tiktok_url(tiktok_url)
        search_id = self.jobs.create_public_search_job(
            tiktok_url=normalized_tiktok_url,
            streamer=streamer,
            creator_id=creator_id,
            date_range=date_range,
        )
        self.executor.submit(
            self._run_public_search_job,
            search_id,
            normalized_tiktok_url,
            streamer,
            date_range,
            accepted_started_at,
        )
        return search_id

    def get_public_search_job(self, search_id: int) -> SearchJobRecord | None:
        return self.jobs.get_public_search_job(search_id)

    def fail_incomplete_public_search_jobs(self) -> None:
        recovered = self.jobs.fail_incomplete_public_search_jobs(
            error_code="WORKER_RESTARTED",
            error_message="The server restarted before this search completed. Please run the search again.",
        )
        for item in recovered or []:
            search_id, streamer = item
            self._observe_terminal(
                search_id=int(search_id),
                streamer=streamer,
                outcome="error",
                status="failed",
                total_duration_ms=None,
                error_code="WORKER_RESTARTED",
                http_status=500,
            )

    def _run_public_search_job(
        self,
        search_id: int,
        tiktok_url: str,
        streamer: str,
        date_range: SearchDateRange | None,
        accepted_started_at: float | None = None,
    ) -> None:
        run_started_at = (
            accepted_started_at
            if accepted_started_at is not None
            else time.perf_counter()
        )
        stage_durations_ms: dict[str, int] = {}

        def record_stage(stage: str, duration_ms: int) -> None:
            stage_durations_ms[str(stage)] = max(int(duration_ms), 0)

        try:
            self.jobs.update_search_job_status(
                search_id,
                status="running",
                stage="validating",
                started=True,
            )
            outcome = self.search_manager.search_tiktok_url(
                tiktok_url,
                streamer,
                date_range=date_range,
                on_stage_change=lambda stage: self.jobs.update_search_job_status(search_id, stage=stage),
                on_stage_timing=record_stage,
            )
            self.jobs.update_search_job_status(search_id, stage="finalizing")
            persist_started_at = time.perf_counter()
            self.jobs.complete_search_job(search_id, outcome)
            record_stage("persist", _duration_ms(time.perf_counter() - persist_started_at))
            self._observe_terminal(
                search_id=search_id,
                streamer=streamer,
                outcome="match" if outcome.result.found else "no_match",
                status="completed",
                total_duration_ms=_duration_ms(time.perf_counter() - run_started_at),
                stage_durations_ms=stage_durations_ms,
                result=outcome.result,
                diagnostics=self._diagnostics(outcome),
            )
        except InputDurationExceededError as exc:
            self._fail_and_observe(
                search_id,
                streamer,
                run_started_at,
                stage_durations_ms,
                error_code="INPUT_DURATION_EXCEEDED",
                error_message=str(exc),
                http_status=400,
                input_duration_seconds=exc.duration_seconds,
            )
        except SearchInputError as exc:
            self._fail_and_observe(
                search_id,
                streamer,
                run_started_at,
                stage_durations_ms,
                error_code="INVALID_SEARCH_INPUT",
                error_message=str(exc),
                http_status=400,
            )
        except InvalidTikTokUrlError as exc:
            self._fail_and_observe(
                search_id,
                streamer,
                run_started_at,
                stage_durations_ms,
                error_code="INVALID_TIKTOK_URL",
                error_message=str(exc),
                http_status=400,
            )
        except DownloadError as exc:
            self._fail_and_observe(
                search_id,
                streamer,
                run_started_at,
                stage_durations_ms,
                error_code="DOWNLOAD_ERROR",
                error_message=str(exc),
                http_status=400,
            )
        except RuntimeError as exc:
            self._fail_and_observe(
                search_id,
                streamer,
                run_started_at,
                stage_durations_ms,
                error_code="PROCESSING_ERROR",
                error_message=str(exc),
                http_status=400,
            )
        except Exception:
            logger.exception("Unexpected public search job failure search_id=%s", search_id)
            self._fail_and_observe(
                search_id,
                streamer,
                run_started_at,
                stage_durations_ms,
                error_code="PROCESSING_ERROR",
                error_message="Unexpected error while processing search",
                http_status=500,
            )

    def _fail_and_observe(
        self,
        search_id: int,
        streamer: str,
        run_started_at: float,
        stage_durations_ms: dict[str, int],
        *,
        error_code: str,
        error_message: str,
        http_status: int,
        input_duration_seconds: float | None = None,
    ) -> None:
        persist_started_at = time.perf_counter()
        self.jobs.fail_search_job(
            search_id,
            error_code=error_code,
            error_message=error_message,
            http_status=http_status,
            input_duration_seconds=input_duration_seconds,
            total_duration_ms=_duration_ms(time.perf_counter() - run_started_at),
        )
        stage_durations_ms["persist"] = _duration_ms(
            time.perf_counter() - persist_started_at
        )
        self._observe_terminal(
            search_id=search_id,
            streamer=streamer,
            outcome="error",
            status="failed",
            total_duration_ms=_duration_ms(time.perf_counter() - run_started_at),
            stage_durations_ms=stage_durations_ms,
            error_code=error_code,
            http_status=http_status,
            diagnostics={
                "input_duration_seconds": input_duration_seconds,
            },
        )

    @staticmethod
    def _diagnostics(outcome) -> dict[str, object | None]:
        metadata = outcome.execution_metadata
        date_range = outcome.date_range
        return {
            "input_type": outcome.input_type,
            "download_source": outcome.download_source,
            "download_host": outcome.download_host,
            "input_duration_seconds": outcome.input_duration_seconds,
            "streamed_from": (
                date_range.streamed_from.isoformat()
                if date_range is not None and date_range.streamed_from is not None
                else None
            ),
            "streamed_to": (
                date_range.streamed_to.isoformat()
                if date_range is not None and date_range.streamed_to is not None
                else None
            ),
            "query_fingerprint_count": metadata.query_fingerprint_count,
            "candidate_count": metadata.candidate_count,
            "segment_count": metadata.segment_count,
            "model_cold_start": metadata.model_cold_start,
            "model_version": metadata.model_version,
            "preprocessing_version": metadata.preprocessing_version,
            "download_duration_ms": metadata.download_duration_ms,
            "probe_duration_ms": metadata.probe_duration_ms,
            "model_startup_duration_ms": metadata.model_startup_duration_ms,
            "fingerprint_preprocessing_duration_ms": metadata.fingerprint_preprocessing_duration_ms,
            "fingerprint_inference_duration_ms": metadata.fingerprint_inference_duration_ms,
            "fingerprint_duration_ms": metadata.fingerprint_duration_ms,
        }

    @staticmethod
    def _observe_terminal(**kwargs) -> None:
        try:
            observe_terminal_search(**kwargs)
        except Exception:
            # Telemetry must never turn a successfully persisted search into a
            # second application failure. The ordinary Uvicorn logger retains
            # the failure for local diagnosis.
            logger.exception("Unable to emit search observability event")
