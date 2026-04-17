from __future__ import annotations

from concurrent.futures import Executor
import logging

from backend.services.remote_clip_downloader import DownloadError, InvalidTikTokUrlError
from backend.services.search_manager import InputDurationExceededError, SearchInputError
from search.models import SearchJobRecord


logger = logging.getLogger("uvicorn.error")


class SearchJobService:
    def __init__(self, store, search_manager, executor: Executor):
        self.store = store
        self.search_manager = search_manager
        self.executor = executor

    def create_public_search_job(self, *, tiktok_url: str, streamer: str, creator_id: int | None) -> int:
        search_id = self.store.create_public_search_job(
            tiktok_url=tiktok_url,
            streamer=streamer,
            creator_id=creator_id,
        )
        self.executor.submit(self._run_public_search_job, search_id, tiktok_url, streamer)
        return search_id

    def get_public_search_job(self, search_id: int) -> SearchJobRecord | None:
        return self.store.get_public_search_job(search_id)

    def fail_incomplete_public_search_jobs(self) -> None:
        fail_incomplete_jobs = getattr(self.store, "fail_incomplete_public_search_jobs", None)
        if not callable(fail_incomplete_jobs):
            return
        fail_incomplete_jobs(
            error_code="WORKER_RESTARTED",
            error_message="The server restarted before this search completed. Please run the search again.",
        )

    def _run_public_search_job(self, search_id: int, tiktok_url: str, streamer: str) -> None:
        try:
            self.store.update_search_job_status(
                search_id,
                status="running",
                stage="validating",
                started=True,
            )
            outcome = self.search_manager.search_tiktok_url(
                tiktok_url,
                streamer,
                on_stage_change=lambda stage: self.store.update_search_job_status(search_id, stage=stage),
            )
            self.store.update_search_job_status(search_id, stage="finalizing")
            self.store.complete_search_job(search_id, outcome)
        except InputDurationExceededError as exc:
            self.store.fail_search_job(
                search_id,
                error_code="INPUT_DURATION_EXCEEDED",
                error_message=str(exc),
                http_status=400,
                input_duration_seconds=exc.duration_seconds,
            )
        except SearchInputError as exc:
            self.store.fail_search_job(
                search_id,
                error_code="INVALID_UPLOAD",
                error_message=str(exc),
                http_status=400,
            )
        except InvalidTikTokUrlError as exc:
            self.store.fail_search_job(
                search_id,
                error_code="INVALID_TIKTOK_URL",
                error_message=str(exc),
                http_status=400,
            )
        except DownloadError as exc:
            self.store.fail_search_job(
                search_id,
                error_code="DOWNLOAD_ERROR",
                error_message=str(exc),
                http_status=400,
            )
        except RuntimeError as exc:
            self.store.fail_search_job(
                search_id,
                error_code="PROCESSING_ERROR",
                error_message=str(exc),
                http_status=400,
            )
        except Exception:
            logger.exception("Unexpected public search job failure search_id=%s", search_id)
            self.store.fail_search_job(
                search_id,
                error_code="PROCESSING_ERROR",
                error_message="Unexpected error while processing search",
                http_status=500,
            )
