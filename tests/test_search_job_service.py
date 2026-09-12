from concurrent.futures import Future
from datetime import datetime, timezone

import pytest

from backend.services.search_jobs import SearchJobService
from backend.services.remote_clip_downloader import DownloadError, InvalidTikTokUrlError
from backend.services.search_manager import SearchInputError
from search.models import SearchDateRange, SearchExecutionMetadata, SearchRequestOutcome, SearchResult


class InlineExecutor:
    def submit(self, fn, *args, **kwargs):
        future = Future()
        try:
            future.set_result(fn(*args, **kwargs))
        except Exception as exc:  # pragma: no cover - surfaced through future result if needed
            future.set_exception(exc)
        return future


class StubStore:
    def __init__(self):
        self.created_jobs = []
        self.status_updates = []
        self.completed = []
        self.failed = []
        self.failure_total_duration_ms = []
        self.recovered = []
        self.recovery_rows = []

    def create_public_search_job(
        self,
        *,
        tiktok_url: str,
        streamer: str,
        creator_id: int | None,
        date_range: SearchDateRange | None = None,
    ) -> int:
        self.created_jobs.append((tiktok_url, streamer, creator_id, date_range))
        return 7

    def update_search_job_status(self, search_id: int, *, status=None, stage=None, started=False) -> None:
        self.status_updates.append((search_id, status, stage, started))

    def complete_search_job(self, search_id: int, outcome: SearchRequestOutcome) -> None:
        self.completed.append((search_id, outcome))

    def fail_search_job(
        self,
        search_id: int,
        *,
        error_code: str,
        error_message: str,
        http_status: int,
        input_duration_seconds=None,
        total_duration_ms=None,
    ) -> None:
        self.failed.append((search_id, error_code, error_message, http_status, input_duration_seconds))
        self.failure_total_duration_ms.append(total_duration_ms)

    def get_public_search_job(self, search_id: int):
        return None

    def fail_incomplete_public_search_jobs(self, *, error_code: str, error_message: str):
        self.recovered.append((error_code, error_message))
        return list(self.recovery_rows)


class StubSearchManager:
    def __init__(self):
        self.raise_error = None
        self.date_ranges: list[SearchDateRange | None] = []

    def search_tiktok_url(
        self,
        url: str,
        streamer: str,
        date_range: SearchDateRange | None = None,
        on_stage_change=None,
        on_stage_timing=None,
    ) -> SearchRequestOutcome:
        self.date_ranges.append(date_range)
        if on_stage_change is not None:
            on_stage_change("downloading")
            on_stage_change("embedding")
        if on_stage_timing is not None:
            on_stage_timing("download", 1)
            on_stage_timing("fingerprint", 2)
        if self.raise_error is not None:
            raise self.raise_error
        return SearchRequestOutcome(
            result=SearchResult(found=False, streamer=streamer, reason="done"),
            execution_metadata=SearchExecutionMetadata(result_reason="done", found_match=False),
            input_type="tiktok_url",
            download_source="tiktok",
            download_host="www.tiktok.com",
        )


def test_search_job_service_completes_job() -> None:
    store = StubStore()
    service = SearchJobService(jobs=store, search_manager=StubSearchManager(), executor=InlineExecutor())

    search_id = service.create_public_search_job(
        tiktok_url="https://www.tiktok.com/@u/video/1",
        streamer="jason",
        creator_id=2,
    )

    assert search_id == 7
    assert store.created_jobs == [("https://www.tiktok.com/@u/video/1", "jason", 2, None)]
    assert store.status_updates[0] == (7, "running", "validating", True)
    assert (7, None, "downloading", False) in store.status_updates
    assert (7, None, "embedding", False) in store.status_updates
    assert (7, None, "finalizing", False) in store.status_updates
    assert store.completed[0][0] == 7
    assert store.failed == []


def test_search_job_service_emits_one_terminal_event(monkeypatch) -> None:
    events: list[dict[str, object]] = []
    monkeypatch.setattr(
        "backend.services.search_jobs.observe_terminal_search",
        lambda **kwargs: events.append(kwargs),
    )
    store = StubStore()
    service = SearchJobService(jobs=store, search_manager=StubSearchManager(), executor=InlineExecutor())

    service.create_public_search_job(
        tiktok_url="https://www.tiktok.com/@u/video/1",
        streamer="jason",
        creator_id=2,
    )

    assert len(events) == 1
    assert events[0]["search_id"] == 7
    assert events[0]["outcome"] == "no_match"
    assert events[0]["status"] == "completed"
    assert events[0]["stage_durations_ms"] == {
        "download": 1,
        "fingerprint": 2,
        "persist": events[0]["stage_durations_ms"]["persist"],
    }


def test_search_job_service_forwards_date_range() -> None:
    store = StubStore()
    manager = StubSearchManager()
    service = SearchJobService(jobs=store, search_manager=manager, executor=InlineExecutor())
    date_range = SearchDateRange(
        streamed_from=datetime(2026, 4, 1, tzinfo=timezone.utc),
        streamed_to=datetime(2026, 4, 8, tzinfo=timezone.utc),
    )

    service.create_public_search_job(
        tiktok_url="https://www.tiktok.com/@u/video/1",
        streamer="jason",
        creator_id=2,
        date_range=date_range,
    )

    assert store.created_jobs == [("https://www.tiktok.com/@u/video/1", "jason", 2, date_range)]
    assert manager.date_ranges == [date_range]


def test_search_job_service_rejects_invalid_url_before_persisting_job() -> None:
    store = StubStore()
    service = SearchJobService(jobs=store, search_manager=StubSearchManager(), executor=InlineExecutor())

    with pytest.raises(InvalidTikTokUrlError):
        service.create_public_search_job(
            tiktok_url="https://www.tiktok.com/@jasontheween",
            streamer="jason",
            creator_id=2,
        )

    assert store.created_jobs == []


def test_search_job_service_fails_job_for_handled_error() -> None:
    store = StubStore()
    manager = StubSearchManager()
    manager.raise_error = DownloadError("download failed")
    service = SearchJobService(jobs=store, search_manager=manager, executor=InlineExecutor())

    service.create_public_search_job(
        tiktok_url="https://www.tiktok.com/@u/video/1",
        streamer="jason",
        creator_id=2,
    )

    assert store.completed == []
    assert store.failed == [(7, "DOWNLOAD_ERROR", "download failed", 400, None)]


def test_search_job_service_emits_error_event_for_failed_job(monkeypatch) -> None:
    events: list[dict[str, object]] = []
    monkeypatch.setattr(
        "backend.services.search_jobs.observe_terminal_search",
        lambda **kwargs: events.append(kwargs),
    )
    store = StubStore()
    manager = StubSearchManager()
    manager.raise_error = DownloadError("download failed")
    service = SearchJobService(jobs=store, search_manager=manager, executor=InlineExecutor())

    service.create_public_search_job(
        tiktok_url="https://www.tiktok.com/@u/video/1",
        streamer="jason",
        creator_id=2,
    )

    assert len(events) == 1
    assert events[0]["search_id"] == 7
    assert events[0]["outcome"] == "error"
    assert events[0]["error_code"] == "DOWNLOAD_ERROR"
    assert store.failure_total_duration_ms[0] is not None


def test_search_job_service_reports_invalid_search_input() -> None:
    store = StubStore()
    manager = StubSearchManager()
    manager.raise_error = SearchInputError("Could not determine input video duration")
    service = SearchJobService(jobs=store, search_manager=manager, executor=InlineExecutor())

    service.create_public_search_job(
        tiktok_url="https://www.tiktok.com/@u/video/1",
        streamer="jason",
        creator_id=2,
    )

    assert store.completed == []
    assert store.failed == [(7, "INVALID_SEARCH_INPUT", "Could not determine input video duration", 400, None)]


def test_search_job_service_marks_incomplete_jobs_failed_on_restart() -> None:
    store = StubStore()
    service = SearchJobService(jobs=store, search_manager=StubSearchManager(), executor=InlineExecutor())

    service.fail_incomplete_public_search_jobs()

    assert store.recovered == [
        (
            "WORKER_RESTARTED",
            "The server restarted before this search completed. Please run the search again.",
        )
    ]


def test_search_job_service_emits_recovery_event_for_incomplete_job(monkeypatch) -> None:
    events: list[dict[str, object]] = []
    monkeypatch.setattr(
        "backend.services.search_jobs.observe_terminal_search",
        lambda **kwargs: events.append(kwargs),
    )
    store = StubStore()
    store.recovery_rows = [(11, "xqc")]
    service = SearchJobService(jobs=store, search_manager=StubSearchManager(), executor=InlineExecutor())

    service.fail_incomplete_public_search_jobs()

    assert events == [
        {
            "search_id": 11,
            "streamer": "xqc",
            "outcome": "error",
            "status": "failed",
            "total_duration_ms": None,
            "error_code": "WORKER_RESTARTED",
            "http_status": 500,
        }
    ]
