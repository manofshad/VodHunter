from concurrent.futures import Future

from backend.services.search_jobs import SearchJobService
from backend.services.remote_clip_downloader import DownloadError
from search.models import SearchExecutionMetadata, SearchRequestOutcome, SearchResult


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
        self.recovered = []

    def create_public_search_job(self, *, tiktok_url: str, streamer: str, creator_id: int | None) -> int:
        self.created_jobs.append((tiktok_url, streamer, creator_id))
        return 7

    def update_search_job_status(self, search_id: int, *, status=None, stage=None, started=False) -> None:
        self.status_updates.append((search_id, status, stage, started))

    def complete_search_job(self, search_id: int, outcome: SearchRequestOutcome) -> None:
        self.completed.append((search_id, outcome))

    def fail_search_job(self, search_id: int, *, error_code: str, error_message: str, http_status: int, input_duration_seconds=None) -> None:
        self.failed.append((search_id, error_code, error_message, http_status, input_duration_seconds))

    def get_public_search_job(self, search_id: int):
        return None

    def fail_incomplete_public_search_jobs(self, *, error_code: str, error_message: str) -> None:
        self.recovered.append((error_code, error_message))


class StubSearchManager:
    def __init__(self):
        self.raise_error = None

    def search_tiktok_url(self, url: str, streamer: str, on_stage_change=None) -> SearchRequestOutcome:
        if on_stage_change is not None:
            on_stage_change("downloading")
            on_stage_change("embedding")
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
    service = SearchJobService(store=store, search_manager=StubSearchManager(), executor=InlineExecutor())

    search_id = service.create_public_search_job(
        tiktok_url="https://www.tiktok.com/@u/video/1",
        streamer="jason",
        creator_id=2,
    )

    assert search_id == 7
    assert store.created_jobs == [("https://www.tiktok.com/@u/video/1", "jason", 2)]
    assert store.status_updates[0] == (7, "running", "validating", True)
    assert (7, None, "downloading", False) in store.status_updates
    assert (7, None, "embedding", False) in store.status_updates
    assert (7, None, "finalizing", False) in store.status_updates
    assert store.completed[0][0] == 7
    assert store.failed == []


def test_search_job_service_fails_job_for_handled_error() -> None:
    store = StubStore()
    manager = StubSearchManager()
    manager.raise_error = DownloadError("download failed")
    service = SearchJobService(store=store, search_manager=manager, executor=InlineExecutor())

    service.create_public_search_job(
        tiktok_url="https://www.tiktok.com/@u/video/1",
        streamer="jason",
        creator_id=2,
    )

    assert store.completed == []
    assert store.failed == [(7, "DOWNLOAD_ERROR", "download failed", 400, None)]


def test_search_job_service_marks_incomplete_jobs_failed_on_restart() -> None:
    store = StubStore()
    service = SearchJobService(store=store, search_manager=StubSearchManager(), executor=InlineExecutor())

    service.fail_incomplete_public_search_jobs()

    assert store.recovered == [
        (
            "WORKER_RESTARTED",
            "The server restarted before this search completed. Please run the search again.",
        )
    ]
