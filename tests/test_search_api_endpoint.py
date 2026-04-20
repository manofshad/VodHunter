import io

from fastapi.testclient import TestClient

from backend.apps.admin import create_admin_app
from backend.apps.public import create_public_app
from search.models import SearchExecutionMetadata, SearchJobRecord, SearchRequestOutcome, SearchResult
from storage.vector_store import VectorStore


class StubSearchManager:
    def __init__(self):
        self.upload_calls = 0
        self.url_calls = 0
        self.last_streamer: str | None = None
        self.last_upload_filename: str | None = None
        self.raise_upload: Exception | None = None
        self.raise_url: Exception | None = None

    def search_upload(self, file, streamer: str, on_stage_change=None) -> SearchRequestOutcome:
        self.upload_calls += 1
        self.last_streamer = streamer
        self.last_upload_filename = file.filename
        if self.raise_upload is not None:
            raise self.raise_upload
        return SearchRequestOutcome(
            result=SearchResult(
                found=False,
                streamer=streamer,
                profile_image_url="https://cdn/profile.png",
                reason="upload test",
                thumbnail_url=None,
                video_url_at_timestamp=None,
            ),
            execution_metadata=SearchExecutionMetadata(result_reason="upload test", found_match=False),
            input_type="file",
            clip_filename=file.filename,
        )

    def search_tiktok_url(self, url: str, streamer: str, on_stage_change=None) -> SearchRequestOutcome:
        self.url_calls += 1
        self.last_streamer = streamer
        if self.raise_url is not None:
            raise self.raise_url
        return SearchRequestOutcome(
            result=SearchResult(
                found=False,
                streamer=streamer,
                profile_image_url="https://cdn/profile.png",
                reason="url test",
                thumbnail_url=None,
                video_url_at_timestamp=None,
            ),
            execution_metadata=SearchExecutionMetadata(result_reason="url test", found_match=False),
            input_type="tiktok_url",
            download_source="tiktok",
            download_host="www.tiktok.com",
        )


class StubStore:
    def __init__(self, streamers: list[dict[str, str | None]] | None = None):
        self.streamers = streamers or [
            {"name": "xqc", "profile_image_url": "https://cdn/xqc.png"},
            {"name": "jason", "profile_image_url": None},
        ]
        self.logged_requests = []

    def list_searchable_streamers(self) -> list[dict[str, str | None]]:
        return list(self.streamers)

    def get_creator_id_by_name(self, name: str) -> int | None:
        normalized_name = (name or "").strip().lower()
        mapping = {"xqc": 1, "jason": 2}
        return mapping.get(normalized_name)

    def log_search_request(self, log) -> None:
        self.logged_requests.append(log)


class StubSearchJobService:
    def __init__(self):
        self.created_jobs: list[dict[str, object]] = []
        self.jobs: dict[int, SearchJobRecord] = {}

    def create_public_search_job(self, *, tiktok_url: str, streamer: str, creator_id: int | None) -> int:
        self.created_jobs.append(
            {
                "tiktok_url": tiktok_url,
                "streamer": streamer,
                "creator_id": creator_id,
            }
        )
        return 101

    def get_public_search_job(self, search_id: int) -> SearchJobRecord | None:
        return self.jobs.get(search_id)


def build_client(app_factory):
    app = app_factory(enable_lifespan=False)
    app.state.store = StubStore()
    app.state.search_manager = StubSearchManager()
    app.state.search_job_service = StubSearchJobService()
    return app, TestClient(app)


def test_public_search_endpoint_accepts_tiktok_url_only() -> None:
    app, client = build_client(create_public_app)

    with client:
        response = client.post(
            "/api/search/clip",
            data={"tiktok_url": "https://www.tiktok.com/@u/video/1", "streamer": "jason"},
        )

    assert response.status_code == 202
    assert response.json() == {"search_id": 101, "status": "queued", "stage": "validating"}
    assert app.state.search_manager.url_calls == 0
    assert app.state.search_job_service.created_jobs == [
        {
            "tiktok_url": "https://www.tiktok.com/@u/video/1",
            "streamer": "jason",
            "creator_id": 2,
        }
    ]
    assert len(app.state.store.logged_requests) == 0


def test_public_search_endpoint_rejects_file_upload() -> None:
    app, client = build_client(create_public_app)

    with client:
        response = client.post(
            "/api/search/clip",
            data={"streamer": "xqc"},
            files={"file": ("clip.mp4", io.BytesIO(b"video"), "video/mp4")},
        )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "INVALID_SEARCH_INPUT"
    assert app.state.search_job_service.created_jobs == []


def test_public_search_endpoint_validates_streamer() -> None:
    app, client = build_client(create_public_app)

    with client:
        response = client.post(
            "/api/search/clip",
            data={"tiktok_url": "https://www.tiktok.com/@u/video/1", "streamer": "ronaldo"},
        )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "INVALID_STREAMER"
    assert app.state.search_job_service.created_jobs == []


def test_public_search_job_endpoint_returns_job_status() -> None:
    app, client = build_client(create_public_app)
    app.state.search_job_service.jobs[101] = SearchJobRecord(
        id=101,
        status="completed",
        stage=None,
        created_at="2026-04-15T00:00:00Z",
        started_at="2026-04-15T00:00:01Z",
        finished_at="2026-04-15T00:00:02Z",
        result=SearchResult(found=False, streamer="jason", reason="url test"),
        error_code=None,
        error_message=None,
    )

    with client:
        response = client.get("/api/search/clip/101")

    assert response.status_code == 200
    assert response.json()["search_id"] == 101
    assert response.json()["status"] == "completed"
    assert response.json()["result"]["reason"] == "url test"


def test_public_search_job_endpoint_returns_404_for_unknown_job() -> None:
    app, client = build_client(create_public_app)

    with client:
        response = client.get("/api/search/clip/999")

    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "SEARCH_NOT_FOUND"


def test_admin_search_endpoint_accepts_tiktok_url() -> None:
    app, client = build_client(create_admin_app)

    with client:
        response = client.post(
            "/api/search/clip",
            data={"tiktok_url": "https://www.tiktok.com/@u/video/1", "streamer": "jason"},
        )

    assert response.status_code == 200
    assert app.state.search_manager.url_calls == 1
    assert app.state.search_manager.upload_calls == 0
    assert app.state.store.logged_requests[0].source_app == "admin"
    assert app.state.store.logged_requests[0].download_source == "tiktok"
    assert app.state.store.logged_requests[0].creator_id == 2


def test_admin_search_endpoint_accepts_file_upload() -> None:
    app, client = build_client(create_admin_app)

    with client:
        response = client.post(
            "/api/search/clip",
            data={"streamer": "xqc"},
            files={"file": ("clip.mp4", io.BytesIO(b"video"), "video/mp4")},
        )

    assert response.status_code == 200
    assert app.state.search_manager.upload_calls == 1
    assert app.state.search_manager.url_calls == 0
    assert app.state.search_manager.last_upload_filename == "clip.mp4"
    assert app.state.search_manager.last_streamer == "xqc"
    assert app.state.store.logged_requests[0].clip_filename == "clip.mp4"
    assert app.state.store.logged_requests[0].input_type == "file"
    assert app.state.store.logged_requests[0].creator_id == 1


def test_admin_search_endpoint_rejects_both_file_and_url() -> None:
    app, client = build_client(create_admin_app)

    with client:
        response = client.post(
            "/api/search/clip",
            data={"tiktok_url": "https://www.tiktok.com/@u/video/1", "streamer": "xqc"},
            files={"file": ("clip.mp4", io.BytesIO(b"video"), "video/mp4")},
        )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "INVALID_SEARCH_INPUT"
    assert app.state.store.logged_requests[0].input_type == "both"


def test_admin_search_endpoint_rejects_neither_file_nor_url() -> None:
    app, client = build_client(create_admin_app)

    with client:
        response = client.post("/api/search/clip", data={"streamer": "xqc"})

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "INVALID_SEARCH_INPUT"
    assert app.state.store.logged_requests[0].input_type is None


def test_admin_search_endpoint_validates_streamer_for_uploads() -> None:
    app, client = build_client(create_admin_app)

    with client:
        response = client.post(
            "/api/search/clip",
            data={"streamer": "ronaldo"},
            files={"file": ("clip.mp4", io.BytesIO(b"video"), "video/mp4")},
        )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "INVALID_STREAMER"
    assert app.state.store.logged_requests[0].clip_filename == "clip.mp4"
    assert app.state.store.logged_requests[0].error_code == "INVALID_STREAMER"


class FakeCursor:
    def __init__(self):
        self.executed: list[tuple[str, tuple | None]] = []

    def execute(self, query: str, params=None):
        self.executed.append((query, params))

    def fetchone(self):
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None


class FakeConnection:
    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None


def test_update_video_status_keeps_deleted_vods_processed_for_legacy_readers() -> None:
    cursor = FakeCursor()
    store = VectorStore.__new__(VectorStore)
    store._connect = lambda: FakeConnection(cursor)

    store.update_video_status(55, "deleted")

    assert cursor.executed == [
        ("UPDATE videos SET status = %s, processed = %s WHERE id = %s", ("deleted", True, 55))
    ]


def test_update_video_status_keeps_reindex_requested_vods_processed_for_legacy_readers() -> None:
    cursor = FakeCursor()
    store = VectorStore.__new__(VectorStore)
    store._connect = lambda: FakeConnection(cursor)

    store.update_video_status(56, "reindex_requested")

    assert cursor.executed == [
        ("UPDATE videos SET status = %s, processed = %s WHERE id = %s", ("reindex_requested", True, 56))
    ]
