import io

from fastapi.testclient import TestClient

from backend.apps.admin import create_admin_app
from backend.apps.public import create_public_app
from search.models import SearchExecutionMetadata, SearchRequestOutcome, SearchResult


class StubSearchManager:
    def __init__(self):
        self.upload_calls = 0
        self.url_calls = 0
        self.last_streamer: str | None = None
        self.last_upload_filename: str | None = None
        self.raise_upload: Exception | None = None
        self.raise_url: Exception | None = None

    def search_upload(self, file, streamer: str) -> SearchRequestOutcome:
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

    def search_tiktok_url(self, url: str, streamer: str) -> SearchRequestOutcome:
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


def build_client(app_factory):
    app = app_factory(enable_lifespan=False)
    app.state.store = StubStore()
    app.state.search_manager = StubSearchManager()
    return app, TestClient(app)


def test_public_search_endpoint_accepts_tiktok_url_only() -> None:
    app, client = build_client(create_public_app)

    with client:
        response = client.post(
            "/api/search/clip",
            data={"tiktok_url": "https://www.tiktok.com/@u/video/1", "streamer": "jason"},
        )

    assert response.status_code == 200
    assert response.json()["profile_image_url"] == "https://cdn/profile.png"
    assert app.state.search_manager.url_calls == 1
    assert app.state.search_manager.last_streamer == "jason"
    assert len(app.state.store.logged_requests) == 1
    assert app.state.store.logged_requests[0].source_app == "public"
    assert app.state.store.logged_requests[0].success is True
    assert app.state.store.logged_requests[0].creator_id == 2


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
    assert len(app.state.store.logged_requests) == 1
    assert app.state.store.logged_requests[0].error_code == "INVALID_SEARCH_INPUT"


def test_public_search_endpoint_validates_streamer() -> None:
    app, client = build_client(create_public_app)

    with client:
        response = client.post(
            "/api/search/clip",
            data={"tiktok_url": "https://www.tiktok.com/@u/video/1", "streamer": "ronaldo"},
        )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "INVALID_STREAMER"
    assert len(app.state.store.logged_requests) == 1
    assert app.state.store.logged_requests[0].error_code == "INVALID_STREAMER"
    assert app.state.store.logged_requests[0].creator_id is None


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
