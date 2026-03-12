import io

import pytest
from fastapi import HTTPException, UploadFile

from backend.apps.admin import create_admin_app
from backend.apps.public import create_public_app
from backend.routers.search import list_searchable_streamers, search_clip
from backend.services.remote_clip_downloader import DownloadError, InvalidTikTokUrlError
from backend.services.search_manager import InputDurationExceededError
from search.models import SearchResult


class StubSearchManager:
    def __init__(self):
        self.upload_calls = 0
        self.url_calls = 0
        self.last_streamer: str | None = None
        self.raise_upload: Exception | None = None
        self.raise_url: Exception | None = None

    def search_upload(self, file: UploadFile, streamer: str) -> SearchResult:
        self.upload_calls += 1
        self.last_streamer = streamer
        if self.raise_upload is not None:
            raise self.raise_upload
        return SearchResult(
            found=False,
            streamer=streamer,
            reason="upload test",
            thumbnail_url=None,
            video_url_at_timestamp=None,
        )

    def search_tiktok_url(self, url: str, streamer: str) -> SearchResult:
        self.url_calls += 1
        self.last_streamer = streamer
        if self.raise_url is not None:
            raise self.raise_url
        return SearchResult(
            found=False,
            streamer=streamer,
            reason="url test",
            thumbnail_url=None,
            video_url_at_timestamp=None,
        )


class StubStore:
    def __init__(self, streamers: list[str] | None = None):
        self.streamers = streamers or ["xqc", "jason"]

    def list_searchable_streamers(self) -> list[str]:
        return list(self.streamers)


class FakeRequest:
    def __init__(self, app):
        self.app = app


def assert_search_behavior_for_app(app) -> None:
    search_manager = StubSearchManager()
    app.state.store = StubStore()
    app.state.search_manager = search_manager
    request = FakeRequest(app)

    file = UploadFile(filename="clip.mp4", file=io.BytesIO(b"data"))
    response = search_clip(request, file=file, tiktok_url=None, streamer="xQc")
    assert not response.found
    assert response.thumbnail_url is None
    assert response.video_url_at_timestamp is None
    assert search_manager.upload_calls == 1
    assert search_manager.url_calls == 0
    assert search_manager.last_streamer == "xqc"

    search_manager = StubSearchManager()
    app.state.search_manager = search_manager
    response = search_clip(request, file=None, tiktok_url="https://www.tiktok.com/@u/video/1", streamer="jason")
    assert not response.found
    assert search_manager.upload_calls == 0
    assert search_manager.url_calls == 1
    assert search_manager.last_streamer == "jason"

    search_manager = StubSearchManager()
    app.state.search_manager = search_manager
    file = UploadFile(filename="clip.mp4", file=io.BytesIO(b"data"))
    with pytest.raises(HTTPException) as exc_info:
        search_clip(request, file=file, tiktok_url="https://www.tiktok.com/@u/video/1", streamer="xqc")
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["code"] == "INVALID_SEARCH_INPUT"

    with pytest.raises(HTTPException) as exc_info:
        search_clip(request, file=None, tiktok_url=None, streamer="xqc")
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["code"] == "INVALID_SEARCH_INPUT"

    with pytest.raises(HTTPException) as exc_info:
        search_clip(request, file=None, tiktok_url="https://www.tiktok.com/@u/video/1", streamer=None)
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["code"] == "INVALID_STREAMER"

    with pytest.raises(HTTPException) as exc_info:
        search_clip(request, file=None, tiktok_url="https://www.tiktok.com/@u/video/1", streamer="ronaldo")
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["code"] == "INVALID_STREAMER"

    search_manager.raise_url = InvalidTikTokUrlError("bad url")
    with pytest.raises(HTTPException) as exc_info:
        search_clip(request, file=None, tiktok_url="https://example.com/v", streamer="xqc")
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["code"] == "INVALID_TIKTOK_URL"

    search_manager.raise_url = DownloadError("download failed")
    with pytest.raises(HTTPException) as exc_info:
        search_clip(request, file=None, tiktok_url="https://www.tiktok.com/@u/video/1", streamer="xqc")
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["code"] == "DOWNLOAD_ERROR"

    search_manager.raise_url = InputDurationExceededError(duration_seconds=214.2, max_duration_seconds=180)
    with pytest.raises(HTTPException) as exc_info:
        search_clip(request, file=None, tiktok_url="https://www.tiktok.com/@u/video/1", streamer="xqc")
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["code"] == "INPUT_DURATION_EXCEEDED"


def test_list_searchable_streamers_endpoint() -> None:
    app = create_public_app(enable_lifespan=False)
    app.state.store = StubStore(streamers=["jason", "xqc"])
    request = FakeRequest(app)

    response = list_searchable_streamers(request)

    assert [item.name for item in response] == ["jason", "xqc"]


@pytest.mark.parametrize(
    "app_factory",
    [create_public_app, create_admin_app],
    ids=["public", "admin"],
)
def test_search_endpoint_behavior_public_and_admin(app_factory) -> None:
    assert_search_behavior_for_app(app_factory(enable_lifespan=False))
