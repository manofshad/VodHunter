import io
import unittest
from pathlib import Path
import sys

from fastapi import HTTPException, UploadFile

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from backend.apps.admin import create_admin_app
from backend.apps.public import create_public_app
from backend.routers.search import search_clip
from backend.services.remote_clip_downloader import DownloadError, InvalidTikTokUrlError
from backend.services.search_manager import InputDurationExceededError
from search.models import SearchResult


class StubSearchManager:
    def __init__(self):
        self.upload_calls = 0
        self.url_calls = 0
        self.raise_upload: Exception | None = None
        self.raise_url: Exception | None = None

    def search_upload(self, file: UploadFile) -> SearchResult:
        self.upload_calls += 1
        if self.raise_upload is not None:
            raise self.raise_upload
        return SearchResult(
            found=False,
            reason="upload test",
            video_url_at_timestamp=None,
        )

    def search_tiktok_url(self, url: str) -> SearchResult:
        self.url_calls += 1
        if self.raise_url is not None:
            raise self.raise_url
        return SearchResult(
            found=False,
            reason="url test",
            video_url_at_timestamp=None,
        )


class FakeRequest:
    def __init__(self, app):
        self.app = app


class TestSearchApiEndpoint(unittest.TestCase):
    def _assert_for_app(self, app) -> None:
        self.search_manager = StubSearchManager()
        app.state.search_manager = self.search_manager
        request = FakeRequest(app)

        file = UploadFile(filename="clip.mp4", file=io.BytesIO(b"data"))
        response = search_clip(request, file=file, tiktok_url=None)
        self.assertFalse(response.found)
        self.assertIsNone(response.video_url_at_timestamp)
        self.assertEqual(self.search_manager.upload_calls, 1)
        self.assertEqual(self.search_manager.url_calls, 0)

        self.search_manager = StubSearchManager()
        app.state.search_manager = self.search_manager
        response = search_clip(request, file=None, tiktok_url="https://www.tiktok.com/@u/video/1")
        self.assertFalse(response.found)
        self.assertEqual(self.search_manager.upload_calls, 0)
        self.assertEqual(self.search_manager.url_calls, 1)

        self.search_manager = StubSearchManager()
        app.state.search_manager = self.search_manager
        file = UploadFile(filename="clip.mp4", file=io.BytesIO(b"data"))
        with self.assertRaises(HTTPException) as ctx:
            search_clip(request, file=file, tiktok_url="https://www.tiktok.com/@u/video/1")
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.detail["code"], "INVALID_SEARCH_INPUT")

        with self.assertRaises(HTTPException) as ctx:
            search_clip(request, file=None, tiktok_url=None)
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.detail["code"], "INVALID_SEARCH_INPUT")

        self.search_manager.raise_url = InvalidTikTokUrlError("bad url")
        with self.assertRaises(HTTPException) as ctx:
            search_clip(request, file=None, tiktok_url="https://example.com/v")
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.detail["code"], "INVALID_TIKTOK_URL")

        self.search_manager.raise_url = DownloadError("download failed")
        with self.assertRaises(HTTPException) as ctx:
            search_clip(request, file=None, tiktok_url="https://www.tiktok.com/@u/video/1")
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.detail["code"], "DOWNLOAD_ERROR")

        self.search_manager.raise_url = InputDurationExceededError(duration_seconds=214.2, max_duration_seconds=180)
        with self.assertRaises(HTTPException) as ctx:
            search_clip(request, file=None, tiktok_url="https://www.tiktok.com/@u/video/1")
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.detail["code"], "INPUT_DURATION_EXCEEDED")

    def test_search_endpoint_behavior_public_and_admin(self) -> None:
        for app in (create_public_app(enable_lifespan=False), create_admin_app(enable_lifespan=False)):
            with self.subTest(app_title=app.title):
                self._assert_for_app(app)


if __name__ == "__main__":
    unittest.main()
