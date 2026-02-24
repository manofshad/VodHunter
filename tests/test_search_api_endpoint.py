import io
import unittest
from pathlib import Path
import sys

from fastapi import HTTPException, UploadFile

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import backend.main as api_main
from backend.services.remote_clip_downloader import DownloadError, InvalidTikTokUrlError
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


class TestSearchApiEndpoint(unittest.TestCase):
    def setUp(self) -> None:
        self.search_manager = StubSearchManager()
        api_main.app.state.search_manager = self.search_manager

    def test_file_only_is_accepted(self) -> None:
        file = UploadFile(filename="clip.mp4", file=io.BytesIO(b"data"))
        response = api_main.search_clip(file=file, tiktok_url=None)
        self.assertFalse(response.found)
        self.assertIsNone(response.video_url_at_timestamp)
        self.assertEqual(self.search_manager.upload_calls, 1)
        self.assertEqual(self.search_manager.url_calls, 0)

    def test_tiktok_url_only_is_accepted(self) -> None:
        response = api_main.search_clip(file=None, tiktok_url="https://www.tiktok.com/@u/video/1")
        self.assertFalse(response.found)
        self.assertEqual(self.search_manager.upload_calls, 0)
        self.assertEqual(self.search_manager.url_calls, 1)

    def test_both_inputs_rejected(self) -> None:
        file = UploadFile(filename="clip.mp4", file=io.BytesIO(b"data"))
        with self.assertRaises(HTTPException) as ctx:
            api_main.search_clip(file=file, tiktok_url="https://www.tiktok.com/@u/video/1")
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.detail["code"], "INVALID_SEARCH_INPUT")

    def test_neither_input_rejected(self) -> None:
        with self.assertRaises(HTTPException) as ctx:
            api_main.search_clip(file=None, tiktok_url=None)
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.detail["code"], "INVALID_SEARCH_INPUT")

    def test_invalid_tiktok_url_maps_error_code(self) -> None:
        self.search_manager.raise_url = InvalidTikTokUrlError("bad url")
        with self.assertRaises(HTTPException) as ctx:
            api_main.search_clip(file=None, tiktok_url="https://example.com/v")
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.detail["code"], "INVALID_TIKTOK_URL")

    def test_download_error_maps_error_code(self) -> None:
        self.search_manager.raise_url = DownloadError("download failed")
        with self.assertRaises(HTTPException) as ctx:
            api_main.search_clip(file=None, tiktok_url="https://www.tiktok.com/@u/video/1")
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.detail["code"], "DOWNLOAD_ERROR")


if __name__ == "__main__":
    unittest.main()
