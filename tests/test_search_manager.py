import io
import os
import tempfile
import unittest
from pathlib import Path
import sys

from fastapi import UploadFile

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from backend.services.remote_clip_downloader import DownloadResult
from backend.services.search_manager import SearchBusyError, SearchManager
from search.models import SearchResult


class FakeMonitorManager:
    def __init__(self, can_search: bool):
        self._can_search = can_search

    def can_search(self) -> bool:
        return self._can_search


class FakeSearchService:
    def __init__(self):
        self.searched_paths: list[str] = []
        self.raise_on_search = False

    def search_file(self, path: str) -> SearchResult:
        self.searched_paths.append(path)
        if self.raise_on_search:
            raise RuntimeError("search failed")
        return SearchResult(found=False, reason="no match")


class FakeDownloader:
    def __init__(self, downloaded_path: str):
        self.downloaded_path = downloaded_path
        self.download_calls: list[str] = []
        self.cleaned_paths: list[str] = []

    def download_tiktok(self, url: str) -> DownloadResult:
        self.download_calls.append(url)
        return DownloadResult(path=self.downloaded_path)

    def cleanup(self, path: str) -> None:
        self.cleaned_paths.append(path)


class TestSearchManager(unittest.TestCase):
    def test_search_tiktok_url_downloads_then_searches(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            clip_path = os.path.join(tmp, "clip.mp4")
            with open(clip_path, "wb") as f:
                f.write(b"clip")

            service = FakeSearchService()
            downloader = FakeDownloader(downloaded_path=clip_path)
            manager = SearchManager(
                search_service=service,  # type: ignore[arg-type]
                monitor_manager=FakeMonitorManager(can_search=True),  # type: ignore[arg-type]
                upload_temp_dir=tmp,
                remote_downloader=downloader,  # type: ignore[arg-type]
            )

            manager.search_tiktok_url("https://www.tiktok.com/@user/video/1")

            self.assertEqual(downloader.download_calls, ["https://www.tiktok.com/@user/video/1"])
            self.assertEqual(service.searched_paths, [clip_path])
            self.assertEqual(downloader.cleaned_paths, [clip_path])

    def test_search_tiktok_url_cleans_up_on_search_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            clip_path = os.path.join(tmp, "clip.mp4")
            with open(clip_path, "wb") as f:
                f.write(b"clip")

            service = FakeSearchService()
            service.raise_on_search = True
            downloader = FakeDownloader(downloaded_path=clip_path)
            manager = SearchManager(
                search_service=service,  # type: ignore[arg-type]
                monitor_manager=FakeMonitorManager(can_search=True),  # type: ignore[arg-type]
                upload_temp_dir=tmp,
                remote_downloader=downloader,  # type: ignore[arg-type]
            )

            with self.assertRaises(RuntimeError):
                manager.search_tiktok_url("https://www.tiktok.com/@user/video/1")

            self.assertEqual(downloader.cleaned_paths, [clip_path])

    def test_search_blocked_for_tiktok_when_monitor_not_idle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = FakeSearchService()
            downloader = FakeDownloader(downloaded_path=os.path.join(tmp, "clip.mp4"))
            manager = SearchManager(
                search_service=service,  # type: ignore[arg-type]
                monitor_manager=FakeMonitorManager(can_search=False),  # type: ignore[arg-type]
                upload_temp_dir=tmp,
                remote_downloader=downloader,  # type: ignore[arg-type]
            )

            with self.assertRaises(SearchBusyError):
                manager.search_tiktok_url("https://www.tiktok.com/@user/video/1")

            self.assertEqual(downloader.download_calls, [])

    def test_upload_temp_file_is_removed_after_search(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = FakeSearchService()
            downloader = FakeDownloader(downloaded_path=os.path.join(tmp, "clip.mp4"))
            manager = SearchManager(
                search_service=service,  # type: ignore[arg-type]
                monitor_manager=FakeMonitorManager(can_search=True),  # type: ignore[arg-type]
                upload_temp_dir=tmp,
                remote_downloader=downloader,  # type: ignore[arg-type]
            )

            upload = UploadFile(filename="query.mp4", file=io.BytesIO(b"video"))
            manager.search_upload(upload)

            temp_files = [name for name in os.listdir(tmp) if name.startswith("upload_")]
            self.assertEqual(temp_files, [])
            self.assertEqual(len(service.searched_paths), 1)


if __name__ == "__main__":
    unittest.main()
