import os
import tempfile

import pytest

from backend.services.media_duration import MediaDurationError
from backend.services.remote_clip_downloader import DownloadResult
from backend.services.search_manager import InputDurationExceededError, SearchInputError, SearchManager
from search.models import SearchResult


class FakeSearchService:
    def __init__(self):
        self.searched_paths: list[tuple[str, str]] = []
        self.raise_on_search = False

    def search_file(self, path: str, streamer: str) -> SearchResult:
        self.searched_paths.append((path, streamer))
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


class TestSearchManager:
    def test_search_tiktok_url_downloads_then_searches(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            clip_path = os.path.join(tmp, "clip.mp4")
            with open(clip_path, "wb") as f:
                f.write(b"clip")
            service = FakeSearchService()
            downloader = FakeDownloader(downloaded_path=clip_path)
            manager = SearchManager(search_service=service, remote_downloader=downloader)
            manager.search_tiktok_url("https://www.tiktok.com/@user/video/1", "xqc")
            assert downloader.download_calls == ["https://www.tiktok.com/@user/video/1"]
            assert service.searched_paths == [(clip_path, "xqc")]
            assert downloader.cleaned_paths == [clip_path]

    def test_search_tiktok_url_cleans_up_on_search_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            clip_path = os.path.join(tmp, "clip.mp4")
            with open(clip_path, "wb") as f:
                f.write(b"clip")
            service = FakeSearchService()
            service.raise_on_search = True
            downloader = FakeDownloader(downloaded_path=clip_path)
            manager = SearchManager(search_service=service, remote_downloader=downloader)
            with pytest.raises(RuntimeError):
                manager.search_tiktok_url("https://www.tiktok.com/@user/video/1", "xqc")
            assert downloader.cleaned_paths == [clip_path]

    def test_search_tiktok_url_not_blocked_when_monitor_would_be_busy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            clip_path = os.path.join(tmp, "clip.mp4")
            with open(clip_path, "wb") as f:
                f.write(b"clip")
            service = FakeSearchService()
            downloader = FakeDownloader(downloaded_path=clip_path)
            manager = SearchManager(search_service=service, remote_downloader=downloader)
            manager.search_tiktok_url("https://www.tiktok.com/@user/video/1", "xqc")
            assert downloader.download_calls == ["https://www.tiktok.com/@user/video/1"]
            assert service.searched_paths == [(clip_path, "xqc")]

    def test_tiktok_rejects_when_duration_exceeds_limit_and_cleans(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            clip_path = os.path.join(tmp, "clip.mp4")
            with open(clip_path, "wb") as f:
                f.write(b"clip")
            service = FakeSearchService()
            downloader = FakeDownloader(downloaded_path=clip_path)
            manager = SearchManager(
                search_service=service,
                remote_downloader=downloader,
                max_duration_seconds=180,
                duration_probe=lambda _: 214.2,
            )
            with pytest.raises(InputDurationExceededError):
                manager.search_tiktok_url("https://www.tiktok.com/@user/video/1", "xqc")
            assert service.searched_paths == []
            assert downloader.cleaned_paths == [clip_path]

    def test_tiktok_rejects_when_duration_probe_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            clip_path = os.path.join(tmp, "clip.mp4")
            with open(clip_path, "wb") as f:
                f.write(b"clip")
            service = FakeSearchService()
            downloader = FakeDownloader(downloaded_path=clip_path)

            def bad_probe(_: str) -> float:
                raise MediaDurationError("Could not determine input video duration")

            manager = SearchManager(
                search_service=service,
                remote_downloader=downloader,
                max_duration_seconds=180,
                duration_probe=bad_probe,
            )
            with pytest.raises(SearchInputError):
                manager.search_tiktok_url("https://www.tiktok.com/@user/video/1", "xqc")
            assert service.searched_paths == []

    def test_search_requires_streamer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            clip_path = os.path.join(tmp, "clip.mp4")
            with open(clip_path, "wb") as f:
                f.write(b"clip")
            service = FakeSearchService()
            downloader = FakeDownloader(downloaded_path=clip_path)
            manager = SearchManager(search_service=service, remote_downloader=downloader)
            with pytest.raises(SearchInputError):
                manager.search_tiktok_url("https://www.tiktok.com/@user/video/1", "   ")
