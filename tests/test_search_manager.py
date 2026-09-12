import os
import tempfile
from datetime import datetime, timezone

import pytest

from backend.services.media_duration import MediaDurationError
from backend.services.remote_clip_downloader import DownloadResult
from backend.services.search_manager import InputDurationExceededError, SearchInputError, SearchManager
from search.models import SearchDateRange, SearchExecutionMetadata, SearchExecutionResult, SearchResult


class FakeSearchService:
    def __init__(self):
        self.searched_paths: list[tuple[str, str]] = []
        self.date_ranges: list[SearchDateRange | None] = []
        self.raise_on_search = False

    def search_file(
        self,
        path: str,
        streamer: str,
        date_range: SearchDateRange | None = None,
        on_stage_change=None,
        on_stage_timing=None,
        query_duration_seconds: float | None = None,
    ) -> SearchExecutionResult:
        self.searched_paths.append((path, streamer))
        self.date_ranges.append(date_range)
        if self.raise_on_search:
            raise RuntimeError("search failed")
        return SearchExecutionResult(
            result=SearchResult(found=False, reason="no match"),
            metadata=SearchExecutionMetadata(result_reason="no match", found_match=False),
        )


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
            manager = SearchManager(
                search_service=service,
                remote_downloader=downloader,
                duration_probe=lambda _: 1.0,
            )
            stage_timings: list[tuple[str, int]] = []
            outcome = manager.search_tiktok_url(
                "https://www.tiktok.com/@user/video/1",
                "xqc",
                on_stage_timing=lambda stage, duration_ms: stage_timings.append(
                    (stage, duration_ms)
                ),
            )
            assert downloader.download_calls == ["https://www.tiktok.com/@user/video/1"]
            assert service.searched_paths == [(clip_path, "xqc")]
            assert downloader.cleaned_paths == [clip_path]
            assert outcome.download_source == "tiktok"
            assert outcome.download_host == "www.tiktok.com"
            assert {stage for stage, _ in stage_timings} >= {"download", "probe"}
            assert outcome.execution_metadata.download_duration_ms is not None
            assert outcome.execution_metadata.probe_duration_ms is not None

    def test_search_tiktok_url_forwards_date_range(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            clip_path = os.path.join(tmp, "clip.mp4")
            with open(clip_path, "wb") as f:
                f.write(b"clip")
            date_range = SearchDateRange(
                streamed_from=datetime(2026, 4, 1, tzinfo=timezone.utc),
                streamed_to=datetime(2026, 4, 8, tzinfo=timezone.utc),
            )
            service = FakeSearchService()
            downloader = FakeDownloader(downloaded_path=clip_path)
            manager = SearchManager(
                search_service=service,
                remote_downloader=downloader,
                duration_probe=lambda _: 1.0,
            )
            outcome = manager.search_tiktok_url("https://www.tiktok.com/@user/video/1", "xqc", date_range=date_range)
            assert service.date_ranges == [date_range]
            assert outcome.date_range == date_range

    def test_search_tiktok_url_cleans_up_on_search_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            clip_path = os.path.join(tmp, "clip.mp4")
            with open(clip_path, "wb") as f:
                f.write(b"clip")
            service = FakeSearchService()
            service.raise_on_search = True
            downloader = FakeDownloader(downloaded_path=clip_path)
            manager = SearchManager(
                search_service=service,
                remote_downloader=downloader,
                duration_probe=lambda _: 1.0,
            )
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
            manager = SearchManager(
                search_service=service,
                remote_downloader=downloader,
                duration_probe=lambda _: 1.0,
            )
            outcome = manager.search_tiktok_url("https://www.tiktok.com/@user/video/1", "xqc")
            assert downloader.download_calls == ["https://www.tiktok.com/@user/video/1"]
            assert service.searched_paths == [(clip_path, "xqc")]
            assert outcome.total_duration_ms is not None

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
            manager = SearchManager(
                search_service=service,
                remote_downloader=downloader,
                duration_probe=lambda _: 1.0,
            )
            with pytest.raises(SearchInputError):
                manager.search_tiktok_url("https://www.tiktok.com/@user/video/1", "   ")
