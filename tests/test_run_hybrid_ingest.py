import threading
import time
import io
import json
import socket
import urllib.error
import urllib.parse
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from pipeline.scheduled_embedder import BACKLOG_PRIORITY, LIVE_PRIORITY
from runners.run_hybrid_ingest import (
    HybridIngestResult,
    _build_backlog,
    _normalize_streamers,
    main,
    run_hybrid_ingest,
    run_multi_streamer_ingest,
)
from storage.records import VodIngestStateRecord, VideoRecord, VideoStatus
from services.twitch_monitor import TwitchMonitor, TwitchRequestError


class FakeMonitor:
    def __init__(
        self,
        *,
        live_sequence: list[bool] | None = None,
        vods: list[dict[str, object]] | None = None,
        vods_by_id: dict[str, dict[str, object]] | None = None,
    ):
        self.live_sequence = list(live_sequence or [False])
        self.vods = list(vods or [])
        self.vods_by_id = dict(vods_by_id or {})
        self.user_profile = {
            "id": "user-1",
            "login": "alice",
            "display_name": "alice",
            "profile_image_url": "https://cdn/profile.png",
        }

    def is_live(self, streamer: str) -> bool:
        if len(self.live_sequence) > 1:
            return bool(self.live_sequence.pop(0))
        return bool(self.live_sequence[0])

    def get_user_profile(self, streamer: str, force_refresh: bool = False) -> dict[str, str]:
        return dict(self.user_profile)

    def list_archive_vods_since(self, user_id: str, created_after):
        return [dict(vod) for vod in self.vods]

    def get_archive_vod_by_id(self, vod_platform_id: str, *, expected_user_id: str):
        assert expected_user_id == "user-1"
        vod = self.vods_by_id.get(vod_platform_id)
        return dict(vod) if vod is not None else None


class FakeStore:
    def __init__(self):
        self.videos_by_url: dict[str, VideoRecord] = {}
        self.vod_state: dict[str, VodIngestStateRecord] = {}
        self.video_status_by_id: dict[int, str | None] = {}
        self.deleted_vod_state_ids: list[str] = []

    def set_video(
        self,
        *,
        video_id: int,
        creator_id: int,
        url: str,
        title: str,
        processed: bool,
        status: str | None,
        thumbnail_url: str | None = None,
        streamed_at: datetime | None = None,
    ) -> None:
        self.videos_by_url[url] = VideoRecord(
            id=video_id,
            creator_id=creator_id,
            url=url,
            title=title,
            thumbnail_url=thumbnail_url,
            status=VideoStatus(status) if status is not None else None,
            processed=processed,
            streamed_at=streamed_at,
        )
        self.video_status_by_id[video_id] = status

    def set_state(
        self,
        *,
        vod_platform_id: str,
        video_id: int,
        streamer: str,
        last_ingested_seconds: int,
        last_seen_duration_seconds: int,
    ) -> None:
        self.vod_state[vod_platform_id] = VodIngestStateRecord(
            vod_platform_id=vod_platform_id,
            video_id=video_id,
            streamer=streamer,
            last_ingested_seconds=last_ingested_seconds,
            last_seen_duration_seconds=last_seen_duration_seconds,
            updated_at=datetime.now(timezone.utc),
        )

    def get_video_by_url(self, url: str):
        return self.videos_by_url.get(url)

    def get_video_status(self, video_id: int):
        return self.video_status_by_id.get(int(video_id))

    def get_vod_ingest_state(self, vod_platform_id: str):
        return self.vod_state.get(vod_platform_id)

    def get(self, vod_platform_id: str):
        return self.get_vod_ingest_state(vod_platform_id)

    def list_pending(self, streamer: str):
        return [
            state
            for state in self.vod_state.values()
            if state.streamer == streamer
            and self.get_video_status(state.video_id) == "indexing"
        ]

    def mark_source_unavailable(self, expected: VodIngestStateRecord, *, stale_before):
        if self.vod_state.get(expected.vod_platform_id) != expected or expected.updated_at >= stale_before:
            return False
        url = f"https://www.twitch.tv/videos/{expected.vod_platform_id}"
        video = self.videos_by_url.get(url)
        if video is None or video.status is not VideoStatus.INDEXING or video.id != expected.video_id:
            return False
        self.videos_by_url[url] = replace(video, status=VideoStatus.UNAVAILABLE, processed=True)
        self.video_status_by_id[video.id] = "unavailable"
        return True

    def delete_vod_ingest_state(self, vod_platform_id: str) -> None:
        self.deleted_vod_state_ids.append(vod_platform_id)
        self.vod_state.pop(vod_platform_id, None)

    def delete(self, vod_platform_id: str) -> None:
        self.delete_vod_ingest_state(vod_platform_id)


class FakeRepositories:
    def __init__(self, store: FakeStore):
        self.videos = store
        self.ingest_states = store
        self.fingerprints = store


class WorkerEmbedder:
    load_calls = 0

    def load(self):
        self.load_calls += 1
        return 0


def twitch_failure(*, retryable=True, retry_at=None, status=None):
    return TwitchRequestError(
        operation="helix_videos", hostname="api.twitch.tv", error_class="TimeoutError",
        retryable=retryable, retry_at=retry_at, status=status,
    )


class FakeSource:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.vod_metadata = kwargs.get("vod_metadata")


class FakeBacklogSession:
    runs: list[str] = []
    stop_calls: list[str] = []
    raise_error_for: set[str] = set()
    run_started_event: threading.Event | None = None

    def __init__(self, source, embedder, fingerprints, poll_interval):
        self.source = source
        self.vod_id = str(source.vod_metadata["id"])
        self.stopped = False

    def run(self) -> None:
        FakeBacklogSession.runs.append(self.vod_id)
        if FakeBacklogSession.run_started_event is not None:
            FakeBacklogSession.run_started_event.set()
        progress_callback = self.source.kwargs.get("progress_callback")
        if progress_callback is not None:
            last_ingested_seconds = 0
            if self.vod_id == "resume":
                last_ingested_seconds = 90
            progress_callback(
                {
                    "event": "chunk_start",
                    "vod_id": self.vod_id,
                    "start_seconds": last_ingested_seconds,
                    "end_seconds": last_ingested_seconds + 30,
                    "percent_complete": 100.0 if self.vod_id == "resume" else 25.0,
                }
            )
        time.sleep(0.02)
        if self.vod_id in FakeBacklogSession.raise_error_for:
            raise RuntimeError(f"boom-{self.vod_id}")
        if not self.stopped:
            if progress_callback is not None:
                progress_callback(
                    {
                        "event": "vod_complete",
                        "vod_id": self.vod_id,
                    }
                )
            self.source.kwargs["videos"].set_video(
                video_id=1,
                creator_id=1,
                url=str(self.source.vod_metadata["url"]),
                title="done",
                thumbnail_url=None,
                processed=True,
                status="searchable",
            )
            self.source.kwargs["videos"].vod_state.pop(self.vod_id, None)

    def stop(self) -> None:
        self.stopped = True
        FakeBacklogSession.stop_calls.append(self.vod_id)
        self.source.kwargs["videos"].set_state(
            vod_platform_id=self.vod_id,
            video_id=1,
            streamer="alice",
            last_ingested_seconds=60,
            last_seen_duration_seconds=120,
        )


class FakeLiveSession:
    runs = 0
    stop_calls = 0

    def __init__(self, source, embedder, fingerprints, poll_interval):
        self.source = source

    def run(self) -> None:
        FakeLiveSession.runs += 1
        time.sleep(0.01)

    def stop(self) -> None:
        FakeLiveSession.stop_calls += 1


class HybridSessionFactory:
    def __call__(self, source, embedder, fingerprints, poll_interval):
        if getattr(source, "vod_metadata", None) is not None:
            return FakeBacklogSession(source, embedder, fingerprints, poll_interval)
        return FakeLiveSession(source, embedder, fingerprints, poll_interval)


class TestRunHybridIngest:
    def setup_method(self) -> None:
        FakeBacklogSession.runs = []
        FakeBacklogSession.stop_calls = []
        FakeBacklogSession.raise_error_for = set()
        FakeBacklogSession.run_started_event = None
        FakeLiveSession.runs = 0
        FakeLiveSession.stop_calls = 0

    def test_saved_cursor_omitted_from_discovery_resumes_outside_lookback(self) -> None:
        vod_id = "2878694994"
        url = f"https://www.twitch.tv/videos/{vod_id}"
        monitor = FakeMonitor(
            vods=[],
            vods_by_id={
                vod_id: {
                    "id": vod_id,
                    "url": url,
                    "user_id": "user-1",
                    "created_at": "2000-01-01T00:00:00Z",
                    "duration_seconds": 22768,
                },
            },
        )
        store = FakeStore()
        store.set_video(
            video_id=50,
            creator_id=1,
            url=url,
            title="Saved partial VOD",
            processed=False,
            status="indexing",
        )
        store.set_state(
            vod_platform_id=vod_id,
            video_id=50,
            streamer="alice",
            last_ingested_seconds=22648,
            last_seen_duration_seconds=22768,
        )
        logs: list[str] = []
        stop_flag = {"done": False}

        def out(line: str) -> None:
            logs.append(line)
            if line.startswith("mode=watch") or line.startswith("completed mode=backlog"):
                stop_flag["done"] = True

        result = run_hybrid_ingest(
            "alice",
            days=1,
            monitor=monitor,
            build_storage=lambda: FakeRepositories(store),
            build_ingest=lambda: {"embedder": object()},
            historical_source_factory=FakeSource,
            live_source_factory=FakeSource,
            session_factory=HybridSessionFactory(),
            out=out,
            should_stop=lambda: stop_flag["done"],
            watch_poll_seconds=0.0,
            backlog_live_poll_seconds=0.0,
            session_wait_seconds=0.0,
            retry_seconds=0.0,
        )

        assert FakeBacklogSession.runs == ["2878694994"]
        assert result.resumed == 1
        assert (
            "starting mode=backlog vod=2878694994 "
            "url=https://www.twitch.tv/videos/2878694994 cursor=22648 backlog=1"
        ) in logs

    @pytest.mark.parametrize("lookup_outcome", ["absent", "recent", "error", "short_duration", "advanced"])
    def test_omitted_cursor_closes_only_on_confirmed_stale_unchanged_absence(self, lookup_outcome) -> None:
        vod_id = "2878694994"
        url = f"https://www.twitch.tv/videos/{vod_id}"
        store = FakeStore()
        store.set_video(video_id=50, creator_id=1, url=url, title="Partial", processed=False, status="indexing")
        store.set_state(
            vod_platform_id=vod_id, video_id=50, streamer="alice",
            last_ingested_seconds=22648, last_seen_duration_seconds=22768,
        )
        if lookup_outcome != "recent":
            store.vod_state[vod_id] = replace(
                store.vod_state[vod_id], updated_at=datetime.now(timezone.utc) - timedelta(hours=1),
            )
        expected = store.vod_state[vod_id]
        monitor = FakeMonitor()

        def lookup(*args, **kwargs):
            if lookup_outcome == "error":
                raise RuntimeError("Twitch request failed")
            if lookup_outcome == "short_duration":
                return {"id": vod_id, "url": url, "duration_seconds": 22647}
            if lookup_outcome == "advanced":
                store.vod_state[vod_id] = replace(expected, last_ingested_seconds=22708)
            return None

        logs = []
        with patch.object(monitor, "get_archive_vod_by_id", side_effect=lookup):
            backlog = _build_backlog(
                twitch_monitor=monitor, videos=store, ingest_states=store, streamer="alice",
                user_id="user-1", days=1, skipped_vods_logged=set(), out=logs.append,
            )

        assert backlog == []
        assert store.get_video_by_url(url).status is (
            VideoStatus.UNAVAILABLE if lookup_outcome == "absent" else VideoStatus.INDEXING
        )
        assert store.vod_state[vod_id] == (
            replace(expected, last_ingested_seconds=22708) if lookup_outcome == "advanced" else expected
        )
        assert any("event=source_unavailable" in line for line in logs) is (lookup_outcome == "absent")
        assert any("error=source_lookup_failed" in line for line in logs) is (
            lookup_outcome in {"error", "short_duration"}
        )

    def test_lookup_failure_does_not_block_other_saved_cursor_recovery(self) -> None:
        store = FakeStore()
        monitor = FakeMonitor()
        for video_id, vod_id in enumerate(["failed", "available"], start=1):
            url = f"https://www.twitch.tv/videos/{vod_id}"
            store.set_video(video_id=video_id, creator_id=1, url=url, title=vod_id, processed=False, status="indexing")
            store.set_state(
                vod_platform_id=vod_id, video_id=video_id, streamer="alice",
                last_ingested_seconds=60, last_seen_duration_seconds=120,
            )

        def lookup(vod_id, **kwargs):
            if vod_id == "failed":
                raise RuntimeError("Twitch request failed")
            return {"id": vod_id, "url": f"https://www.twitch.tv/videos/{vod_id}", "duration_seconds": 120}

        with patch.object(monitor, "get_archive_vod_by_id", side_effect=lookup):
            backlog = _build_backlog(
                twitch_monitor=monitor, videos=store, ingest_states=store, streamer="alice",
                user_id="user-1", days=1, skipped_vods_logged=set(), out=lambda _: None,
            )

        assert [(item.vod["id"], item.existing_state.last_ingested_seconds) for item in backlog] == [("available", 60)]
        assert store.get_video_status(1) == "indexing"

    def test_unavailable_vod_is_skipped_even_when_discovered_again(self) -> None:
        store = FakeStore()
        url = "https://www.twitch.tv/videos/2878694994"
        store.set_video(video_id=50, creator_id=1, url=url, title="Partial", processed=True, status="unavailable")
        store.set_state(
            vod_platform_id="2878694994", video_id=50, streamer="alice",
            last_ingested_seconds=22648, last_seen_duration_seconds=22768,
        )
        expected = store.vod_state["2878694994"]
        backlog = _build_backlog(
            twitch_monitor=FakeMonitor(vods=[{"id": "2878694994", "url": url}]),
            videos=store, ingest_states=store, streamer="alice", user_id="user-1",
            days=1, skipped_vods_logged=set(), out=lambda _: None,
        )
        assert backlog == []
        assert store.get_video_status(50) == "unavailable"
        assert store.vod_state["2878694994"] == expected

    def test_watch_mode_when_no_backlog_and_offline(self) -> None:
        monitor = FakeMonitor(live_sequence=[False, False, False], vods=[])
        store = FakeStore()
        logs: list[str] = []
        checks = {"count": 0}

        def should_stop() -> bool:
            checks["count"] += 1
            return checks["count"] > 2

        result = run_hybrid_ingest(
            "alice",
            monitor=monitor,
            build_storage=lambda: FakeRepositories(store),
            build_ingest=lambda: {"embedder": object()},
            historical_source_factory=FakeSource,
            live_source_factory=FakeSource,
            session_factory=HybridSessionFactory(),
            out=logs.append,
            should_stop=should_stop,
            watch_poll_seconds=0.0,
            backlog_live_poll_seconds=0.0,
            session_wait_seconds=0.0,
            retry_seconds=0.0,
        )

        assert result.watch_cycles >= 1
        assert FakeBacklogSession.runs == []
        assert FakeLiveSession.runs == 0
        assert any(line == "mode=watch streamer=alice backlog=0 is_live=false" for line in logs)

    def test_processes_newest_missing_backlog_first(self) -> None:
        monitor = FakeMonitor(
            live_sequence=[False, False],
            vods=[
                {
                    "id": "newest",
                    "url": "https://www.twitch.tv/videos/newest",
                    "created_at": "2026-09-12T12:00:00Z",
                },
                {
                    "id": "older",
                    "url": "https://www.twitch.tv/videos/older",
                    "created_at": "2026-09-10T12:00:00Z",
                },
            ],
        )
        store = FakeStore()
        logs: list[str] = []
        stop_flag = {"done": False}

        def should_stop() -> bool:
            return stop_flag["done"]

        def out(line: str) -> None:
            logs.append(line)
            if line == "completed mode=backlog vod=newest url=https://www.twitch.tv/videos/newest":
                stop_flag["done"] = True

        def historical_source_factory(**kwargs):
            assert kwargs['twitch_monitor'] is monitor
            return FakeSource(**kwargs)

        result = run_hybrid_ingest(
            "alice",
            monitor=monitor,
            build_storage=lambda: FakeRepositories(store),
            build_ingest=lambda: {"embedder": object()},
            historical_source_factory=historical_source_factory,
            live_source_factory=FakeSource,
            session_factory=HybridSessionFactory(),
            out=out,
            should_stop=should_stop,
            watch_poll_seconds=0.0,
            backlog_live_poll_seconds=0.0,
            session_wait_seconds=0.0,
            retry_seconds=0.0,
        )

        assert result.backlog_ingested == 1
        assert FakeBacklogSession.runs == ["newest"]
        assert any(line == "processing vod=newest chunk=0-30 progress=25.0% backlog=2" for line in logs)
        assert any(line == "completed vod=newest progress=100.0% backlog=2" for line in logs)
        assert any(line == "completed mode=backlog vod=newest url=https://www.twitch.tv/videos/newest" for line in logs)

    def test_skips_processed_vod_and_resumes_partial(self) -> None:
        monitor = FakeMonitor(
            live_sequence=[False, False],
            vods=[
                {"id": "resume", "url": "https://www.twitch.tv/videos/resume"},
                {"id": "processed", "url": "https://www.twitch.tv/videos/processed"},
            ],
        )
        store = FakeStore()
        store.set_video(
            video_id=2,
            creator_id=1,
            url="https://www.twitch.tv/videos/processed",
            title="done",
            processed=True,
            status=None,
        )
        store.set_state(
            vod_platform_id="resume",
            video_id=1,
            streamer="alice",
            last_ingested_seconds=90,
            last_seen_duration_seconds=120,
        )
        logs: list[str] = []
        stop_flag = {"done": False}

        def out(line: str) -> None:
            logs.append(line)
            if line == "completed mode=backlog vod=resume url=https://www.twitch.tv/videos/resume":
                stop_flag["done"] = True

        result = run_hybrid_ingest(
            "alice",
            monitor=monitor,
            build_storage=lambda: FakeRepositories(store),
            build_ingest=lambda: {"embedder": object()},
            historical_source_factory=FakeSource,
            live_source_factory=FakeSource,
            session_factory=HybridSessionFactory(),
            out=out,
            should_stop=lambda: stop_flag["done"],
            watch_poll_seconds=0.0,
            backlog_live_poll_seconds=0.0,
            session_wait_seconds=0.0,
            retry_seconds=0.0,
        )

        assert result.resumed == 1
        assert any(line == "skip processed vod=processed url=https://www.twitch.tv/videos/processed" for line in logs)
        assert any(line == "resume mode=backlog vod=resume cursor=90" for line in logs)
        assert any(line == "processing vod=resume chunk=90-120 progress=100.0% backlog=1" for line in logs)
        assert any(line == "completed vod=resume progress=100.0% backlog=1" for line in logs)

    def test_backlog_selection_uses_status_first_and_processed_fallback(self) -> None:
        monitor = FakeMonitor(
            vods=[
                {"id": "missing", "url": "https://www.twitch.tv/videos/missing"},
                {"id": "reindex", "url": "https://www.twitch.tv/videos/reindex"},
                {"id": "deleted", "url": "https://www.twitch.tv/videos/deleted"},
                {"id": "searchable", "url": "https://www.twitch.tv/videos/searchable"},
                {"id": "indexing", "url": "https://www.twitch.tv/videos/indexing"},
                {"id": "legacy-processed", "url": "https://www.twitch.tv/videos/legacy-processed"},
                {"id": "legacy-unprocessed", "url": "https://www.twitch.tv/videos/legacy-unprocessed"},
            ]
        )
        store = FakeStore()
        store.set_video(
            video_id=1,
            creator_id=1,
            url="https://www.twitch.tv/videos/reindex",
            title="Needs reindex",
            processed=True,
            status="reindex_requested",
        )
        store.set_state(
            vod_platform_id="reindex",
            video_id=1,
            streamer="alice",
            last_ingested_seconds=90,
            last_seen_duration_seconds=120,
        )
        store.set_video(
            video_id=2,
            creator_id=1,
            url="https://www.twitch.tv/videos/deleted",
            title="Deleted",
            processed=True,
            status="deleted",
        )
        store.set_video(
            video_id=3,
            creator_id=1,
            url="https://www.twitch.tv/videos/searchable",
            title="Searchable",
            processed=True,
            status="searchable",
        )
        store.set_video(
            video_id=4,
            creator_id=1,
            url="https://www.twitch.tv/videos/indexing",
            title="Indexing",
            processed=False,
            status="indexing",
        )
        store.set_video(
            video_id=5,
            creator_id=1,
            url="https://www.twitch.tv/videos/legacy-processed",
            title="Legacy processed",
            processed=True,
            status=None,
        )
        store.set_video(
            video_id=6,
            creator_id=1,
            url="https://www.twitch.tv/videos/legacy-unprocessed",
            title="Legacy unprocessed",
            processed=False,
            status=None,
        )
        logs: list[str] = []

        backlog = _build_backlog(
            twitch_monitor=monitor,
            videos=store,
            ingest_states=store,
            streamer="alice",
            user_id="user-1",
            days=30,
            skipped_vods_logged=set(),
            out=logs.append,
        )

        assert [candidate.vod["id"] for candidate in backlog] == [
            "missing",
            "reindex",
            "legacy-unprocessed",
        ]
        assert any(line == "skip deleted vod=deleted url=https://www.twitch.tv/videos/deleted" for line in logs)
        assert any(line == "skip searchable vod=searchable url=https://www.twitch.tv/videos/searchable" for line in logs)
        assert any(line == "skip indexing vod=indexing url=https://www.twitch.tv/videos/indexing" for line in logs)
        assert any(
            line == "skip processed vod=legacy-processed url=https://www.twitch.tv/videos/legacy-processed"
            for line in logs
        )

    def test_backlog_orders_eligible_vods_newest_first(self) -> None:
        monitor = FakeMonitor(
            vods=[
                {
                    "id": "older",
                    "url": "https://www.twitch.tv/videos/older",
                    "created_at": "2026-09-01T12:00:00Z",
                },
                {
                    "id": "newest",
                    "url": "https://www.twitch.tv/videos/newest",
                    "created_at": "2026-09-03T12:00:00Z",
                },
                {
                    "id": "already-searchable",
                    "url": "https://www.twitch.tv/videos/already-searchable",
                    "created_at": "2026-09-04T12:00:00Z",
                },
                {
                    "id": "middle",
                    "url": "https://www.twitch.tv/videos/middle",
                    "created_at": "2026-09-02T12:00:00Z",
                },
            ]
        )
        store = FakeStore()
        store.set_video(
            video_id=10,
            creator_id=1,
            url="https://www.twitch.tv/videos/already-searchable",
            title="Already searchable",
            processed=True,
            status="searchable",
        )

        backlog = _build_backlog(
            twitch_monitor=monitor,
            videos=store,
            ingest_states=store,
            streamer="alice",
            user_id="user-1",
            days=30,
            skipped_vods_logged=set(),
            out=lambda _: None,
        )

        assert [candidate.vod["id"] for candidate in backlog] == [
            "newest",
            "middle",
            "older",
        ]

    def test_reindex_requested_vod_does_not_resume_stale_state(self) -> None:
        monitor = FakeMonitor(
            live_sequence=[False, False],
            vods=[
                {"id": "reindex", "url": "https://www.twitch.tv/videos/reindex"},
            ],
        )
        store = FakeStore()
        store.set_video(
            video_id=7,
            creator_id=1,
            url="https://www.twitch.tv/videos/reindex",
            title="Needs reindex",
            processed=True,
            status="reindex_requested",
        )
        store.set_state(
            vod_platform_id="reindex",
            video_id=7,
            streamer="alice",
            last_ingested_seconds=90,
            last_seen_duration_seconds=120,
        )
        logs: list[str] = []
        stop_flag = {"done": False}

        def out(line: str) -> None:
            logs.append(line)
            if line == "completed mode=backlog vod=reindex url=https://www.twitch.tv/videos/reindex":
                stop_flag["done"] = True

        result = run_hybrid_ingest(
            "alice",
            monitor=monitor,
            build_storage=lambda: FakeRepositories(store),
            build_ingest=lambda: {"embedder": object()},
            historical_source_factory=FakeSource,
            live_source_factory=FakeSource,
            session_factory=HybridSessionFactory(),
            out=out,
            should_stop=lambda: stop_flag["done"],
            watch_poll_seconds=0.0,
            backlog_live_poll_seconds=0.0,
            session_wait_seconds=0.0,
            retry_seconds=0.0,
        )

        assert result.resumed == 0
        assert store.deleted_vod_state_ids == ["reindex"]
        assert not any(line.startswith("resume mode=backlog vod=reindex") for line in logs)
        assert any(
            line == "starting mode=backlog vod=reindex url=https://www.twitch.tv/videos/reindex cursor=0 backlog=1"
            for line in logs
        )

    def test_legacy_processed_vod_with_stale_state_still_skips_via_processed_fallback(self) -> None:
        monitor = FakeMonitor(
            vods=[
                {"id": "legacy-processed", "url": "https://www.twitch.tv/videos/legacy-processed"},
            ]
        )
        store = FakeStore()
        store.set_video(
            video_id=8,
            creator_id=1,
            url="https://www.twitch.tv/videos/legacy-processed",
            title="Legacy processed",
            processed=True,
            status=None,
        )
        store.set_state(
            vod_platform_id="legacy-processed",
            video_id=8,
            streamer="alice",
            last_ingested_seconds=90,
            last_seen_duration_seconds=120,
        )
        logs: list[str] = []

        backlog = _build_backlog(
            twitch_monitor=monitor,
            videos=store,
            ingest_states=store,
            streamer="alice",
            user_id="user-1",
            days=30,
            skipped_vods_logged=set(),
            out=logs.append,
        )

        assert backlog == []
        assert any(
            line == "skip processed vod=legacy-processed url=https://www.twitch.tv/videos/legacy-processed"
            for line in logs
        )

    def test_indexing_vod_with_saved_state_remains_resumable(self) -> None:
        monitor = FakeMonitor(
            vods=[
                {"id": "resume-indexing", "url": "https://www.twitch.tv/videos/resume-indexing"},
            ]
        )
        store = FakeStore()
        store.set_video(
            video_id=9,
            creator_id=1,
            url="https://www.twitch.tv/videos/resume-indexing",
            title="Resume indexing",
            processed=False,
            status="indexing",
        )
        store.set_state(
            vod_platform_id="resume-indexing",
            video_id=9,
            streamer="alice",
            last_ingested_seconds=45,
            last_seen_duration_seconds=120,
        )
        logs: list[str] = []

        backlog = _build_backlog(
            twitch_monitor=monitor,
            videos=store,
            ingest_states=store,
            streamer="alice",
            user_id="user-1",
            days=30,
            skipped_vods_logged=set(),
            out=logs.append,
        )

        assert [candidate.vod["id"] for candidate in backlog] == ["resume-indexing"]
        assert backlog[0].existing_state is store.vod_state["resume-indexing"]
        assert not any(line == "skip indexing vod=resume-indexing url=https://www.twitch.tv/videos/resume-indexing" for line in logs)

    def test_preempts_backlog_for_live_and_starts_live_session(self) -> None:
        monitor = FakeMonitor(
            live_sequence=[False, False, True, False],
            vods=[
                {"id": "vod-1", "url": "https://www.twitch.tv/videos/vod-1"},
            ],
        )
        store = FakeStore()
        logs: list[str] = []
        stop_flag = {"done": False}
        FakeBacklogSession.run_started_event = threading.Event()

        def out(line: str) -> None:
            logs.append(line)
            if line == "handoff event=live_to_backlog streamer=alice":
                stop_flag["done"] = True

        result = run_hybrid_ingest(
            "alice",
            monitor=monitor,
            build_storage=lambda: FakeRepositories(store),
            build_ingest=lambda: {"embedder": object()},
            historical_source_factory=FakeSource,
            live_source_factory=FakeSource,
            session_factory=HybridSessionFactory(),
            out=out,
            should_stop=lambda: stop_flag["done"],
            watch_poll_seconds=0.0,
            backlog_live_poll_seconds=0.0,
            session_wait_seconds=0.0,
            retry_seconds=0.0,
        )

        assert FakeBacklogSession.runs == ["vod-1"]
        assert FakeBacklogSession.stop_calls == ["vod-1"]
        assert FakeLiveSession.runs == 1
        assert result.handoffs_to_live == 1
        assert result.handoffs_to_backlog == 1
        assert store.vod_state["vod-1"].last_ingested_seconds == 60
        assert any(line == "processing vod=vod-1 chunk=0-30 progress=25.0% backlog=1" for line in logs)
        assert any(line == "handoff_requested event=backlog_to_live vod=vod-1" for line in logs)
        assert any(line == "handoff event=backlog_to_live streamer=alice vod=vod-1" for line in logs)
        assert not any(line == "completed vod=vod-1 progress=100.0% backlog=1" for line in logs)

    def test_should_stop_during_backlog_does_not_report_completion(self) -> None:
        monitor = FakeMonitor(
            live_sequence=[False, False],
            vods=[
                {"id": "vod-1", "url": "https://www.twitch.tv/videos/vod-1"},
            ],
        )
        store = FakeStore()
        logs: list[str] = []
        started = threading.Event()
        stopped = threading.Event()

        class BlockingBacklogSession(FakeBacklogSession):
            def run(self) -> None:
                started.set()
                assert stopped.wait(timeout=5.0)

            def stop(self) -> None:
                super().stop()
                stopped.set()

        result = run_hybrid_ingest(
            "alice",
            monitor=monitor,
            build_storage=lambda: FakeRepositories(store),
            build_ingest=lambda: {"embedder": object()},
            historical_source_factory=FakeSource,
            live_source_factory=FakeSource,
            session_factory=BlockingBacklogSession,
            out=logs.append,
            should_stop=started.is_set,
            watch_poll_seconds=0.0,
            backlog_live_poll_seconds=0.0,
            session_wait_seconds=0.0,
            retry_seconds=0.0,
        )
        assert stopped.is_set()
        assert FakeBacklogSession.stop_calls == ["vod-1"]
        assert result.backlog_ingested == 0
        assert result.handoffs_to_live == 0
        assert not any(line == "completed mode=backlog vod=vod-1 url=https://www.twitch.tv/videos/vod-1" for line in logs)

    def test_should_stop_during_live_does_not_report_handoff(self) -> None:
        monitor = FakeMonitor(live_sequence=[True, False], vods=[])
        store = FakeStore()
        logs: list[str] = []
        stop_flag = {"done": False}

        class BlockingLiveSession(FakeLiveSession):
            started = threading.Event()
            stopped = threading.Event()

            def run(self) -> None:
                BlockingLiveSession.started.set()
                assert BlockingLiveSession.stopped.wait(timeout=5.0)

            def stop(self) -> None:
                super().stop()
                BlockingLiveSession.stopped.set()

        class StopAwareFactory:
            def __call__(self, source, embedder, fingerprints, poll_interval):
                if getattr(source, "vod_metadata", None) is not None:
                    return FakeBacklogSession(source, embedder, fingerprints, poll_interval)
                return BlockingLiveSession(source, embedder, fingerprints, poll_interval)

        def should_stop() -> bool:
            return stop_flag["done"]

        def trigger_stop() -> None:
            BlockingLiveSession.started.wait(timeout=1.0)
            stop_flag["done"] = True

        stopper = threading.Thread(target=trigger_stop, daemon=True)
        stopper.start()
        result = run_hybrid_ingest(
            "alice",
            monitor=monitor,
            build_storage=lambda: FakeRepositories(store),
            build_ingest=lambda: {"embedder": object()},
            historical_source_factory=FakeSource,
            live_source_factory=FakeSource,
            session_factory=StopAwareFactory(),
            out=logs.append,
            should_stop=should_stop,
            watch_poll_seconds=0.0,
            backlog_live_poll_seconds=0.0,
            session_wait_seconds=0.0,
            retry_seconds=0.0,
        )
        stopper.join(timeout=1.0)

        assert FakeLiveSession.stop_calls == 1
        assert result.handoffs_to_backlog == 0
        assert not any(line == "handoff event=live_to_backlog streamer=alice" for line in logs)

    def test_shutdown_after_handoff_request_does_not_report_live_handoff(self) -> None:
        monitor = FakeMonitor(
            live_sequence=[False, True],
            vods=[
                {"id": "vod-1", "url": "https://www.twitch.tv/videos/vod-1"},
            ],
        )
        store = FakeStore()
        logs: list[str] = []
        stop_flag = {"done": False}

        def out(line: str) -> None:
            logs.append(line)
            if line == "handoff_requested event=backlog_to_live vod=vod-1":
                stop_flag["done"] = True

        result = run_hybrid_ingest(
            "alice",
            monitor=monitor,
            build_storage=lambda: FakeRepositories(store),
            build_ingest=lambda: {"embedder": object()},
            historical_source_factory=FakeSource,
            live_source_factory=FakeSource,
            session_factory=HybridSessionFactory(),
            out=out,
            should_stop=lambda: stop_flag["done"],
            watch_poll_seconds=0.0,
            backlog_live_poll_seconds=0.0,
            session_wait_seconds=0.0,
            retry_seconds=0.0,
        )

        assert result.handoffs_to_live == 0
        assert not any(line == "handoff event=backlog_to_live streamer=alice vod=vod-1" for line in logs)

    def test_completed_backlog_is_not_mislabeled_as_live_handoff(self) -> None:
        class SlowLiveCheckMonitor(FakeMonitor):
            def is_live(self, streamer: str) -> bool:
                time.sleep(0.03)
                return super().is_live(streamer)

        monitor = SlowLiveCheckMonitor(
            live_sequence=[False, True],
            vods=[
                {"id": "vod-1", "url": "https://www.twitch.tv/videos/vod-1"},
            ],
        )
        store = FakeStore()
        logs: list[str] = []
        stop_flag = {"done": False}

        def out(line: str) -> None:
            logs.append(line)
            if line == "completed mode=backlog vod=vod-1 url=https://www.twitch.tv/videos/vod-1":
                stop_flag["done"] = True

        result = run_hybrid_ingest(
            "alice",
            monitor=monitor,
            build_storage=lambda: FakeRepositories(store),
            build_ingest=lambda: {"embedder": object()},
            historical_source_factory=FakeSource,
            live_source_factory=FakeSource,
            session_factory=HybridSessionFactory(),
            out=out,
            should_stop=lambda: stop_flag["done"],
            watch_poll_seconds=0.0,
            backlog_live_poll_seconds=0.0,
            session_wait_seconds=0.0,
            retry_seconds=0.0,
        )

        assert result.backlog_ingested == 1
        assert result.handoffs_to_live == 0
        assert any(line == "completed mode=backlog vod=vod-1 url=https://www.twitch.tv/videos/vod-1" for line in logs)

    def test_live_failure_retries_without_terminating(self) -> None:
        class FailingLiveSession(FakeLiveSession):
            def run(self) -> None:
                FakeLiveSession.runs += 1
                raise RuntimeError("live-boom")

        class FailureAwareFactory:
            def __call__(self, source, embedder, fingerprints, poll_interval):
                if getattr(source, "vod_metadata", None) is not None:
                    return FakeBacklogSession(source, embedder, fingerprints, poll_interval)
                return FailingLiveSession(source, embedder, fingerprints, poll_interval)

        monitor = FakeMonitor(live_sequence=[True, False, False], vods=[])
        store = FakeStore()
        logs: list[str] = []
        checks = {"count": 0}

        def should_stop() -> bool:
            checks["count"] += 1
            return checks["count"] > 3

        result = run_hybrid_ingest(
            "alice",
            monitor=monitor,
            build_storage=lambda: FakeRepositories(store),
            build_ingest=lambda: {"embedder": object()},
            historical_source_factory=FakeSource,
            live_source_factory=FakeSource,
            session_factory=FailureAwareFactory(),
            out=logs.append,
            should_stop=should_stop,
            watch_poll_seconds=0.0,
            backlog_live_poll_seconds=0.0,
            session_wait_seconds=0.0,
            retry_seconds=0.0,
        )

        assert result.failed == 1
        assert any("event=controller_retry streamer=alice phase=live_session error=RuntimeError" in line for line in logs)

    def test_multi_streamer_worker_loads_one_model_and_uses_priority_clients(self) -> None:
        class FakeEmbedder:
            model_version = "model"
            preprocessing_version = "preprocess"
            embedding_dim = 128
            is_loaded = False

            def __init__(self) -> None:
                self.load_calls = 0

            def load(self) -> int:
                self.load_calls += 1
                self.is_loaded = True
                return 123

            def extract(self, audio_path: str, *, offset_seconds: float = 0.0):
                raise AssertionError("controllers should not run inference in this test")

        embedder = FakeEmbedder()
        repositories = FakeRepositories(FakeStore())
        monitor = FakeMonitor()
        seen: dict[str, dict[str, object]] = {}

        def controller(streamer: str, days: int, **kwargs) -> HybridIngestResult:
            state = kwargs["build_ingest"]()
            assert kwargs["build_storage"]() is repositories
            assert kwargs["provision_streamer"] is True
            seen[streamer] = state
            return HybridIngestResult(watch_cycles=1)

        result = run_multi_streamer_ingest(
            ["JasonTheWeen", "stableronaldo"],
            monitor=monitor,  # type: ignore[arg-type]
            build_storage=lambda: repositories,  # type: ignore[arg-type]
            build_ingest=lambda: {"embedder": embedder},
            controller=controller,
            out=lambda _: None,
            should_stop=lambda: len(seen) == 2,
        )

        assert result.streamers == ("jasontheween", "stableronaldo")
        assert set(result.results) == {"jasontheween", "stableronaldo"}
        assert embedder.load_calls == 1
        schedulers = {
            state["live_embedder"].scheduler  # type: ignore[union-attr]
            for state in seen.values()
        }
        assert len(schedulers) == 1
        for state in seen.values():
            assert state["embedder"] is embedder
            assert state["live_embedder"].priority == LIVE_PRIORITY  # type: ignore[union-attr]
            assert state["backlog_embedder"].priority == BACKLOG_PRIORITY  # type: ignore[union-attr]

    def test_archive_dns_failure_recovers_while_other_streamer_keeps_polling(self) -> None:
        class Embedder:
            load_calls = 0

            def load(self):
                self.load_calls += 1
                return 0

        monitor = TwitchMonitor("client", "secret")
        monitor._access_token = "token"
        archives = {"jasontheween": 0, "stableronaldo": 0}
        logs = []
        healthy_polled = threading.Event()
        deadline = time.monotonic() + 1.0

        def request(req, **kwargs):
            parsed = urllib.parse.urlparse(req.full_url)
            query = urllib.parse.parse_qs(parsed.query)
            if parsed.path.endswith("/users"):
                streamer = query["login"][0]
                payload = {"data": [{"id": streamer, "login": streamer}]}
            elif parsed.path.endswith("/streams"):
                payload = {"data": []}
            else:
                streamer = query["user_id"][0]
                archives[streamer] += 1
                if streamer == "jasontheween" and archives[streamer] == 1:
                    assert healthy_polled.wait(0.5)
                    raise urllib.error.URLError(socket.gaierror(-3, "Temporary failure in name resolution"))
                if streamer == "stableronaldo":
                    healthy_polled.set()
                payload = {"data": []}
            return io.BytesIO(json.dumps(payload).encode())

        def controller(streamer, days, **kwargs):
            return run_hybrid_ingest(
                streamer, days, **kwargs,
                watch_poll_seconds=0.001, retry_seconds=0.001,
            )

        embedder = Embedder()
        with patch("urllib.request.urlopen", side_effect=request):
            run_multi_streamer_ingest(
                ["jasontheween", "stableronaldo"], monitor=monitor,
                build_storage=lambda: FakeRepositories(FakeStore()),
                build_ingest=lambda: {"embedder": embedder},
                controller=controller, out=logs.append,
                should_stop=lambda: (
                    archives["jasontheween"] >= 2 and archives["stableronaldo"] >= 2
                ) or time.monotonic() >= deadline,
            )

        assert archives["jasontheween"] >= 2, archives
        assert archives["stableronaldo"] >= 2, archives
        assert embedder.load_calls == 1

    def test_archive_outage_backs_off_to_cap_and_resets_after_discovery_succeeds(self) -> None:
        monitor = FakeMonitor()
        attempts = []
        delays = []
        logs = []

        def archives(**kwargs):
            attempts.append(len(attempts) + 1)
            if len(attempts) not in {8, 10}:
                raise twitch_failure()
            return []

        with patch.object(monitor, "list_archive_vods_since", side_effect=archives), patch(
            "runners.run_hybrid_ingest._sleep_interruptibly", side_effect=lambda delay, stop: delays.append(delay),
        ), patch("runners.run_hybrid_ingest.random.uniform", side_effect=lambda low, high: high):
            result = run_hybrid_ingest(
                "alice", monitor=monitor, out=logs.append,
                build_storage=lambda: FakeRepositories(FakeStore()),
                build_ingest=lambda: {"embedder": object()},
                should_stop=lambda: len(attempts) >= 10, watch_poll_seconds=0,
            )

        assert [delay for delay in delays if delay] == [5, 10, 20, 40, 60, 60, 60, 5]
        assert result.watch_cycles == 2
        assert sum("event=controller_recovered" in line for line in logs) == 2
        assert any("degraded=true" in line for line in logs)

    def test_rate_limit_wait_honors_reset_and_permanent_auth_is_not_retried(self) -> None:
        monitor = FakeMonitor()
        stop = threading.Event()
        delays = []

        def wait(delay, should_stop):
            delays.append(delay)
            stop.set()

        with patch.object(monitor, "list_archive_vods_since", side_effect=twitch_failure(retry_at=175, status=429)), patch(
            "runners.run_hybrid_ingest.time.time", return_value=100,
        ), patch("runners.run_hybrid_ingest._sleep_interruptibly", side_effect=wait):
            run_hybrid_ingest(
                "alice", monitor=monitor, out=lambda line: None,
                build_storage=lambda: FakeRepositories(FakeStore()),
                build_ingest=lambda: {"embedder": object()}, should_stop=stop.is_set,
            )
        assert delays == [75]

        with patch.object(monitor, "list_archive_vods_since", side_effect=twitch_failure(retryable=False, status=403)) as lookup, patch(
            "runners.run_hybrid_ingest._sleep_interruptibly",
        ) as wait:
            with pytest.raises(TwitchRequestError):
                run_hybrid_ingest(
                    "alice", monitor=monitor, out=lambda line: None,
                    build_storage=lambda: FakeRepositories(FakeStore()),
                    build_ingest=lambda: {"embedder": object()},
                )
        assert lookup.call_count == 1
        wait.assert_not_called()

    def test_shutdown_interrupts_retry_wait_without_another_twitch_request(self) -> None:
        monitor = FakeMonitor()
        stop = threading.Event()
        retrying = threading.Event()

        def stop_during_retry():
            assert retrying.wait(1)
            time.sleep(0.02)
            stop.set()

        stopper = threading.Thread(target=stop_during_retry)
        stopper.start()
        started_at = time.monotonic()
        with patch.object(monitor, "list_archive_vods_since", side_effect=twitch_failure()) as lookup:
            run_hybrid_ingest(
                "alice", monitor=monitor,
                out=lambda line: retrying.set() if "event=controller_retry" in line else None,
                build_storage=lambda: FakeRepositories(FakeStore()),
                build_ingest=lambda: {"embedder": object()},
                should_stop=stop.is_set, retry_seconds=60,
            )
        stopper.join()
        assert time.monotonic() - started_at < 1
        assert lookup.call_count == 1

    def test_profile_outage_does_not_block_other_streamer_provisioning_or_session(self) -> None:
        healthy_started = threading.Event()
        recovered_started = threading.Event()
        provisioned = set()
        profile_calls = {"jasontheween": 0, "stableronaldo": 0}
        active = {"jasontheween": 0, "stableronaldo": 0}
        peak = dict(active)
        logs = []

        class Monitor:
            def get_user_profile(self, streamer, force_refresh=False):
                profile_calls[streamer] += 1
                if streamer == "jasontheween" and profile_calls[streamer] == 1:
                    assert healthy_started.wait(1)
                    raise twitch_failure()
                return {"id": streamer}

            def get_live_streamers(self, streamers):
                return set(streamers)

        class Store(FakeStore):
            def create_or_get_creator(self, streamer, *args, **kwargs):
                return streamer

        class Partitions:
            def ensure_creator_partition(self, creator_id):
                provisioned.add(creator_id)
                return creator_id

        repositories = FakeRepositories(Store())
        repositories.embedding_partitions = Partitions()

        class Session:
            def __init__(self, source, **kwargs):
                self.streamer = source.kwargs["streamer"]
                self.stop_event = threading.Event()

            def run(self):
                assert self.streamer in provisioned
                active[self.streamer] += 1
                peak[self.streamer] = max(peak[self.streamer], active[self.streamer])
                if self.streamer == "stableronaldo":
                    healthy_started.set()
                else:
                    recovered_started.set()
                try:
                    assert self.stop_event.wait(2)
                finally:
                    active[self.streamer] -= 1

            def stop(self):
                self.stop_event.set()

        def controller(streamer, days, **kwargs):
            return run_hybrid_ingest(
                streamer, days, **kwargs, live_source_factory=FakeSource,
                session_factory=Session, retry_seconds=0.001, session_wait_seconds=0.001,
            )

        deadline = time.monotonic() + 2
        embedder = WorkerEmbedder()
        run_multi_streamer_ingest(
            ["jasontheween", "stableronaldo"], monitor=Monitor(),
            build_storage=lambda: repositories, build_ingest=lambda: {"embedder": embedder},
            controller=controller, out=logs.append,
            should_stop=lambda: recovered_started.is_set() or time.monotonic() >= deadline,
        )
        assert healthy_started.is_set() and recovered_started.is_set()
        assert profile_calls == {"jasontheween": 2, "stableronaldo": 1}
        assert peak == {"jasontheween": 1, "stableronaldo": 1}
        assert active == {"jasontheween": 0, "stableronaldo": 0}
        assert embedder.load_calls == 1
        assert any("event=controller_recovered streamer=jasontheween phase=provision" in line for line in logs)

    @pytest.mark.parametrize("exit_kind", ["exception", "return"])
    def test_worker_restarts_only_the_unexpected_controller_exit(self, exit_kind) -> None:
        calls = {"jasontheween": 0, "stableronaldo": 0}
        active = dict(calls)
        peak = dict(calls)
        healthy_running = threading.Event()
        recovered = threading.Event()
        logs = []

        def controller(streamer, days, **kwargs):
            calls[streamer] += 1
            active[streamer] += 1
            peak[streamer] = max(peak[streamer], active[streamer])
            try:
                if streamer == "stableronaldo":
                    healthy_running.set()
                    assert recovered.wait(1)
                elif calls[streamer] == 1:
                    assert healthy_running.wait(1)
                    if exit_kind == "exception":
                        raise RuntimeError("unexpected failure with private details")
                else:
                    recovered.set()
                return HybridIngestResult()
            finally:
                active[streamer] -= 1

        embedder = WorkerEmbedder()
        with patch("runners.run_hybrid_ingest.MONITOR_RETRY_SECONDS", 0.001):
            run_multi_streamer_ingest(
                ["jasontheween", "stableronaldo"], monitor=FakeMonitor(),
                build_storage=lambda: FakeRepositories(FakeStore()),
                build_ingest=lambda: {"embedder": embedder},
                controller=controller, out=logs.append, should_stop=recovered.is_set,
            )
        assert calls == {"jasontheween": 2, "stableronaldo": 1}
        assert peak == {"jasontheween": 1, "stableronaldo": 1}
        assert active == {"jasontheween": 0, "stableronaldo": 0}
        assert embedder.load_calls == 1
        assert any("event=controller_retry streamer=jasontheween phase=controller error=RuntimeError" in line for line in logs)
        assert not any("private details" in line for line in logs)

    def test_permanent_controller_auth_failure_is_visible_without_stopping_peer(self) -> None:
        healthy_ran = threading.Event()
        failure_logged = threading.Event()
        calls = {"jasontheween": 0, "stableronaldo": 0}
        logs = []

        def controller(streamer, days, **kwargs):
            calls[streamer] += 1
            if streamer == "jasontheween":
                raise twitch_failure(retryable=False, status=401)
            healthy_ran.set()
            assert failure_logged.wait(1)
            return HybridIngestResult()

        def out(line):
            logs.append(line)
            if "event=controller_failed streamer=jasontheween" in line:
                failure_logged.set()

        run_multi_streamer_ingest(
            ["jasontheween", "stableronaldo"], monitor=FakeMonitor(),
            build_storage=lambda: FakeRepositories(FakeStore()),
            build_ingest=lambda: {"embedder": WorkerEmbedder()},
            controller=controller, out=out,
            should_stop=lambda: healthy_ran.is_set() and failure_logged.is_set(),
        )
        assert calls == {"jasontheween": 1, "stableronaldo": 1}
        assert any("permanent=true" in line and "error=" in line for line in logs)

    def test_offline_successful_poll_heartbeat_has_fixed_cadence(self) -> None:
        now = [0]
        polls = []
        logs = []
        monitor = FakeMonitor()

        def archives(**kwargs):
            polls.append(now[0])
            return []

        with patch.object(monitor, "list_archive_vods_since", side_effect=archives), patch(
            "runners.run_hybrid_ingest.time.monotonic", side_effect=lambda: now[0],
        ), patch("runners.run_hybrid_ingest._sleep_interruptibly", side_effect=lambda delay, stop: now.__setitem__(0, now[0] + delay)):
            run_hybrid_ingest(
                "alice", monitor=monitor, out=logs.append,
                build_storage=lambda: FakeRepositories(FakeStore()),
                build_ingest=lambda: {"embedder": object()},
                should_stop=lambda: len(polls) >= 6, watch_poll_seconds=30,
            )
        assert polls == [0, 30, 60, 90, 120, 150]
        assert sum("event=controller_poll streamer=alice phase=watch" in line for line in logs) == 3

    @pytest.mark.parametrize("request_error", [True, False])
    def test_live_poll_recovers_request_failure_but_does_not_clear_media_failure(self, request_error) -> None:
        observed_poll = threading.Event()
        stop = threading.Event()
        logs = []
        runs = []
        delays = []

        class Source(FakeSource):
            last_successful_poll_at = None
            ingest_cursor_seconds = 0

        class Session:
            def __init__(self, source, **kwargs):
                self.source = source

            def run(self):
                runs.append(self)
                if len(runs) == 2:
                    self.source.last_successful_poll_at = 1.0
                    assert observed_poll.wait(1)
                raise twitch_failure() if request_error else RuntimeError("media failed")

            def stop(self):
                observed_poll.set()

        def out(line):
            logs.append(line)
            if "event=controller_poll" in line:
                observed_poll.set()
            if sum("event=controller_retry" in logged for logged in logs) >= 2:
                stop.set()

        def wait(delay, should_stop):
            if delay > 1:
                delays.append(delay)
            else:
                time.sleep(0.001)

        with patch("runners.run_hybrid_ingest._sleep_interruptibly", side_effect=wait), patch(
            "runners.run_hybrid_ingest.random.uniform", side_effect=lambda low, high: high,
        ):
            run_hybrid_ingest(
                "alice", monitor=FakeMonitor(live_sequence=[True]), out=out,
                build_storage=lambda: FakeRepositories(FakeStore()),
                build_ingest=lambda: {"embedder": object()},
                live_source_factory=Source, session_factory=Session,
                should_stop=stop.is_set, session_wait_seconds=0.001,
            )
        assert delays == ([5, 5] if request_error else [5, 10])
        assert any("event=controller_recovered" in line for line in logs) is request_error
        assert not any("event=controller_progress" in line for line in logs)

    def test_backlog_committed_progress_resets_retry_on_controller_thread(self) -> None:
        recovered = threading.Event()
        stop = threading.Event()
        logs = []
        runs = []
        owner_thread = threading.get_ident()
        delays = []

        class Source(FakeSource):
            ingest_cursor_seconds = 0

        class Session:
            def __init__(self, source, **kwargs):
                self.source = source

            def run(self):
                runs.append(self)
                if len(runs) == 2:
                    self.source.ingest_cursor_seconds = 60
                    assert recovered.wait(1)
                raise twitch_failure()

            def stop(self):
                recovered.set()

        def out(line):
            logs.append(line)
            if "event=controller_recovered" in line:
                assert threading.get_ident() == owner_thread
                recovered.set()
            if sum("event=controller_retry" in logged for logged in logs) >= 2:
                stop.set()

        def wait(delay, should_stop):
            if delay > 1:
                delays.append(delay)
            else:
                time.sleep(0.001)

        with patch("runners.run_hybrid_ingest._sleep_interruptibly", side_effect=wait), patch(
            "runners.run_hybrid_ingest.random.uniform", side_effect=lambda low, high: high,
        ):
            run_hybrid_ingest(
                "alice", monitor=FakeMonitor(vods=[{"id": "vod", "url": "https://www.twitch.tv/videos/vod"}]),
                out=out, build_storage=lambda: FakeRepositories(FakeStore()),
                build_ingest=lambda: {"embedder": object()},
                historical_source_factory=Source, session_factory=Session,
                should_stop=stop.is_set, backlog_live_poll_seconds=0.001,
            )
        assert delays == [5, 5]
        assert any("event=controller_recovered streamer=alice phase=backlog_session" in line for line in logs)

    def test_live_poll_recovers_archive_discovery_outage_when_streamer_goes_live(self) -> None:
        observed_recovery = threading.Event()
        stop = threading.Event()
        logs = []

        class Source(FakeSource):
            last_successful_poll_at = None
            ingest_cursor_seconds = 0

        class Session:
            def __init__(self, source, **kwargs):
                self.source = source

            def run(self):
                self.source.last_successful_poll_at = 1.0
                assert observed_recovery.wait(1)
                stop.set()

            def stop(self):
                observed_recovery.set()

        def out(line):
            logs.append(line)
            if "event=controller_recovered streamer=alice phase=archive_discovery" in line:
                observed_recovery.set()

        monitor = FakeMonitor(live_sequence=[False, True])
        with patch.object(monitor, "list_archive_vods_since", side_effect=twitch_failure()):
            run_hybrid_ingest(
                "alice", monitor=monitor, out=out,
                build_storage=lambda: FakeRepositories(FakeStore()),
                build_ingest=lambda: {"embedder": object()},
                live_source_factory=Source, session_factory=Session,
                should_stop=stop.is_set, retry_seconds=0.001, session_wait_seconds=0.001,
            )
        assert observed_recovery.is_set()
        assert not any("event=controller_progress" in line for line in logs)

    def test_unexpected_backlog_poll_failure_stops_and_joins_existing_session(self) -> None:
        started = threading.Event()
        finished = threading.Event()
        stop = threading.Event()

        class Session:
            def __init__(self, source, **kwargs):
                pass

            def run(self):
                started.set()
                assert stop.wait(1)
                finished.set()

            def stop(self):
                stop.set()

        monitor = FakeMonitor(vods=[{"id": "vod", "url": "https://www.twitch.tv/videos/vod"}])

        def is_live(streamer):
            if not started.is_set():
                return False
            raise RuntimeError("unexpected controller poll failure")

        with patch.object(monitor, "is_live", side_effect=is_live):
            with pytest.raises(RuntimeError, match="unexpected controller poll failure"):
                run_hybrid_ingest(
                    "alice", monitor=monitor, out=lambda line: None,
                    build_storage=lambda: FakeRepositories(FakeStore()),
                    build_ingest=lambda: {"embedder": object()},
                    historical_source_factory=FakeSource, session_factory=Session,
                    backlog_live_poll_seconds=0.001,
                )
        assert stop.is_set() and finished.is_set()

    def test_transient_preemption_poll_failure_keeps_backlog_session_processing(self) -> None:
        failed_poll = threading.Event()
        committed = threading.Event()
        completed = threading.Event()
        stops = []
        logs = []
        monitor = FakeMonitor(vods=[{"id": "vod", "url": "https://www.twitch.tv/videos/vod"}])

        class Session:
            def __init__(self, source, **kwargs):
                pass

            def run(self):
                assert failed_poll.wait(1)
                committed.set()

            def stop(self):
                stops.append(True)
                failed_poll.set()

        checks = []

        def is_live(streamer):
            checks.append(True)
            if len(checks) == 1:
                return False
            if not committed.is_set():
                raise twitch_failure()
            return False

        def out(line):
            logs.append(line)
            if "phase=backlog_live_check" in line:
                failed_poll.set()
            if line.startswith("completed mode=backlog"):
                completed.set()

        with patch.object(monitor, "is_live", side_effect=is_live):
            result = run_hybrid_ingest(
                "alice", monitor=monitor, out=out,
                build_storage=lambda: FakeRepositories(FakeStore()),
                build_ingest=lambda: {"embedder": object()},
                historical_source_factory=FakeSource, session_factory=Session,
                should_stop=completed.is_set, backlog_live_poll_seconds=0.001,
            )
        assert committed.is_set() and completed.is_set()
        assert result.backlog_ingested == 1
        assert stops == []
        assert any("phase=backlog_live_check error=TimeoutError" in line for line in logs)

    @pytest.mark.parametrize("retryable", [True, False])
    def test_saved_cursor_lookup_propagates_twitch_failures_without_marking_absent(self, retryable) -> None:
        store = FakeStore()
        store.set_video(video_id=1, creator_id=1, url="https://www.twitch.tv/videos/saved", title="saved", processed=False, status="indexing")
        store.set_state(vod_platform_id="saved", video_id=1, streamer="alice", last_ingested_seconds=60, last_seen_duration_seconds=120)
        monitor = FakeMonitor()
        with patch.object(monitor, "get_archive_vod_by_id", side_effect=twitch_failure(retryable=retryable)):
            with pytest.raises(TwitchRequestError):
                _build_backlog(
                    twitch_monitor=monitor, videos=store, ingest_states=store,
                    streamer="alice", user_id="user-1", days=1,
                    skipped_vods_logged=set(), out=lambda line: None,
                )
        assert store.get_video_status(1) == "indexing"
        assert store.vod_state["saved"].last_ingested_seconds == 60

    def test_normalize_streamers_accepts_repeated_and_csv_values(self) -> None:
        assert _normalize_streamers(
            ["JasonTheWeen", "stableronaldo, jasontheween"]
        ) == ["jasontheween", "stableronaldo"]

    def test_main_returns_zero(self) -> None:
        import runners.run_hybrid_ingest as module

        original = module.run_hybrid_ingest
        try:
            module.run_hybrid_ingest = lambda streamer, days: None
            assert main(["--streamer", "alice", "--days", "30"]) == 0
        finally:
            module.run_hybrid_ingest = original

    def test_main_uses_code_owned_default_streamer_roster(self) -> None:
        import runners.run_hybrid_ingest as module

        calls: list[tuple[list[str], int]] = []
        original = module.run_multi_streamer_ingest
        try:
            module.run_multi_streamer_ingest = lambda streamers, days: calls.append(
                (streamers, days)
            )
            assert main(["--days", "30"]) == 0
        finally:
            module.run_multi_streamer_ingest = original

        assert calls == [(["jasontheween", "stableronaldo"], 30)]
