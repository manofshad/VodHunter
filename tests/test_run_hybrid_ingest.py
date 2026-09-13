import threading
import time
from datetime import datetime, timezone

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


class FakeMonitor:
    def __init__(
        self,
        *,
        live_sequence: list[bool] | None = None,
        vods: list[dict[str, object]] | None = None,
    ):
        self.live_sequence = list(live_sequence or [False])
        self.vods = list(vods or [])
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

    def test_processes_oldest_missing_backlog_first(self) -> None:
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
            if line == "completed mode=backlog vod=older url=https://www.twitch.tv/videos/older":
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
            should_stop=should_stop,
            watch_poll_seconds=0.0,
            backlog_live_poll_seconds=0.0,
            session_wait_seconds=0.0,
            retry_seconds=0.0,
        )

        assert result.backlog_ingested == 1
        assert FakeBacklogSession.runs == ["older"]
        assert any(line == "processing vod=older chunk=0-30 progress=25.0% backlog=2" for line in logs)
        assert any(line == "completed vod=older progress=100.0% backlog=2" for line in logs)
        assert any(line == "completed mode=backlog vod=older url=https://www.twitch.tv/videos/older" for line in logs)

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
        stop_flag = {"done": False}
        FakeBacklogSession.run_started_event = threading.Event()

        def should_stop() -> bool:
            return stop_flag["done"]

        def trigger_stop() -> None:
            assert FakeBacklogSession.run_started_event is not None
            FakeBacklogSession.run_started_event.wait(timeout=1.0)
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
            session_factory=HybridSessionFactory(),
            out=logs.append,
            should_stop=should_stop,
            watch_poll_seconds=0.0,
            backlog_live_poll_seconds=0.0,
            session_wait_seconds=0.0,
            retry_seconds=0.0,
        )
        stopper.join(timeout=1.0)

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

            def run(self) -> None:
                BlockingLiveSession.started.set()
                while not stop_flag["done"]:
                    time.sleep(0.01)

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
        assert any(line == "failed mode=live streamer=alice error=live-boom" for line in logs)

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
            assert kwargs["provision_streamer"] is False
            seen[streamer] = state
            return HybridIngestResult(watch_cycles=1)

        result = run_multi_streamer_ingest(
            ["JasonTheWeen", "stableronaldo"],
            monitor=monitor,  # type: ignore[arg-type]
            build_storage=lambda: repositories,  # type: ignore[arg-type]
            build_ingest=lambda: {"embedder": embedder},
            controller=controller,
            out=lambda _: None,
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
