import threading
import time

from runners.run_hybrid_ingest import main, run_hybrid_ingest


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
        self.videos_by_url: dict[str, tuple[int, int, str, str, str | None, bool]] = {}
        self.vod_state: dict[str, dict[str, object]] = {}

    def get_video_by_url(self, url: str):
        return self.videos_by_url.get(url)

    def get_vod_ingest_state(self, vod_platform_id: str):
        return self.vod_state.get(vod_platform_id)


class FakeSource:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.vod_metadata = kwargs.get("vod_metadata")


class FakeBacklogSession:
    runs: list[str] = []
    stop_calls: list[str] = []
    raise_error_for: set[str] = set()
    run_started_event: threading.Event | None = None

    def __init__(self, source, embedder, store, poll_interval):
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
            self.source.kwargs["store"].videos_by_url[str(self.source.vod_metadata["url"])] = (
                1,
                1,
                str(self.source.vod_metadata["url"]),
                "done",
                None,
                True,
            )
            self.source.kwargs["store"].vod_state.pop(self.vod_id, None)

    def stop(self) -> None:
        self.stopped = True
        FakeBacklogSession.stop_calls.append(self.vod_id)
        self.source.kwargs["store"].vod_state[self.vod_id] = {
            "vod_platform_id": self.vod_id,
            "video_id": 1,
            "streamer": "alice",
            "last_ingested_seconds": 60,
            "last_seen_duration_seconds": 120,
        }


class FakeLiveSession:
    runs = 0
    stop_calls = 0

    def __init__(self, source, embedder, store, poll_interval):
        self.source = source

    def run(self) -> None:
        FakeLiveSession.runs += 1
        time.sleep(0.01)

    def stop(self) -> None:
        FakeLiveSession.stop_calls += 1


class HybridSessionFactory:
    def __call__(self, source, embedder, store, poll_interval):
        if getattr(source, "vod_metadata", None) is not None:
            return FakeBacklogSession(source, embedder, store, poll_interval)
        return FakeLiveSession(source, embedder, store, poll_interval)


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
            build_store=lambda: {"store": store},
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
                {"id": "newest", "url": "https://www.twitch.tv/videos/newest"},
                {"id": "older", "url": "https://www.twitch.tv/videos/older"},
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

        result = run_hybrid_ingest(
            "alice",
            monitor=monitor,
            build_store=lambda: {"store": store},
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
        store.videos_by_url["https://www.twitch.tv/videos/processed"] = (
            2,
            1,
            "https://www.twitch.tv/videos/processed",
            "done",
            None,
            True,
        )
        store.vod_state["resume"] = {
            "vod_platform_id": "resume",
            "video_id": 1,
            "streamer": "alice",
            "last_ingested_seconds": 90,
            "last_seen_duration_seconds": 120,
        }
        logs: list[str] = []
        stop_flag = {"done": False}

        def out(line: str) -> None:
            logs.append(line)
            if line == "completed mode=backlog vod=resume url=https://www.twitch.tv/videos/resume":
                stop_flag["done"] = True

        result = run_hybrid_ingest(
            "alice",
            monitor=monitor,
            build_store=lambda: {"store": store},
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
            build_store=lambda: {"store": store},
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
        assert store.vod_state["vod-1"]["last_ingested_seconds"] == 60
        assert any(line == "processing vod=vod-1 chunk=0-30 progress=25.0% backlog=1" for line in logs)
        assert any(line == "handoff_requested event=backlog_to_live vod=vod-1" for line in logs)
        assert any(line == "handoff event=backlog_to_live streamer=alice vod=vod-1" for line in logs)
        assert not any(line == "completed vod=vod-1 progress=100.0% backlog=1" for line in logs)

    def test_live_failure_retries_without_terminating(self) -> None:
        class FailingLiveSession(FakeLiveSession):
            def run(self) -> None:
                FakeLiveSession.runs += 1
                raise RuntimeError("live-boom")

        class FailureAwareFactory:
            def __call__(self, source, embedder, store, poll_interval):
                if getattr(source, "vod_metadata", None) is not None:
                    return FakeBacklogSession(source, embedder, store, poll_interval)
                return FailingLiveSession(source, embedder, store, poll_interval)

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
            build_store=lambda: {"store": store},
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

    def test_main_returns_zero(self) -> None:
        import runners.run_hybrid_ingest as module

        original = module.run_hybrid_ingest
        try:
            module.run_hybrid_ingest = lambda streamer, days: None
            assert main(["--streamer", "alice", "--days", "30"]) == 0
        finally:
            module.run_hybrid_ingest = original
