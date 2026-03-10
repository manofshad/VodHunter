import unittest
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from runners.run_backfill_ingest import main, run_backfill_ingest


class FakeMonitor:
    def __init__(self, vods):
        self.vods = list(vods)

    def get_user_id(self, streamer: str) -> str:
        return "user-1"

    def list_archive_vods_since(self, user_id: str, created_after):
        return list(self.vods)


class FakeStore:
    def __init__(self):
        self.videos_by_url: dict[str, tuple[int, int, str, str, str | None, bool]] = {}
        self.vod_state: dict[str, dict] = {}

    def get_video_by_url(self, url: str):
        return self.videos_by_url.get(url)

    def get_vod_ingest_state(self, vod_platform_id: str):
        return self.vod_state.get(vod_platform_id)


class FakeSource:
    def __init__(self, streamer, vod_metadata, store, chunk_seconds, temp_dir, progress_callback=None):
        self.streamer = streamer
        self.vod_metadata = vod_metadata
        self.store = store
        self.chunk_seconds = chunk_seconds
        self.temp_dir = temp_dir
        self.progress_callback = progress_callback


class FakeSession:
    def __init__(self, source, embedder, store, poll_interval):
        self.source = source

    def run(self) -> None:
        if self.source.vod_metadata.get("should_fail"):
            raise RuntimeError("boom")


class TestRunBackfillIngest(unittest.TestCase):
    def _build_state(self, store: FakeStore):
        return {"store": store, "embedder": object()}

    def test_skips_processed_resumes_partial_and_continues_on_failure(self) -> None:
        store = FakeStore()
        store.videos_by_url["https://www.twitch.tv/videos/processed"] = (
            1,
            1,
            "https://www.twitch.tv/videos/processed",
            "Processed",
            None,
            True,
        )
        store.vod_state["resume"] = {
            "vod_platform_id": "resume",
            "video_id": 2,
            "streamer": "alice",
            "last_ingested_seconds": 60,
            "last_seen_duration_seconds": 120,
            "updated_at": "now",
        }

        monitor = FakeMonitor(
            [
                {"id": "resume", "url": "https://www.twitch.tv/videos/resume"},
                {"id": "processed", "url": "https://www.twitch.tv/videos/processed"},
                {"id": "fail", "url": "https://www.twitch.tv/videos/fail", "should_fail": True},
            ]
        )
        logs: list[str] = []
        seen_vods: list[str] = []

        def source_factory(**kwargs):
            seen_vods.append(kwargs["vod_metadata"]["id"])
            return FakeSource(**kwargs)

        result = run_backfill_ingest(
            "Alice",
            7,
            monitor=monitor,  # type: ignore[arg-type]
            build_state=lambda: self._build_state(store),
            source_factory=source_factory,  # type: ignore[arg-type]
            session_factory=FakeSession,  # type: ignore[arg-type]
            out=logs.append,
        )

        self.assertEqual(seen_vods, ["resume", "fail"])
        self.assertEqual(result.ingested, 1)
        self.assertEqual(result.resumed, 1)
        self.assertEqual(result.skipped, 1)
        self.assertEqual(result.failed, 1)
        self.assertTrue(any(line.startswith("resume vod=resume") for line in logs))
        self.assertTrue(any(line.startswith("starting vod 1/3 vod=resume") for line in logs))
        self.assertTrue(any(line.startswith("skip processed vod=processed") for line in logs))
        self.assertTrue(any(line.startswith("failed vod=fail") for line in logs))

    def test_main_returns_non_zero_on_failure(self) -> None:
        import runners.run_backfill_ingest as module

        original = module.run_backfill_ingest
        try:
            module.run_backfill_ingest = lambda streamer, days: type("R", (), {"failed": 1})()  # type: ignore[assignment]
            self.assertEqual(main(["--streamer", "alice", "--days", "3"]), 1)
        finally:
            module.run_backfill_ingest = original  # type: ignore[assignment]


if __name__ == "__main__":
    unittest.main()
