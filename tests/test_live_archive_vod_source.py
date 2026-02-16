import os
import tempfile
import unittest
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from sources.live_archive_vod_source import LiveArchiveVODSource
from storage.vector_store import VectorStore


class FakeMonitor:
    def __init__(self, live_sequence: list[bool], vod_duration_seconds: int = 240):
        self.live_sequence = list(live_sequence)
        self.vod_duration_seconds = vod_duration_seconds

    def is_live(self, streamer: str) -> bool:
        if len(self.live_sequence) > 1:
            return self.live_sequence.pop(0)
        return self.live_sequence[0]

    def get_user_id(self, streamer: str) -> str:
        return "user-1"

    def get_latest_archive_vod(self, user_id: str):
        return {
            "id": "vod-1",
            "url": "https://www.twitch.tv/videos/vod-1",
            "title": "Live stream",
            "duration_seconds": self.vod_duration_seconds,
        }


class TestLiveArchiveVODSource(unittest.TestCase):
    def _make_source(self, tmp: str, live_sequence: list[bool]) -> LiveArchiveVODSource:
        store = VectorStore(db_path=f"{tmp}/meta.db", vector_file=f"{tmp}/v.npy", id_file=f"{tmp}/i.npy")
        store.init_db()

        monitor = FakeMonitor(live_sequence=live_sequence)
        source = LiveArchiveVODSource(
            streamer="alice",
            store=store,
            twitch_monitor=monitor,  # type: ignore[arg-type]
            chunk_seconds=60,
            lag_seconds=120,
            poll_seconds=0.0,
            finalize_checks=2,
            temp_dir=f"{tmp}/chunks",
        )

        def fake_extract_chunk(start_seconds: int, duration_seconds: int) -> str:
            out = os.path.join(source.temp_dir, f"chunk_{start_seconds}_{duration_seconds}.wav")
            os.makedirs(source.temp_dir, exist_ok=True)
            with open(out, "wb") as f:
                f.write(b"fake")
            return out

        source._extract_chunk = fake_extract_chunk  # type: ignore[method-assign]
        return source

    def test_cursor_advances_on_next_poll(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = self._make_source(tmp, live_sequence=[True, True, True])
            source.start()

            chunk1 = source.next_chunk()
            self.assertIsNotNone(chunk1)
            self.assertEqual(source.ingest_cursor_seconds, 0)

            chunk2 = source.next_chunk()
            self.assertIsNotNone(chunk2)
            self.assertEqual(source.ingest_cursor_seconds, 60)

    def test_finalize_marks_video_processed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = self._make_source(tmp, live_sequence=[True, True, False, False, False, False])
            source.start()

            for _ in range(20):
                source.next_chunk()
                if source.is_finished:
                    break

            self.assertTrue(source.is_finished)
            self.assertIsNotNone(source.video_id)
            assert source.video_id is not None

            row = source.store.get_video_by_url("https://www.twitch.tv/videos/vod-1")
            self.assertIsNotNone(row)
            assert row is not None
            self.assertTrue(row[4])


if __name__ == "__main__":
    unittest.main()
