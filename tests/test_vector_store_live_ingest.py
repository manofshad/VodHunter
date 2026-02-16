import tempfile
import unittest
from pathlib import Path
import sys

import numpy as np

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from storage.vector_store import VectorStore


class TestVectorStoreLiveIngest(unittest.TestCase):
    def test_fingerprint_insert_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = f"{tmp}/meta.db"
            store = VectorStore(db_path=db_path, vector_file=f"{tmp}/v.npy", id_file=f"{tmp}/i.npy")
            store.init_db()

            creator_id = store.create_or_get_creator("alice", "https://twitch.tv/alice")
            video_id = store.create_video(creator_id, "https://www.twitch.tv/videos/1", "Stream", False)

            first = store.store_fingerprints(video_id=video_id, timestamps=np.array([1.0, 2.0], dtype=np.float32))
            second = store.store_fingerprints(video_id=video_id, timestamps=np.array([1.0, 2.0], dtype=np.float32))

            self.assertEqual(first, second)

    def test_live_ingest_state_upsert_and_read(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = f"{tmp}/meta.db"
            store = VectorStore(db_path=db_path, vector_file=f"{tmp}/v.npy", id_file=f"{tmp}/i.npy")
            store.init_db()

            creator_id = store.create_or_get_creator("alice", "https://twitch.tv/alice")
            video_id = store.create_video(creator_id, "https://www.twitch.tv/videos/1", "Stream", False)

            store.upsert_live_ingest_state(
                vod_platform_id="vod-1",
                video_id=video_id,
                streamer="alice",
                last_ingested_seconds=120,
                last_seen_duration_seconds=240,
            )
            state = store.get_live_ingest_state("vod-1")

            self.assertIsNotNone(state)
            assert state is not None
            self.assertEqual(state["last_ingested_seconds"], 120)
            self.assertEqual(state["last_seen_duration_seconds"], 240)


if __name__ == "__main__":
    unittest.main()
