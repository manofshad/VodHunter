import unittest
from pathlib import Path
import sys

import numpy as np

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipeline.ingest_session import IngestSession
from sources.audio_chunk import AudioChunk


class FakeSource:
    def __init__(self):
        self.video_id = 55
        self.creator_id = 7
        self.is_finished = False
        self._returned = False
        self.started = False
        self.stopped = False

    def start(self) -> None:
        self.started = True

    def next_chunk(self):
        if self._returned:
            self.is_finished = True
            return None
        self._returned = True
        return AudioChunk(audio_path="chunk.wav", offset_seconds=0.0, duration_seconds=60.0)

    def stop(self) -> None:
        self.stopped = True


class FakeEmbedder:
    def embed(self, audio_path: str, offset_seconds: float = 0.0):
        return np.array([[0.1, 0.2]], dtype=np.float32), np.array([offset_seconds], dtype=np.float32)


class FakeStore:
    def __init__(self):
        self.append_call = None

    def store_fingerprints(self, video_id: int, timestamps: np.ndarray):
        return [101]

    def append_vectors(self, embeddings: np.ndarray, ids: list[int], creator_id: int | None):
        self.append_call = (embeddings, ids, creator_id)


class TestIngestSession(unittest.TestCase):
    def test_append_vectors_receives_creator_id(self) -> None:
        source = FakeSource()
        store = FakeStore()
        session = IngestSession(
            source=source,  # type: ignore[arg-type]
            embedder=FakeEmbedder(),  # type: ignore[arg-type]
            store=store,  # type: ignore[arg-type]
            poll_interval=0.0,
        )

        session.run()

        self.assertTrue(source.started)
        self.assertTrue(source.stopped)
        self.assertIsNotNone(store.append_call)
        _, ids, creator_id = store.append_call
        self.assertEqual(ids, [101])
        self.assertEqual(creator_id, 7)


if __name__ == "__main__":
    unittest.main()
