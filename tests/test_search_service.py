import unittest
from pathlib import Path
import sys

import numpy as np

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from search.models import AlignmentResult
from search.search_service import SearchService


class FakePreprocessor:
    def prepare(self, clip_path: str) -> str:
        return clip_path

    def cleanup(self, path: str) -> None:
        return None


class FakeQueryEmbedder:
    def __init__(self, embeddings: np.ndarray, timestamps: np.ndarray):
        self.embeddings = embeddings
        self.timestamps = timestamps

    def embed(self, wav_path: str):
        return self.embeddings, self.timestamps


class FakeMatcher:
    def match(self, query_embeddings, db_vectors, db_ids):
        return np.array([[1.0]], dtype=np.float32), np.array([[int(db_ids[0])]], dtype=np.int64)


class FakeAlignment:
    def __init__(self, result: AlignmentResult):
        self.result = result

    def align(self, neighbor_ids, query_timestamps):
        return self.result


class FakeStore:
    def __init__(self):
        self.last_streamer: str | None = None

    def query_similar_fingerprint_ids(self, query_embeddings: np.ndarray, top_k: int, streamer: str):
        self.last_streamer = streamer
        return np.array([[1.0]], dtype=np.float32), np.array([[10]], dtype=np.int64)

    def get_video_with_creator(self, video_id: int):
        return (
            777,
            "https://www.twitch.tv/videos/2699020769",
            "Sample title",
            "xqc",
        )


class TestSearchService(unittest.TestCase):
    def test_found_result_includes_timestamp_url(self) -> None:
        store = FakeStore()
        service = SearchService(
            store=store,  # type: ignore[arg-type]
            preprocessor=FakePreprocessor(),  # type: ignore[arg-type]
            query_embedder=FakeQueryEmbedder(
                embeddings=np.array([[0.1, 0.2]], dtype=np.float32),
                timestamps=np.array([0.0], dtype=np.float32),
            ),  # type: ignore[arg-type]
            matcher=FakeMatcher(),  # type: ignore[arg-type]
            alignment=FakeAlignment(
                AlignmentResult(
                    found=True,
                    video_id=777,
                    timestamp_seconds=1368,
                    score=0.9,
                    reason="ok",
                )
            ),  # type: ignore[arg-type]
        )

        result = service.search_file("clip.mp4", "xQc")

        self.assertTrue(result.found)
        self.assertEqual(store.last_streamer, "xqc")
        self.assertEqual(result.video_url_at_timestamp, "https://www.twitch.tv/videos/2699020769?t=22m48s")

    def test_not_found_result_has_no_timestamp_url(self) -> None:
        service = SearchService(
            store=FakeStore(),  # type: ignore[arg-type]
            preprocessor=FakePreprocessor(),  # type: ignore[arg-type]
            query_embedder=FakeQueryEmbedder(
                embeddings=np.array([[0.1, 0.2]], dtype=np.float32),
                timestamps=np.array([0.0], dtype=np.float32),
            ),  # type: ignore[arg-type]
            matcher=FakeMatcher(),  # type: ignore[arg-type]
            alignment=FakeAlignment(
                AlignmentResult(
                    found=False,
                    reason="No aligned match found",
                )
            ),  # type: ignore[arg-type]
        )

        result = service.search_file("clip.mp4", "xqc")

        self.assertFalse(result.found)
        self.assertIsNone(result.video_url_at_timestamp)


if __name__ == "__main__":
    unittest.main()
