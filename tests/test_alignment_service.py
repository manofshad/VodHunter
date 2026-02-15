import unittest
import numpy as np
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from search.alignment_service import AlignmentConfig, AlignmentService


class FakeStore:
    def __init__(self):
        self.rows = {
            1: (1, 100, 10.0),
            2: (2, 100, 11.0),
            3: (3, 200, 40.0),
        }

    def get_fingerprint_rows(self, ids: list[int]):
        out = []
        for fp_id in ids:
            row = self.rows.get(int(fp_id))
            if row is not None:
                out.append(row)
        return out


class TestAlignmentService(unittest.TestCase):
    def test_picks_best_offset_candidate(self) -> None:
        store = FakeStore()
        svc = AlignmentService(
            store=store,  # type: ignore[arg-type]
            config=AlignmentConfig(min_vote_count=2, min_vote_ratio=0.50),
        )

        neighbor_ids = np.array(
            [
                [1, 3],
                [2, 3],
                [2, 1],
            ],
            dtype=np.int64,
        )
        query_ts = np.array([0.0, 1.0, 1.0], dtype=np.float32)

        result = svc.align(neighbor_ids=neighbor_ids, query_timestamps=query_ts)

        self.assertTrue(result.found)
        self.assertEqual(result.video_id, 100)
        self.assertEqual(result.timestamp_seconds, 10)
        self.assertIsNotNone(result.score)
        self.assertGreaterEqual(float(result.score), 0.5)

    def test_rejects_below_threshold(self) -> None:
        store = FakeStore()
        svc = AlignmentService(
            store=store,  # type: ignore[arg-type]
            config=AlignmentConfig(min_vote_count=5, min_vote_ratio=0.90),
        )

        neighbor_ids = np.array(
            [
                [1, 3],
                [2, 3],
            ],
            dtype=np.int64,
        )
        query_ts = np.array([0.0, 1.0], dtype=np.float32)

        result = svc.align(neighbor_ids=neighbor_ids, query_timestamps=query_ts)

        self.assertFalse(result.found)
        self.assertIsNotNone(result.reason)


if __name__ == "__main__":
    unittest.main()
