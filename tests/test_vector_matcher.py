import unittest
import numpy as np
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from search.vector_matcher import VectorMatcher


class TestVectorMatcher(unittest.TestCase):
    def test_top_k_ordering(self) -> None:
        matcher = VectorMatcher(top_k=2)
        db_vectors = np.array(
            [
                [1.0, 0.0],
                [0.0, 1.0],
                [-1.0, 0.0],
            ],
            dtype=np.float32,
        )
        db_ids = np.array([10, 20, 30], dtype=np.int64)
        query = np.array(
            [
                [0.9, 0.1],
                [-0.8, 0.2],
            ],
            dtype=np.float32,
        )

        scores, ids = matcher.match(query_embeddings=query, db_vectors=db_vectors, db_ids=db_ids)

        self.assertEqual(scores.shape, (2, 2))
        self.assertEqual(ids.shape, (2, 2))
        self.assertEqual(int(ids[0, 0]), 10)
        self.assertEqual(int(ids[1, 0]), 30)
        self.assertGreaterEqual(float(scores[0, 0]), float(scores[0, 1]))
        self.assertGreaterEqual(float(scores[1, 0]), float(scores[1, 1]))

    def test_empty_inputs(self) -> None:
        matcher = VectorMatcher(top_k=10)
        scores, ids = matcher.match(
            query_embeddings=np.array([]),
            db_vectors=np.array([]),
            db_ids=np.array([]),
        )

        self.assertEqual(scores.shape, (0, 0))
        self.assertEqual(ids.shape, (0, 0))


if __name__ == "__main__":
    unittest.main()
