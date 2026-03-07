import unittest
from pathlib import Path
import sys

import numpy as np

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from storage.vector_store import VectorStore


class FakeCursor:
    def __init__(self):
        self.executed: list[tuple[str, tuple | None]] = []
        self._fetchall_result = [(11, 0.91), (12, 0.88)]

    def execute(self, query: str, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return list(self._fetchall_result)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None


class FakeConnection:
    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None


class TestVectorStoreStreamerScope(unittest.TestCase):
    def test_query_similar_fingerprint_ids_filters_by_streamer(self) -> None:
        cursor = FakeCursor()
        store = VectorStore.__new__(VectorStore)
        store.pgvector_probes = 10
        store._connect = lambda: FakeConnection(cursor)  # type: ignore[method-assign]

        scores, ids = store.query_similar_fingerprint_ids(
            query_embeddings=np.array([[0.1, 0.2]], dtype=np.float32),
            top_k=2,
            streamer="xQc",
        )

        self.assertAlmostEqual(scores[0][0], 0.91, places=6)
        self.assertAlmostEqual(scores[0][1], 0.88, places=6)
        self.assertEqual(ids.tolist(), [[11, 12]])
        query_sql, query_params = cursor.executed[1]
        self.assertIn("JOIN creators c ON c.id = v.creator_id", query_sql)
        self.assertIn("WHERE LOWER(c.name) = %s", query_sql)
        self.assertEqual(query_params[1], "xqc")

    def test_list_searchable_streamers_returns_names(self) -> None:
        cursor = FakeCursor()
        cursor._fetchall_result = [("Jason",), ("ronaldo",)]
        store = VectorStore.__new__(VectorStore)
        store._connect = lambda: FakeConnection(cursor)  # type: ignore[method-assign]

        names = store.list_searchable_streamers()

        self.assertEqual(names, ["Jason", "ronaldo"])
        self.assertIn("SELECT c.name", cursor.executed[0][0])
        self.assertIn("GROUP BY c.name", cursor.executed[0][0])


if __name__ == "__main__":
    unittest.main()
