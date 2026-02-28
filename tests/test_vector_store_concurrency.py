import tempfile
import threading
import unittest
from pathlib import Path
import sys

import numpy as np

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from storage.vector_store import VectorStore


class TestVectorStoreConcurrency(unittest.TestCase):
    def test_load_and_append_are_safe_under_concurrency(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = VectorStore(
                db_path=str(Path(tmp) / "meta.db"),
                vector_file=str(Path(tmp) / "vectors.npy"),
                id_file=str(Path(tmp) / "ids.npy"),
            )

            first_embeddings = np.ones((2, 4), dtype=np.float32)
            store.append_vectors(first_embeddings, [1, 2])

            errors: list[Exception] = []

            def writer() -> None:
                try:
                    for idx in range(20):
                        vecs = np.full((1, 4), float(idx + 3), dtype=np.float32)
                        store.append_vectors(vecs, [idx + 3])
                except Exception as exc:  # pragma: no cover - explicit failure collection
                    errors.append(exc)

            def reader() -> None:
                try:
                    for _ in range(80):
                        vectors, ids = store.load_vectors_and_ids()
                        self.assertEqual(vectors.shape[0], ids.shape[0])
                except Exception as exc:  # pragma: no cover - explicit failure collection
                    errors.append(exc)

            write_thread = threading.Thread(target=writer)
            read_threads = [threading.Thread(target=reader) for _ in range(3)]

            write_thread.start()
            for t in read_threads:
                t.start()

            write_thread.join()
            for t in read_threads:
                t.join()

            self.assertEqual(errors, [])

            vectors, ids = store.load_vectors_and_ids()
            self.assertEqual(vectors.shape[0], ids.shape[0])
            self.assertEqual(vectors.shape[0], 22)


if __name__ == "__main__":
    unittest.main()
