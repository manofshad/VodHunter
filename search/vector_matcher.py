import numpy as np


class VectorMatcher:
    def __init__(self, top_k: int = 10):
        self.top_k = top_k

    def match(
        self,
        query_embeddings: np.ndarray,
        db_vectors: np.ndarray,
        db_ids: np.ndarray,
    ) -> tuple[np.ndarray, np.ndarray]:
        if query_embeddings.size == 0:
            return np.empty((0, 0), dtype=np.float32), np.empty((0, 0), dtype=np.int64)
        if db_vectors.size == 0 or db_ids.size == 0:
            return np.empty((0, 0), dtype=np.float32), np.empty((0, 0), dtype=np.int64)
        if len(db_vectors) != len(db_ids):
            raise ValueError("Vector and fingerprint ID arrays are misaligned")

        q = self._l2_normalize_rows(query_embeddings.astype(np.float32))
        d = self._l2_normalize_rows(db_vectors.astype(np.float32))

        sim = np.dot(q, d.T)
        k = min(self.top_k, sim.shape[1])
        if k <= 0:
            return np.empty((0, 0), dtype=np.float32), np.empty((0, 0), dtype=np.int64)

        top_idx = np.argsort(sim, axis=1)[:, -k:][:, ::-1]
        top_scores = np.take_along_axis(sim, top_idx, axis=1)
        top_ids = db_ids[top_idx]

        return top_scores, top_ids

    def _l2_normalize_rows(self, x: np.ndarray) -> np.ndarray:
        norms = np.linalg.norm(x, axis=1, keepdims=True)
        return x / (norms + 1e-10)
