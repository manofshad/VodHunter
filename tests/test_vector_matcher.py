import numpy as np
from search.vector_matcher import VectorMatcher

class TestVectorMatcher:

    def test_top_k_ordering(self) -> None:
        matcher = VectorMatcher(top_k=2)
        db_vectors = np.array([[1.0, 0.0], [0.0, 1.0], [-1.0, 0.0]], dtype=np.float32)
        db_ids = np.array([10, 20, 30], dtype=np.int64)
        query = np.array([[0.9, 0.1], [-0.8, 0.2]], dtype=np.float32)
        scores, ids = matcher.match(query_embeddings=query, db_vectors=db_vectors, db_ids=db_ids)
        assert scores.shape == (2, 2)
        assert ids.shape == (2, 2)
        assert int(ids[0, 0]) == 10
        assert int(ids[1, 0]) == 30
        assert float(scores[0, 0]) >= float(scores[0, 1])
        assert float(scores[1, 0]) >= float(scores[1, 1])

    def test_empty_inputs(self) -> None:
        matcher = VectorMatcher(top_k=10)
        scores, ids = matcher.match(query_embeddings=np.array([]), db_vectors=np.array([]), db_ids=np.array([]))
        assert scores.shape == (0, 0)
        assert ids.shape == (0, 0)
