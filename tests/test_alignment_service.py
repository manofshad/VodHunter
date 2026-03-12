import numpy as np
from search.alignment_service import AlignmentConfig, AlignmentService

class FakeStore:

    def __init__(self):
        self.rows = {1: (1, 100, 10.0), 2: (2, 100, 11.0), 3: (3, 200, 40.0)}

    def get_fingerprint_rows(self, ids: list[int]):
        out = []
        for fp_id in ids:
            row = self.rows.get(int(fp_id))
            if row is not None:
                out.append(row)
        return out

class TestAlignmentService:

    def test_picks_best_offset_candidate(self) -> None:
        store = FakeStore()
        svc = AlignmentService(store=store, config=AlignmentConfig(min_vote_count=2, min_vote_ratio=0.5))
        neighbor_ids = np.array([[1, 3], [2, 3], [2, 1]], dtype=np.int64)
        query_ts = np.array([0.0, 1.0, 1.0], dtype=np.float32)
        result = svc.align(neighbor_ids=neighbor_ids, query_timestamps=query_ts)
        assert result.found
        assert result.video_id == 100
        assert result.timestamp_seconds == 10
        assert result.score is not None
        assert float(result.score) >= 0.5

    def test_rejects_below_threshold(self) -> None:
        store = FakeStore()
        svc = AlignmentService(store=store, config=AlignmentConfig(min_vote_count=5, min_vote_ratio=0.9))
        neighbor_ids = np.array([[1, 3], [2, 3]], dtype=np.int64)
        query_ts = np.array([0.0, 1.0], dtype=np.float32)
        result = svc.align(neighbor_ids=neighbor_ids, query_timestamps=query_ts)
        assert not result.found
        assert result.reason is not None
