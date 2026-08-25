import numpy as np
from search.models import AlignmentResult, FingerprintCandidate, SearchDateRange, SearchSegment
from search.search_service import SearchService

class FakePreprocessor:

    def prepare(self, clip_path: str) -> str:
        return clip_path

    def cleanup(self, path: str) -> None:
        return None

class FakeQueryEmbedder:

    def embed(self, wav_path: str):
        return (np.array([[0.1, 0.2]], dtype=np.float32), np.array([0.0], dtype=np.float32))

class FakeAlignment:

    def align_candidates(self, candidates, *, query_duration_seconds):
        if not candidates:
            return AlignmentResult(found=False, reason='no neighbors')
        segment = SearchSegment(0.0, 0.5, 777, 120.0, 120.5, 0.9, 1.0, 120.0, 0.9, 1.0, 1, 1)
        return AlignmentResult(found=True, video_id=777, timestamp_seconds=120, score=0.9, reason='ok', segments=[segment], query_duration_seconds=query_duration_seconds)

class FakeStoreWithKnn:

    def __init__(self):
        self.called = 0
        self.streamer: str | None = None
        self.creator_id: int | None = None

    def get_creator_id_by_name(self, name: str):
        self.streamer = name
        return 42

    def query_fingerprint_candidates(
        self,
        query_embeddings: np.ndarray,
        query_timestamps: np.ndarray,
        top_k: int,
        creator_id: int,
        model_version=None,
        preprocessing_version=None,
        date_range: SearchDateRange | None = None,
    ):
        self.called += 1
        self.creator_id = creator_id
        return [FingerprintCandidate(0, 0.0, 10, 777, 120.0, 0.99, 0)]

    def get_video_with_creator(self, video_id: int):
        return (777, 'https://www.twitch.tv/videos/2699020769', 'Sample title', 'xqc', None, 'https://cdn/xqc.png')

class TestSearchServiceStoreKnn:

    def test_uses_store_knn_path_when_available(self) -> None:
        store = FakeStoreWithKnn()
        service = SearchService(store=store, preprocessor=FakePreprocessor(), query_embedder=FakeQueryEmbedder(), alignment=FakeAlignment(), top_k=10)
        execution = service.search_file('clip.mp4', 'xQc')
        result = execution.result
        assert result.found
        assert store.called == 1
        assert store.streamer == 'xqc'
        assert store.creator_id == 42
        assert result.profile_image_url == 'https://cdn/xqc.png'
        assert execution.metadata.vector_query_duration_ms is not None
