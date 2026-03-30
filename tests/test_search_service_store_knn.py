import numpy as np
from search.models import AlignmentResult
from search.search_service import SearchService

class FakePreprocessor:

    def prepare(self, clip_path: str) -> str:
        return clip_path

    def cleanup(self, path: str) -> None:
        return None

class FakeQueryEmbedder:

    def embed(self, wav_path: str):
        return (np.array([[0.1, 0.2]], dtype=np.float32), np.array([0.0], dtype=np.float32))

class FakeMatcher:
    top_k = 10

    def match(self, query_embeddings, db_vectors, db_ids):
        raise AssertionError('legacy matcher should not be called when store KNN API exists')

class FakeAlignment:

    def align(self, neighbor_ids, query_timestamps):
        if neighbor_ids.size == 0:
            return AlignmentResult(found=False, reason='no neighbors')
        return AlignmentResult(found=True, video_id=777, timestamp_seconds=120, score=0.9, reason='ok')

class FakeStoreWithKnn:

    def __init__(self):
        self.called = 0
        self.streamer: str | None = None
        self.creator_id: int | None = None

    def get_creator_id_by_name(self, name: str):
        self.streamer = name
        return 42

    def query_similar_fingerprint_ids(self, query_embeddings: np.ndarray, top_k: int, creator_id: int):
        self.called += 1
        self.creator_id = creator_id
        return (np.array([[0.99]], dtype=np.float32), np.array([[10]], dtype=np.int64))

    def get_video_with_creator(self, video_id: int):
        return (777, 'https://www.twitch.tv/videos/2699020769', 'Sample title', 'xqc', None, 'https://cdn/xqc.png')

class TestSearchServiceStoreKnn:

    def test_uses_store_knn_path_when_available(self) -> None:
        store = FakeStoreWithKnn()
        service = SearchService(store=store, preprocessor=FakePreprocessor(), query_embedder=FakeQueryEmbedder(), matcher=FakeMatcher(), alignment=FakeAlignment())
        execution = service.search_file('clip.mp4', 'xQc')
        result = execution.result
        assert result.found
        assert store.called == 1
        assert store.streamer == 'xqc'
        assert store.creator_id == 42
        assert result.profile_image_url == 'https://cdn/xqc.png'
        assert execution.metadata.vector_query_duration_ms is not None
