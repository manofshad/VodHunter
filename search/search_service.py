from search.alignment_service import AlignmentService
from search.models import SearchResult
from search.query_embedder import QueryEmbedder
from search.query_preprocessor import QueryPreprocessor
from search.vector_matcher import VectorMatcher
from storage.vector_store import VectorStore


class SearchService:
    def __init__(
        self,
        store: VectorStore,
        preprocessor: QueryPreprocessor,
        query_embedder: QueryEmbedder,
        matcher: VectorMatcher,
        alignment: AlignmentService,
    ):
        self.store = store
        self.preprocessor = preprocessor
        self.query_embedder = query_embedder
        self.matcher = matcher
        self.alignment = alignment

    def search_file(self, clip_path: str) -> SearchResult:
        prepared_wav = None
        try:
            prepared_wav = self.preprocessor.prepare(clip_path)
            query_embeddings, query_timestamps = self.query_embedder.embed(prepared_wav)

            if query_embeddings.size == 0:
                return SearchResult(found=False, reason="No embeddings generated for query clip")

            db_vectors, db_ids = self.store.load_vectors_and_ids()
            if db_vectors.size == 0 or db_ids.size == 0:
                return SearchResult(found=False, reason="Vector index is empty")

            _, neighbor_ids = self.matcher.match(query_embeddings, db_vectors, db_ids)
            alignment = self.alignment.align(neighbor_ids, query_timestamps)

            if not alignment.found or alignment.video_id is None:
                return SearchResult(found=False, reason=alignment.reason or "No aligned match found")

            video_row = self.store.get_video_with_creator(alignment.video_id)
            if video_row is None:
                return SearchResult(found=False, reason="Aligned video metadata not found")

            video_id, video_url, title, streamer = video_row
            return SearchResult(
                found=True,
                streamer=streamer,
                video_id=video_id,
                video_url=video_url,
                title=title,
                timestamp_seconds=alignment.timestamp_seconds,
                score=alignment.score,
                reason=alignment.reason,
            )
        finally:
            if prepared_wav is not None:
                self.preprocessor.cleanup(prepared_wav)
