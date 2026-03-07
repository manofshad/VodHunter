from search.alignment_service import AlignmentService
from search.models import SearchResult
from search.query_embedder import QueryEmbedder
from search.query_preprocessor import QueryPreprocessor
from search.twitch_time import build_twitch_timestamp_url
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

    def search_file(self, clip_path: str, streamer: str) -> SearchResult:
        prepared_wav = None
        try:
            normalized_streamer = streamer.strip().lower()
            if not normalized_streamer:
                raise ValueError("streamer is required")

            prepared_wav = self.preprocessor.prepare(clip_path)
            query_embeddings, query_timestamps = self.query_embedder.embed(prepared_wav)

            if query_embeddings.size == 0:
                return SearchResult(found=False, reason="No embeddings generated for query clip")

            top_k = int(getattr(self.matcher, "top_k", 10))
            _, neighbor_ids = self.store.query_similar_fingerprint_ids(
                query_embeddings=query_embeddings,
                top_k=top_k,
                streamer=normalized_streamer,
            )

            if neighbor_ids.size == 0:
                return SearchResult(found=False, streamer=normalized_streamer, reason=f"No indexed clips found for streamer: {normalized_streamer}")

            alignment = self.alignment.align(neighbor_ids, query_timestamps)

            if not alignment.found or alignment.video_id is None:
                return SearchResult(
                    found=False,
                    streamer=normalized_streamer,
                    reason=alignment.reason or "No aligned match found",
                )

            video_row = self.store.get_video_with_creator(alignment.video_id)
            if video_row is None:
                return SearchResult(
                    found=False,
                    streamer=normalized_streamer,
                    reason="Aligned video metadata not found",
                )

            video_id, video_url, title, streamer = video_row
            video_url_at_timestamp = build_twitch_timestamp_url(video_url, alignment.timestamp_seconds)
            return SearchResult(
                found=True,
                streamer=streamer,
                video_id=video_id,
                video_url=video_url,
                video_url_at_timestamp=video_url_at_timestamp,
                title=title,
                timestamp_seconds=alignment.timestamp_seconds,
                score=alignment.score,
                reason=alignment.reason,
            )
        finally:
            if prepared_wav is not None:
                self.preprocessor.cleanup(prepared_wav)
