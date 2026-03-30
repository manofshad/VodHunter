import logging
import time

from search.alignment_service import AlignmentService
from search.models import SearchExecutionMetadata, SearchExecutionResult, SearchResult
from search.query_embedder import QueryEmbedder
from search.query_preprocessor import QueryPreprocessor
from search.twitch_time import build_twitch_timestamp_url
from search.vector_matcher import VectorMatcher
from storage.vector_store import VectorStore

logger = logging.getLogger("uvicorn.error")


def _duration_ms(seconds: float | None) -> int | None:
    if seconds is None:
        return None
    return max(int(round(seconds * 1000.0)), 0)


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

    def search_file(self, clip_path: str, streamer: str) -> SearchExecutionResult:
        prepared_wav = None
        total_started_at = time.perf_counter()
        metadata = SearchExecutionMetadata()
        try:
            normalized_streamer = streamer.strip().lower()
            if not normalized_streamer:
                raise ValueError("streamer is required")

            started_at = time.perf_counter()
            prepared_wav = self.preprocessor.prepare(clip_path)
            preprocess_seconds = time.perf_counter() - started_at
            metadata.preprocess_duration_ms = _duration_ms(preprocess_seconds)

            started_at = time.perf_counter()
            query_embeddings, query_timestamps = self.query_embedder.embed(prepared_wav)
            embed_seconds = time.perf_counter() - started_at
            metadata.embed_duration_ms = _duration_ms(embed_seconds)

            if query_embeddings.size == 0:
                result = SearchResult(found=False, reason="No embeddings generated for query clip")
                metadata.result_reason = result.reason
                metadata.found_match = result.found
                logger.info(
                    "timing event=search_pipeline seconds=%.2f preprocess_seconds=%.2f embed_seconds=%.2f vector_query_seconds=0.00 alignment_seconds=0.00 result=no_embeddings streamer=%s",
                    time.perf_counter() - total_started_at,
                    preprocess_seconds,
                    embed_seconds,
                    normalized_streamer,
                )
                return SearchExecutionResult(result=result, metadata=metadata)

            top_k = int(getattr(self.matcher, "top_k", 10))
            creator_id = self.store.get_creator_id_by_name(normalized_streamer)
            if creator_id is None:
                result = SearchResult(
                    found=False,
                    streamer=normalized_streamer,
                    profile_image_url=None,
                    reason=f"No indexed clips found for streamer: {normalized_streamer}",
                )
                metadata.result_reason = result.reason
                metadata.found_match = result.found
                logger.info(
                    "timing event=search_pipeline seconds=%.2f preprocess_seconds=%.2f embed_seconds=%.2f vector_query_seconds=0.00 alignment_seconds=0.00 result=unknown_streamer streamer=%s",
                    time.perf_counter() - total_started_at,
                    preprocess_seconds,
                    embed_seconds,
                    normalized_streamer,
                )
                return SearchExecutionResult(result=result, metadata=metadata)
            logger.info(
                "timing event=search_creator_lookup streamer=%s creator_id=%d query_embedding_count=%d top_k=%d",
                normalized_streamer,
                creator_id,
                int(query_embeddings.shape[0]),
                top_k,
            )
            started_at = time.perf_counter()
            _, neighbor_ids = self.store.query_similar_fingerprint_ids(
                query_embeddings=query_embeddings,
                top_k=top_k,
                creator_id=creator_id,
            )
            vector_query_seconds = time.perf_counter() - started_at
            metadata.vector_query_duration_ms = _duration_ms(vector_query_seconds)

            if neighbor_ids.size == 0:
                result = SearchResult(found=False, streamer=normalized_streamer, profile_image_url=None, reason=f"No indexed clips found for streamer: {normalized_streamer}")
                metadata.result_reason = result.reason
                metadata.found_match = result.found
                logger.info(
                    "timing event=search_pipeline seconds=%.2f preprocess_seconds=%.2f embed_seconds=%.2f vector_query_seconds=%.2f alignment_seconds=0.00 result=no_neighbors streamer=%s",
                    time.perf_counter() - total_started_at,
                    preprocess_seconds,
                    embed_seconds,
                    vector_query_seconds,
                    normalized_streamer,
                )
                return SearchExecutionResult(result=result, metadata=metadata)

            started_at = time.perf_counter()
            alignment = self.alignment.align(neighbor_ids, query_timestamps)
            alignment_seconds = time.perf_counter() - started_at
            metadata.alignment_duration_ms = _duration_ms(alignment_seconds)

            if not alignment.found or alignment.video_id is None:
                result = SearchResult(
                    found=False,
                    streamer=normalized_streamer,
                    profile_image_url=None,
                    reason=alignment.reason or "No aligned match found",
                )
                metadata.result_reason = result.reason
                metadata.found_match = result.found
                metadata.score = alignment.score
                logger.info(
                    "timing event=search_pipeline seconds=%.2f preprocess_seconds=%.2f embed_seconds=%.2f vector_query_seconds=%.2f alignment_seconds=%.2f result=not_found streamer=%s",
                    time.perf_counter() - total_started_at,
                    preprocess_seconds,
                    embed_seconds,
                    vector_query_seconds,
                    alignment_seconds,
                    normalized_streamer,
                )
                return SearchExecutionResult(result=result, metadata=metadata)

            video_row = self.store.get_video_with_creator(alignment.video_id)
            if video_row is None:
                result = SearchResult(
                    found=False,
                    streamer=normalized_streamer,
                    profile_image_url=None,
                    reason="Aligned video metadata not found",
                )
                metadata.result_reason = result.reason
                metadata.found_match = result.found
                metadata.matched_video_id = alignment.video_id
                metadata.matched_timestamp_seconds = alignment.timestamp_seconds
                metadata.score = alignment.score
                logger.info(
                    "timing event=search_pipeline seconds=%.2f preprocess_seconds=%.2f embed_seconds=%.2f vector_query_seconds=%.2f alignment_seconds=%.2f result=missing_video streamer=%s",
                    time.perf_counter() - total_started_at,
                    preprocess_seconds,
                    embed_seconds,
                    vector_query_seconds,
                    alignment_seconds,
                    normalized_streamer,
                )
                return SearchExecutionResult(result=result, metadata=metadata)

            video_id, video_url, title, streamer, thumbnail_url, profile_image_url = video_row
            video_url_at_timestamp = build_twitch_timestamp_url(video_url, alignment.timestamp_seconds)
            result = SearchResult(
                found=True,
                streamer=streamer,
                profile_image_url=profile_image_url,
                video_id=video_id,
                video_url=video_url,
                video_url_at_timestamp=video_url_at_timestamp,
                thumbnail_url=thumbnail_url,
                title=title,
                timestamp_seconds=alignment.timestamp_seconds,
                score=alignment.score,
                reason=alignment.reason,
            )
            metadata.result_reason = result.reason
            metadata.found_match = result.found
            metadata.matched_video_id = result.video_id
            metadata.matched_timestamp_seconds = result.timestamp_seconds
            metadata.score = result.score
            logger.info(
                "timing event=search_pipeline seconds=%.2f preprocess_seconds=%.2f embed_seconds=%.2f vector_query_seconds=%.2f alignment_seconds=%.2f result=found streamer=%s",
                time.perf_counter() - total_started_at,
                preprocess_seconds,
                embed_seconds,
                vector_query_seconds,
                alignment_seconds,
                normalized_streamer,
            )
            return SearchExecutionResult(result=result, metadata=metadata)
        finally:
            if prepared_wav is not None:
                self.preprocessor.cleanup(prepared_wav)
