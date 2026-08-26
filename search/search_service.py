from __future__ import annotations

from dataclasses import replace
import logging
import time
from typing import Any, Callable

import numpy as np

from search.alignment_service import AlignmentService
from search.models import (
    SearchDateRange,
    SearchExecutionMetadata,
    SearchExecutionResult,
    SearchResult,
    UnmatchedRange,
)
from search.query_embedder import QueryEmbedder
from search.query_preprocessor import QueryPreprocessor
from search.twitch_time import build_twitch_timestamp_url
from storage.vector_store import VectorStore

logger = logging.getLogger("uvicorn.error")


def _duration_ms(seconds: float | None) -> int | None:
    if seconds is None:
        return None
    return max(int(round(seconds * 1000.0)), 0)


def _read_attr(source: object | None, *names: str) -> Any:
    for name in names:
        value = getattr(source, name, None)
        if value is not None:
            return value
    return None


def _embedding_observation(query_embedder: QueryEmbedder) -> dict[str, Any]:
    """Normalize local and Modal NMFP timing/version observations."""

    response = getattr(query_embedder, "last_response", None)
    nested_embedder = getattr(query_embedder, "embedder", None)
    extraction = getattr(nested_embedder, "last_result", None)
    source = response or extraction
    metrics = getattr(source, "metrics", source)

    model_version = _read_attr(source, "model_version") or _read_attr(
        query_embedder, "model_version"
    ) or _read_attr(nested_embedder, "model_version")
    preprocessing_version = _read_attr(source, "preprocessing_version") or _read_attr(
        query_embedder, "preprocessing_version"
    ) or _read_attr(nested_embedder, "preprocessing_version")
    return {
        "duration_seconds": _read_attr(source, "duration_seconds")
        or _read_attr(metrics, "audio_duration_seconds"),
        "cold_start": _read_attr(metrics, "cold_start"),
        "model_startup_duration_ms": _read_attr(
            metrics, "model_load_duration_ms", "model_startup_duration_ms"
        ),
        "fingerprint_preprocessing_duration_ms": _read_attr(
            metrics, "preprocessing_duration_ms"
        ),
        "fingerprint_inference_duration_ms": _read_attr(metrics, "inference_duration_ms"),
        "fingerprint_duration_ms": _read_attr(metrics, "total_duration_ms"),
        "model_version": model_version,
        "preprocessing_version": preprocessing_version,
    }


def _whole_query_unmatched(query_duration_seconds: float) -> list[UnmatchedRange]:
    if query_duration_seconds <= 0:
        return []
    return [UnmatchedRange(query_start=0.0, query_end=query_duration_seconds)]


class SearchService:
    def __init__(
        self,
        store: VectorStore,
        preprocessor: QueryPreprocessor,
        query_embedder: QueryEmbedder,
        alignment: AlignmentService,
        top_k: int = 10,
    ):
        self.store = store
        self.preprocessor = preprocessor
        self.query_embedder = query_embedder
        self.alignment = alignment
        self.top_k = max(int(top_k), 1)

    def search_file(
        self,
        clip_path: str,
        streamer: str,
        date_range: SearchDateRange | None = None,
        on_stage_change: Callable[[str], None] | None = None,
        query_duration_seconds: float | None = None,
    ) -> SearchExecutionResult:
        prepared_wav = None
        total_started_at = time.perf_counter()
        metadata = SearchExecutionMetadata()
        try:
            normalized_streamer = streamer.strip().lower()
            if not normalized_streamer:
                raise ValueError("streamer is required")

            if on_stage_change is not None:
                on_stage_change("preprocessing")
            started_at = time.perf_counter()
            if query_duration_seconds is None:
                prepared_wav = self.preprocessor.prepare(clip_path)
            else:
                prepared_wav = self.preprocessor.prepare(
                    clip_path,
                    duration_limit_seconds=query_duration_seconds,
                )
            metadata.preprocess_duration_ms = _duration_ms(time.perf_counter() - started_at)

            if on_stage_change is not None:
                on_stage_change("fingerprinting")
            started_at = time.perf_counter()
            query_embeddings, query_timestamps = self.query_embedder.embed(prepared_wav)
            metadata.embed_duration_ms = _duration_ms(time.perf_counter() - started_at)
            metadata.query_fingerprint_count = int(len(query_timestamps))

            observation = _embedding_observation(self.query_embedder)
            metadata.model_startup_duration_ms = observation["model_startup_duration_ms"]
            metadata.fingerprint_preprocessing_duration_ms = observation[
                "fingerprint_preprocessing_duration_ms"
            ]
            metadata.fingerprint_inference_duration_ms = observation[
                "fingerprint_inference_duration_ms"
            ]
            metadata.fingerprint_duration_ms = observation["fingerprint_duration_ms"]
            metadata.model_cold_start = observation["cold_start"]
            metadata.model_version = observation["model_version"] or getattr(
                self.store, "model_version", None
            )
            metadata.preprocessing_version = observation[
                "preprocessing_version"
            ] or getattr(self.store, "preprocessing_version", None)

            resolved_duration = query_duration_seconds
            if resolved_duration is None:
                resolved_duration = observation["duration_seconds"]
            if resolved_duration is None:
                resolved_duration = self._duration_from_timestamps(query_timestamps)
            resolved_duration = max(float(resolved_duration), 0.0)

            if query_embeddings.size == 0:
                return self._finish_not_found(
                    normalized_streamer,
                    "No fingerprints generated for query clip",
                    resolved_duration,
                    metadata,
                    total_started_at,
                )

            if on_stage_change is not None:
                on_stage_change("retrieving")
            creator_id = self.store.get_creator_id_by_name(normalized_streamer)
            if creator_id is None:
                return self._finish_not_found(
                    normalized_streamer,
                    f"No indexed clips found for streamer: {normalized_streamer}",
                    resolved_duration,
                    metadata,
                    total_started_at,
                )

            top_k = self.top_k
            logger.info(
                "timing event=search_creator_lookup streamer=%s creator_id=%d "
                "query_fingerprint_count=%d top_k=%d",
                normalized_streamer,
                creator_id,
                int(query_embeddings.shape[0]),
                top_k,
            )
            started_at = time.perf_counter()
            candidates = self.store.query_fingerprint_candidates(
                query_embeddings=query_embeddings,
                query_timestamps=query_timestamps,
                top_k=top_k,
                creator_id=creator_id,
                model_version=metadata.model_version,
                preprocessing_version=metadata.preprocessing_version,
                date_range=date_range,
            )
            metadata.vector_query_duration_ms = _duration_ms(time.perf_counter() - started_at)
            metadata.candidate_count = len(candidates)

            if on_stage_change is not None:
                on_stage_change("aligning")
            started_at = time.perf_counter()
            alignment = self.alignment.align_candidates(
                candidates,
                query_duration_seconds=resolved_duration,
            )
            metadata.alignment_duration_ms = _duration_ms(time.perf_counter() - started_at)
            metadata.segment_count = len(alignment.segments)

            if not alignment.found or alignment.video_id is None:
                return self._finish_not_found(
                    normalized_streamer,
                    alignment.reason or "No aligned match found",
                    resolved_duration,
                    metadata,
                    total_started_at,
                    score=alignment.score,
                    unmatched_ranges=alignment.unmatched_ranges,
                )

            if on_stage_change is not None:
                on_stage_change("finalizing")
            video_rows = {
                video_id: self.store.get_video_with_creator(video_id)
                for video_id in {segment.video_id for segment in alignment.segments}
            }
            primary_row = video_rows.get(alignment.video_id)
            if primary_row is None:
                metadata.matched_video_id = alignment.video_id
                metadata.matched_timestamp_seconds = alignment.timestamp_seconds
                metadata.score = alignment.score
                return self._finish_not_found(
                    normalized_streamer,
                    "Aligned video metadata not found",
                    resolved_duration,
                    metadata,
                    total_started_at,
                    score=alignment.score,
                    unmatched_ranges=alignment.unmatched_ranges,
                )

            segments = []
            for segment in alignment.segments:
                row = video_rows.get(segment.video_id)
                timestamp_url = None
                if row is not None:
                    timestamp_url = build_twitch_timestamp_url(
                        row[1], int(round(segment.vod_start))
                    )
                segments.append(replace(segment, video_url_at_timestamp=timestamp_url))

            video_id, video_url, title, streamer_name, thumbnail_url, profile_image_url = primary_row
            primary_timestamp = int(alignment.timestamp_seconds or 0)
            result = SearchResult(
                found=True,
                streamer=streamer_name,
                profile_image_url=profile_image_url,
                video_id=video_id,
                video_url=video_url,
                video_url_at_timestamp=build_twitch_timestamp_url(
                    video_url, primary_timestamp
                ),
                thumbnail_url=thumbnail_url,
                title=title,
                timestamp_seconds=primary_timestamp,
                score=alignment.score,
                reason=alignment.reason,
                segments=segments,
                unmatched_ranges=alignment.unmatched_ranges,
                query_duration_seconds=resolved_duration,
            )
            metadata.result_reason = result.reason
            metadata.found_match = True
            metadata.matched_video_id = result.video_id
            metadata.matched_timestamp_seconds = result.timestamp_seconds
            metadata.score = result.score
            self._log_completion(total_started_at, normalized_streamer, metadata, "found")
            return SearchExecutionResult(result=result, metadata=metadata)
        finally:
            if prepared_wav is not None:
                self.preprocessor.cleanup(prepared_wav)

    @staticmethod
    def _duration_from_timestamps(query_timestamps: np.ndarray) -> float:
        if query_timestamps.size == 0:
            return 0.0
        return float(query_timestamps[-1]) + 0.5

    def _finish_not_found(
        self,
        streamer: str,
        reason: str,
        query_duration_seconds: float,
        metadata: SearchExecutionMetadata,
        total_started_at: float,
        *,
        score: float | None = None,
        unmatched_ranges: list[UnmatchedRange] | None = None,
    ) -> SearchExecutionResult:
        result = SearchResult(
            found=False,
            streamer=streamer,
            reason=reason,
            score=score,
            segments=[],
            unmatched_ranges=(
                unmatched_ranges
                if unmatched_ranges is not None
                else _whole_query_unmatched(query_duration_seconds)
            ),
            query_duration_seconds=query_duration_seconds,
        )
        metadata.result_reason = reason
        metadata.found_match = False
        metadata.score = score
        if metadata.candidate_count is None:
            metadata.candidate_count = 0
        if metadata.segment_count is None:
            metadata.segment_count = 0
        self._log_completion(total_started_at, streamer, metadata, "not_found")
        return SearchExecutionResult(result=result, metadata=metadata)

    @staticmethod
    def _log_completion(
        total_started_at: float,
        streamer: str,
        metadata: SearchExecutionMetadata,
        result: str,
    ) -> None:
        logger.info(
            "timing event=search_pipeline total_ms=%d audio_preprocess_ms=%s "
            "query_fingerprint_wall_ms=%s model_startup_ms=%s fingerprint_preprocess_ms=%s "
            "fingerprint_inference_ms=%s fingerprint_worker_total_ms=%s vector_retrieval_ms=%s "
            "alignment_ms=%s query_fingerprint_count=%s candidate_count=%s segment_count=%s "
            "cold_start=%s result=%s streamer=%s model_version=%s preprocessing_version=%s",
            _duration_ms(time.perf_counter() - total_started_at),
            metadata.preprocess_duration_ms,
            metadata.embed_duration_ms,
            metadata.model_startup_duration_ms,
            metadata.fingerprint_preprocessing_duration_ms,
            metadata.fingerprint_inference_duration_ms,
            metadata.fingerprint_duration_ms,
            metadata.vector_query_duration_ms,
            metadata.alignment_duration_ms,
            metadata.query_fingerprint_count,
            metadata.candidate_count,
            metadata.segment_count,
            metadata.model_cold_start,
            result,
            streamer,
            metadata.model_version,
            metadata.preprocessing_version,
        )
