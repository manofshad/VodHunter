from __future__ import annotations

from dataclasses import dataclass, replace
import logging
import time
from typing import Callable, Protocol

import numpy as np

from search.alignment_service import AlignmentService
from search.models import (
    FingerprintCandidate,
    SearchDateRange,
    SearchExecutionMetadata,
    SearchExecutionResult,
    SearchResult,
    SearchSegment,
    SearchSource,
    UnmatchedRange,
)
from search.query_embedder import QueryEmbedder
from search.query_preprocessor import QueryPreprocessor
from search.twitch_time import build_twitch_timestamp_url
from storage.records import VideoRecord


class _VideoReader(Protocol):
    def get_creator_id_by_name(self, name: str) -> int | None: ...

    def get_video_with_creator(self, video_id: int) -> VideoRecord | None: ...


class _FingerprintReader(Protocol):
    model_version: str
    preprocessing_version: str

    def query_fingerprint_candidates(
        self,
        query_embeddings: np.ndarray,
        query_timestamps: np.ndarray,
        top_k: int,
        creator_id: int,
        model_version: str | None = None,
        preprocessing_version: str | None = None,
        date_range: SearchDateRange | None = None,
    ) -> list[FingerprintCandidate]: ...

logger = logging.getLogger("uvicorn.error")


@dataclass(frozen=True, slots=True)
class _RankedSourceEntry:
    is_primary: bool
    representative: SearchSegment
    source: SearchSource


def _duration_ms(seconds: float | None) -> int | None:
    if seconds is None:
        return None
    return max(int(round(seconds * 1000.0)), 0)


def _record_stage_timing(
    stage: str,
    started_at: float,
    on_stage_timing: Callable[[str, int], None] | None,
) -> None:
    if on_stage_timing is not None:
        on_stage_timing(stage, _duration_ms(time.perf_counter() - started_at) or 0)


def _embedding_observation(query_embedder: QueryEmbedder) -> dict[str, object | None]:
    """Read timing and identity from the local NMFP extraction result."""

    extraction = query_embedder.last_result
    if extraction is None:
        return {
            "duration_seconds": None,
            "cold_start": None,
            "model_startup_duration_ms": None,
            "fingerprint_preprocessing_duration_ms": None,
            "fingerprint_inference_duration_ms": None,
            "fingerprint_duration_ms": None,
            "model_version": None,
            "preprocessing_version": None,
        }

    metrics = extraction.metrics
    return {
        "duration_seconds": metrics.audio_duration_seconds,
        "cold_start": metrics.cold_start,
        "model_startup_duration_ms": metrics.model_load_duration_ms,
        "fingerprint_preprocessing_duration_ms": metrics.preprocessing_duration_ms,
        "fingerprint_inference_duration_ms": metrics.inference_duration_ms,
        "fingerprint_duration_ms": metrics.total_duration_ms,
        "model_version": extraction.model_version,
        "preprocessing_version": extraction.preprocessing_version,
    }


def _whole_query_unmatched(query_duration_seconds: float) -> list[UnmatchedRange]:
    if query_duration_seconds <= 0:
        return []
    return [UnmatchedRange(query_start=0.0, query_end=query_duration_seconds)]


class SearchService:
    def __init__(
        self,
        videos: _VideoReader,
        fingerprints: _FingerprintReader,
        preprocessor: QueryPreprocessor,
        query_embedder: QueryEmbedder,
        alignment: AlignmentService,
        top_k: int = 10,
    ):
        self.videos = videos
        self.fingerprints = fingerprints
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
        on_stage_timing: Callable[[str, int], None] | None = None,
        query_duration_seconds: float | None = None,
    ) -> SearchExecutionResult:
        prepared_wav = None
        metadata = SearchExecutionMetadata()
        try:
            normalized_streamer = streamer.strip().lower()
            if not normalized_streamer:
                raise ValueError("streamer is required")

            if on_stage_change is not None:
                on_stage_change("preprocessing")
            started_at = time.perf_counter()
            try:
                if query_duration_seconds is None:
                    prepared_wav = self.preprocessor.prepare(clip_path)
                else:
                    prepared_wav = self.preprocessor.prepare(
                        clip_path,
                        duration_limit_seconds=query_duration_seconds,
                    )
            finally:
                _record_stage_timing("audio_preprocess", started_at, on_stage_timing)
            metadata.preprocess_duration_ms = _duration_ms(time.perf_counter() - started_at)

            if on_stage_change is not None:
                on_stage_change("fingerprinting")
            started_at = time.perf_counter()
            try:
                query_embeddings, query_timestamps = self.query_embedder.embed(prepared_wav)
            finally:
                _record_stage_timing("fingerprint", started_at, on_stage_timing)
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
                self.fingerprints, "model_version", None
            )
            metadata.preprocessing_version = observation[
                "preprocessing_version"
            ] or getattr(self.fingerprints, "preprocessing_version", None)

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
                )

            if on_stage_change is not None:
                on_stage_change("retrieving")
            started_at = time.perf_counter()
            try:
                creator_id = self.videos.get_creator_id_by_name(normalized_streamer)
                if creator_id is None:
                    return self._finish_not_found(
                        normalized_streamer,
                        f"No indexed clips found for streamer: {normalized_streamer}",
                        resolved_duration,
                        metadata,
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
                query_started_at = time.perf_counter()
                candidates = self.fingerprints.query_fingerprint_candidates(
                    query_embeddings=query_embeddings,
                    query_timestamps=query_timestamps,
                    top_k=top_k,
                    creator_id=creator_id,
                    model_version=metadata.model_version,
                    preprocessing_version=metadata.preprocessing_version,
                    date_range=date_range,
                )
            finally:
                _record_stage_timing("vector_retrieval", started_at, on_stage_timing)
            metadata.vector_query_duration_ms = _duration_ms(
                time.perf_counter() - query_started_at
            )
            metadata.candidate_count = len(candidates)

            if on_stage_change is not None:
                on_stage_change("aligning")
            started_at = time.perf_counter()
            try:
                alignment = self.alignment.align_candidates(
                    candidates,
                    query_duration_seconds=resolved_duration,
                )
            finally:
                _record_stage_timing("alignment", started_at, on_stage_timing)
            metadata.alignment_duration_ms = _duration_ms(time.perf_counter() - started_at)
            metadata.segment_count = len(alignment.segments)

            if not alignment.found or alignment.video_id is None:
                return self._finish_not_found(
                    normalized_streamer,
                    alignment.reason or "No aligned match found",
                    resolved_duration,
                    metadata,
                    score=alignment.score,
                    unmatched_ranges=alignment.unmatched_ranges,
                )

            if on_stage_change is not None:
                on_stage_change("finalizing")
            video_rows = {
                video_id: self.videos.get_video_with_creator(video_id)
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
                    score=alignment.score,
                    unmatched_ranges=alignment.unmatched_ranges,
                )

            segments = []
            for segment in alignment.segments:
                row = video_rows.get(segment.video_id)
                timestamp_url = None
                if row is not None:
                    timestamp_url = build_twitch_timestamp_url(
                        row.url, int(round(segment.vod_start))
                    )
                segments.append(replace(segment, video_url_at_timestamp=timestamp_url))

            segments_by_video_id: dict[int, list[SearchSegment]] = {}
            for segment in segments:
                segments_by_video_id.setdefault(segment.video_id, []).append(segment)

            source_entries: list[_RankedSourceEntry] = []
            for source_video_id, source_segments in segments_by_video_id.items():
                source_row = video_rows.get(source_video_id)
                representative = max(
                    source_segments,
                    key=lambda segment: (
                        segment.ranking_score,
                        segment.duration_seconds,
                        segment.supporting_fingerprints,
                    ),
                )
                if source_row is None:
                    source_entries.append(
                        _RankedSourceEntry(
                            is_primary=source_video_id == alignment.video_id,
                            representative=representative,
                            source=SearchSource(
                                video_id=source_video_id,
                                video_url_at_timestamp=representative.video_url_at_timestamp,
                                streamer=normalized_streamer,
                                segments=source_segments,
                            ),
                        )
                    )
                    continue

                source_entries.append(
                    _RankedSourceEntry(
                        is_primary=source_row.id == alignment.video_id,
                        representative=representative,
                        source=SearchSource(
                            video_id=source_row.id,
                            video_url=source_row.url,
                            video_url_at_timestamp=representative.video_url_at_timestamp,
                            thumbnail_url=source_row.thumbnail_url,
                            title=source_row.title,
                            streamer=source_row.creator_name,
                            profile_image_url=source_row.creator_profile_image_url,
                            segments=source_segments,
                        ),
                    )
                )

            source_entries.sort(
                key=lambda entry: (
                    entry.is_primary,
                    entry.representative.ranking_score,
                    entry.representative.duration_seconds,
                    entry.representative.supporting_fingerprints,
                ),
                reverse=True,
            )
            sources = [entry.source for entry in source_entries]

            primary_timestamp = int(alignment.timestamp_seconds or 0)
            result = SearchResult(
                found=True,
                streamer=primary_row.creator_name,
                profile_image_url=primary_row.creator_profile_image_url,
                video_id=primary_row.id,
                video_url=primary_row.url,
                video_url_at_timestamp=build_twitch_timestamp_url(
                    primary_row.url, primary_timestamp
                ),
                thumbnail_url=primary_row.thumbnail_url,
                title=primary_row.title,
                timestamp_seconds=primary_timestamp,
                score=alignment.score,
                reason=alignment.reason,
                sources=sources,
                segments=segments,
                unmatched_ranges=alignment.unmatched_ranges,
                query_duration_seconds=resolved_duration,
            )
            metadata.result_reason = result.reason
            metadata.found_match = True
            metadata.matched_video_id = result.video_id
            metadata.matched_timestamp_seconds = result.timestamp_seconds
            metadata.score = result.score
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
        return SearchExecutionResult(result=result, metadata=metadata)
