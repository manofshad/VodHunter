from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable

import numpy as np


@dataclass(frozen=True)
class CutDetectionSettings:
    """Tunable rules for turning NMFP candidates into continuous source tracks."""

    top_k: int = 10
    hop_seconds: float = 0.5
    offset_bin_seconds: float = 0.5
    offset_tolerance_seconds: float = 1.0
    max_unmatched_gap_seconds: float = 2.0
    min_support: int = 6
    min_duration_seconds: float = 4.0
    min_density: float = 0.4
    merge_query_gap_seconds: float = 1.0
    merge_offset_tolerance_seconds: float = 4.0
    max_segments: int = 12

    def __post_init__(self) -> None:
        positive = {
            "top_k": self.top_k,
            "hop_seconds": self.hop_seconds,
            "offset_bin_seconds": self.offset_bin_seconds,
            "offset_tolerance_seconds": self.offset_tolerance_seconds,
            "min_support": self.min_support,
            "min_duration_seconds": self.min_duration_seconds,
            "max_segments": self.max_segments,
        }
        for name, value in positive.items():
            if value <= 0:
                raise ValueError(f"{name} must be positive")
        if self.max_unmatched_gap_seconds < 0 or self.merge_query_gap_seconds < 0:
            raise ValueError("Gap tolerances cannot be negative")
        if not 0 < self.min_density <= 1:
            raise ValueError("min_density must be in (0, 1]")


@dataclass(frozen=True)
class FingerprintCandidate:
    query_index: int
    query_time: float
    database_index: int
    vod_time: float
    offset: float
    similarity: float
    rank: int


@dataclass(frozen=True)
class AlignmentSegment:
    query_start: float
    query_end: float
    vod_start: float
    vod_end: float
    offset_seconds: float
    confidence: float
    mean_similarity: float
    density: float
    supporting_fingerprints: int
    top_rank_fingerprints: int
    score: float

    @property
    def duration_seconds(self) -> float:
        return self.query_end - self.query_start

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CutSearchResult:
    engine: str
    query_id: str
    query_duration_seconds: float
    primary_vod_start_seconds: float | None
    segments: list[AlignmentSegment]
    unmatched_ranges: list[dict[str, float]]
    search_duration_ms: int
    settings: CutDetectionSettings
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["segments"] = [segment.to_dict() for segment in self.segments]
        value["settings"] = asdict(self.settings)
        return value


def _normalize_rows(values: np.ndarray) -> np.ndarray:
    values = values.astype(np.float32, copy=False)
    norms = np.linalg.norm(values, axis=1, keepdims=True)
    return values / np.maximum(norms, 1e-10)


def top_k_fingerprint_candidates(
    query_embeddings: np.ndarray,
    query_timestamps: np.ndarray,
    database_embeddings: np.ndarray,
    database_timestamps: np.ndarray,
    *,
    top_k: int,
) -> list[FingerprintCandidate]:
    """Return ranked VOD candidates for every query fingerprint."""

    if query_embeddings.size == 0 or database_embeddings.size == 0:
        return []
    if len(query_embeddings) != len(query_timestamps):
        raise ValueError("Query embeddings and timestamps are misaligned")
    if len(database_embeddings) != len(database_timestamps):
        raise ValueError("Database embeddings and timestamps are misaligned")

    query = _normalize_rows(query_embeddings)
    database = _normalize_rows(database_embeddings)
    k = min(max(1, int(top_k)), len(database))
    similarities = np.dot(query, database.T)
    unordered = np.argpartition(similarities, -k, axis=1)[:, -k:]
    unordered_scores = np.take_along_axis(similarities, unordered, axis=1)
    order = np.argsort(unordered_scores, axis=1)[:, ::-1]
    candidate_indices = np.take_along_axis(unordered, order, axis=1)
    candidate_scores = np.take_along_axis(unordered_scores, order, axis=1)

    candidates: list[FingerprintCandidate] = []
    for query_index, query_time in enumerate(query_timestamps):
        for rank, (database_index, similarity) in enumerate(
            zip(candidate_indices[query_index], candidate_scores[query_index])
        ):
            vod_time = float(database_timestamps[database_index])
            candidates.append(
                FingerprintCandidate(
                    query_index=query_index,
                    query_time=float(query_time),
                    database_index=int(database_index),
                    vod_time=vod_time,
                    offset=vod_time - float(query_time),
                    similarity=float(similarity),
                    rank=rank,
                )
            )
    return candidates


def _candidate_runs(
    candidates: Iterable[FingerprintCandidate],
    settings: CutDetectionSettings,
) -> list[list[FingerprintCandidate]]:
    candidates = list(candidates)
    if not candidates:
        return []

    bucket_size = settings.offset_bin_seconds
    buckets = sorted({round(candidate.offset / bucket_size) * bucket_size for candidate in candidates})
    runs: list[list[FingerprintCandidate]] = []
    maximum_step = settings.hop_seconds + settings.max_unmatched_gap_seconds + 1e-6

    for center in buckets:
        best_by_query: dict[int, FingerprintCandidate] = {}
        for candidate in candidates:
            if abs(candidate.offset - center) > settings.offset_tolerance_seconds:
                continue
            current = best_by_query.get(candidate.query_index)
            if current is None or (candidate.similarity, -candidate.rank) > (current.similarity, -current.rank):
                best_by_query[candidate.query_index] = candidate
        ordered = sorted(best_by_query.values(), key=lambda candidate: candidate.query_time)
        run: list[FingerprintCandidate] = []
        for candidate in ordered:
            if run and candidate.query_time - run[-1].query_time > maximum_step:
                runs.append(run)
                run = []
            run.append(candidate)
        if run:
            runs.append(run)
    return runs


def _segment_from_run(
    run: list[FingerprintCandidate],
    settings: CutDetectionSettings,
) -> AlignmentSegment | None:
    if len(run) < settings.min_support:
        return None
    median_offset = float(np.median([candidate.offset for candidate in run]))
    refined = [
        candidate
        for candidate in run
        if abs(candidate.offset - median_offset) <= settings.offset_tolerance_seconds
    ]
    if len(refined) < settings.min_support:
        return None

    query_start = refined[0].query_time
    query_end = refined[-1].query_time + settings.hop_seconds
    duration = query_end - query_start
    if duration + 1e-6 < settings.min_duration_seconds:
        return None
    expected_count = max(1, round(duration / settings.hop_seconds))
    density = min(1.0, len(refined) / expected_count)
    if density + 1e-9 < settings.min_density:
        return None

    similarities = [candidate.similarity for candidate in refined]
    reciprocal_ranks = [1.0 / (candidate.rank + 1) for candidate in refined]
    mean_similarity = float(np.mean(similarities))
    mean_reciprocal_rank = float(np.mean(reciprocal_ranks))
    top_rank = sum(candidate.rank == 0 for candidate in refined)
    confidence = max(0.0, min(1.0, density * mean_similarity * mean_reciprocal_rank))
    score = len(refined) * density * mean_similarity * mean_reciprocal_rank
    return AlignmentSegment(
        query_start=query_start,
        query_end=query_end,
        vod_start=query_start + median_offset,
        vod_end=query_end + median_offset,
        offset_seconds=median_offset,
        confidence=confidence,
        mean_similarity=mean_similarity,
        density=density,
        supporting_fingerprints=len(refined),
        top_rank_fingerprints=top_rank,
        score=score,
    )


def _intersection_seconds(left: AlignmentSegment, right: AlignmentSegment) -> float:
    return max(0.0, min(left.query_end, right.query_end) - max(left.query_start, right.query_start))


def _deduplicate_segments(
    segments: Iterable[AlignmentSegment],
    settings: CutDetectionSettings,
) -> list[AlignmentSegment]:
    accepted: list[AlignmentSegment] = []
    ranked = sorted(
        segments,
        key=lambda segment: (segment.score, segment.duration_seconds, segment.supporting_fingerprints),
        reverse=True,
    )
    for segment in ranked:
        conflicts = False
        for existing in accepted:
            intersection = _intersection_seconds(segment, existing)
            shorter = min(segment.duration_seconds, existing.duration_seconds)
            if shorter > 0 and intersection / shorter >= 0.5:
                conflicts = True
                break
        if not conflicts:
            accepted.append(segment)
        if len(accepted) >= settings.max_segments:
            break
    return sorted(accepted, key=lambda segment: segment.query_start)


def _merge_segments(
    segments: Iterable[AlignmentSegment],
    settings: CutDetectionSettings,
) -> list[AlignmentSegment]:
    merged: list[AlignmentSegment] = []
    for segment in segments:
        if not merged:
            merged.append(segment)
            continue
        previous = merged[-1]
        query_gap = segment.query_start - previous.query_end
        if (
            query_gap <= settings.merge_query_gap_seconds
            and abs(segment.offset_seconds - previous.offset_seconds)
            <= settings.merge_offset_tolerance_seconds
        ):
            total_support = previous.supporting_fingerprints + segment.supporting_fingerprints
            offset = (
                previous.offset_seconds * previous.supporting_fingerprints
                + segment.offset_seconds * segment.supporting_fingerprints
            ) / total_support
            query_start = min(previous.query_start, segment.query_start)
            query_end = max(previous.query_end, segment.query_end)
            mean_similarity = (
                previous.mean_similarity * previous.supporting_fingerprints
                + segment.mean_similarity * segment.supporting_fingerprints
            ) / total_support
            density = min(
                1.0,
                total_support / max(1, round((query_end - query_start) / settings.hop_seconds)),
            )
            confidence = max(previous.confidence, segment.confidence)
            merged[-1] = AlignmentSegment(
                query_start=query_start,
                query_end=query_end,
                vod_start=query_start + offset,
                vod_end=query_end + offset,
                offset_seconds=offset,
                confidence=confidence,
                mean_similarity=mean_similarity,
                density=density,
                supporting_fingerprints=total_support,
                top_rank_fingerprints=previous.top_rank_fingerprints + segment.top_rank_fingerprints,
                score=previous.score + segment.score,
            )
        else:
            merged.append(segment)
    return merged


def build_alignment_segments(
    candidates: Iterable[FingerprintCandidate],
    settings: CutDetectionSettings,
) -> list[AlignmentSegment]:
    """Find non-overlapping, continuous NMFP alignment tracks."""

    proposed = [
        segment
        for run in _candidate_runs(candidates, settings)
        if (segment := _segment_from_run(run, settings)) is not None
    ]
    return _merge_segments(_deduplicate_segments(proposed, settings), settings)


def unmatched_query_ranges(
    segments: Iterable[AlignmentSegment],
    query_duration_seconds: float,
) -> list[dict[str, float]]:
    ranges: list[dict[str, float]] = []
    cursor = 0.0
    for segment in sorted(segments, key=lambda item: item.query_start):
        if segment.query_start > cursor + 1e-6:
            ranges.append({"query_start": cursor, "query_end": segment.query_start})
        cursor = max(cursor, segment.query_end)
    if query_duration_seconds > cursor + 1e-6:
        ranges.append({"query_start": cursor, "query_end": query_duration_seconds})
    return ranges


def cut_aware_vector_alignment(
    query_id: str,
    query_embeddings: np.ndarray,
    query_timestamps: np.ndarray,
    database_embeddings: np.ndarray,
    database_timestamps: np.ndarray,
    *,
    query_duration_seconds: float,
    settings: CutDetectionSettings,
) -> CutSearchResult:
    started_at = time.perf_counter()
    candidates = top_k_fingerprint_candidates(
        query_embeddings,
        query_timestamps,
        database_embeddings,
        database_timestamps,
        top_k=settings.top_k,
    )
    segments = build_alignment_segments(candidates, settings)
    primary = max(segments, key=lambda segment: segment.score, default=None)
    return CutSearchResult(
        engine="nmfp_triplet",
        query_id=query_id,
        query_duration_seconds=query_duration_seconds,
        primary_vod_start_seconds=primary.vod_start if primary else None,
        segments=segments,
        unmatched_ranges=unmatched_query_ranges(segments, query_duration_seconds),
        search_duration_ms=round((time.perf_counter() - started_at) * 1000),
        settings=settings,
        diagnostics={
            "query_fingerprints": len(query_embeddings),
            "candidate_count": len(candidates),
            "matched_fingerprints": sum(segment.supporting_fingerprints for segment in segments),
        },
    )
