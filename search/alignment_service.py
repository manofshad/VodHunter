from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Iterable

import numpy as np

from search.models import (
    AlignmentResult,
    FingerprintCandidate,
    SearchSegment,
    UnmatchedRange,
)

@dataclass(frozen=True)
class AlignmentConfig:
    """Tunable rules for turning ranked NMFP neighbors into cut-aware tracks."""

    top_k: int = 10
    fingerprint_hop_seconds: float = 0.5
    offset_bin_seconds: float = 0.5
    offset_tolerance_seconds: float = 1.0
    max_unmatched_gap_seconds: float = 2.0
    min_support: int = 6
    min_segment_duration_seconds: float = 4.0
    min_density: float = 0.4
    min_score: float = 0.10
    merge_query_gap_seconds: float = 1.0
    merge_offset_tolerance_seconds: float = 4.0
    max_segments: int = 12

    def __post_init__(self) -> None:
        positive = {
            "top_k": self.top_k,
            "fingerprint_hop_seconds": self.fingerprint_hop_seconds,
            "offset_bin_seconds": self.offset_bin_seconds,
            "offset_tolerance_seconds": self.offset_tolerance_seconds,
            "min_support": self.min_support,
            "min_segment_duration_seconds": self.min_segment_duration_seconds,
            "max_segments": self.max_segments,
        }
        for name, value in positive.items():
            if not math.isfinite(float(value)) or value <= 0:
                raise ValueError(f"{name} must be positive")

        non_negative = {
            "max_unmatched_gap_seconds": self.max_unmatched_gap_seconds,
            "merge_query_gap_seconds": self.merge_query_gap_seconds,
            "merge_offset_tolerance_seconds": self.merge_offset_tolerance_seconds,
        }
        for name, value in non_negative.items():
            if not math.isfinite(float(value)) or value < 0:
                raise ValueError(f"{name} cannot be negative")

        if not math.isfinite(self.min_density) or not 0 < self.min_density <= 1:
            raise ValueError("min_density must be in (0, 1]")

        if not math.isfinite(self.min_score) or not 0 < self.min_score <= 1:
            raise ValueError("min_score must be in (0, 1]")


def _candidate_runs(
    candidates: Iterable[FingerprintCandidate],
    config: AlignmentConfig,
) -> list[list[FingerprintCandidate]]:
    candidate_list = list(candidates)
    if not candidate_list:
        return []

    bucket_size = config.offset_bin_seconds
    buckets = sorted(
        {
            (
                candidate.video_id,
                round(candidate.offset / bucket_size) * bucket_size,
            )
            for candidate in candidate_list
        }
    )
    runs: list[list[FingerprintCandidate]] = []
    maximum_step = (
        config.fingerprint_hop_seconds + config.max_unmatched_gap_seconds + 1e-6
    )

    for video_id, center in buckets:
        best_by_query: dict[int, FingerprintCandidate] = {}
        for candidate in candidate_list:
            if candidate.video_id != video_id:
                continue
            if abs(candidate.offset - center) > config.offset_tolerance_seconds:
                continue
            current = best_by_query.get(candidate.query_index)
            if current is None or (
                candidate.similarity,
                -candidate.rank,
                -candidate.fingerprint_id,
            ) > (
                current.similarity,
                -current.rank,
                -current.fingerprint_id,
            ):
                best_by_query[candidate.query_index] = candidate

        ordered = sorted(
            best_by_query.values(),
            key=lambda candidate: (candidate.query_time, candidate.query_index),
        )
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
    config: AlignmentConfig,
) -> SearchSegment | None:
    if len(run) < config.min_support:
        return None

    median_offset = float(np.median([candidate.offset for candidate in run]))
    refined = [
        candidate
        for candidate in run
        if abs(candidate.offset - median_offset) <= config.offset_tolerance_seconds
    ]
    if len(refined) < config.min_support:
        return None

    refined.sort(key=lambda candidate: (candidate.query_time, candidate.query_index))
    query_start = refined[0].query_time
    query_end = refined[-1].query_time + config.fingerprint_hop_seconds
    duration = query_end - query_start
    if duration + 1e-6 < config.min_segment_duration_seconds:
        return None

    expected_count = max(1, round(duration / config.fingerprint_hop_seconds))
    density = min(1.0, len(refined) / expected_count)
    if density + 1e-9 < config.min_density:
        return None

    similarities = [candidate.similarity for candidate in refined]
    reciprocal_ranks = [1.0 / (candidate.rank + 1) for candidate in refined]
    mean_similarity = float(np.mean(similarities))
    mean_reciprocal_rank = float(np.mean(reciprocal_ranks))
    top_rank = sum(candidate.rank == 0 for candidate in refined)

    # API confidence stays bounded. The separate ranking score intentionally
    # grows with support and is used only to choose among supported tracks.
    confidence = max(
        0.0,
        min(1.0, density * mean_similarity * mean_reciprocal_rank),
    )
    ranking_score = len(refined) * density * mean_similarity * mean_reciprocal_rank
    video_id = refined[0].video_id
    return SearchSegment(
        query_start=query_start,
        query_end=query_end,
        video_id=video_id,
        vod_start=query_start + median_offset,
        vod_end=query_end + median_offset,
        score=confidence,
        ranking_score=ranking_score,
        offset_seconds=median_offset,
        mean_similarity=mean_similarity,
        density=density,
        supporting_fingerprints=len(refined),
        top_rank_fingerprints=top_rank,
    )


def _intersection_seconds(left: SearchSegment, right: SearchSegment) -> float:
    return max(
        0.0,
        min(left.query_end, right.query_end) - max(left.query_start, right.query_start),
    )


def _deduplicate_segments(
    segments: Iterable[SearchSegment],
    config: AlignmentConfig,
) -> list[SearchSegment]:
    accepted: list[SearchSegment] = []
    ranked = sorted(
        segments,
        key=lambda segment: (
            segment.ranking_score,
            segment.duration_seconds,
            segment.supporting_fingerprints,
            -segment.video_id,
        ),
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
        if len(accepted) >= config.max_segments:
            break
    return sorted(accepted, key=lambda segment: (segment.query_start, segment.video_id))


def _merge_segments(
    segments: Iterable[SearchSegment],
    config: AlignmentConfig,
) -> list[SearchSegment]:
    merged: list[SearchSegment] = []
    for segment in segments:
        if not merged:
            merged.append(segment)
            continue

        previous = merged[-1]
        query_gap = segment.query_start - previous.query_end
        if (
            segment.video_id == previous.video_id
            and query_gap <= config.merge_query_gap_seconds
            and abs(segment.offset_seconds - previous.offset_seconds)
            <= config.merge_offset_tolerance_seconds
        ):
            total_support = (
                previous.supporting_fingerprints + segment.supporting_fingerprints
            )
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
                total_support
                / max(
                    1,
                    round(
                        (query_end - query_start)
                        / config.fingerprint_hop_seconds
                    ),
                ),
            )
            merged[-1] = SearchSegment(
                query_start=query_start,
                query_end=query_end,
                video_id=previous.video_id,
                vod_start=query_start + offset,
                vod_end=query_end + offset,
                score=max(previous.score, segment.score),
                ranking_score=previous.ranking_score + segment.ranking_score,
                offset_seconds=offset,
                mean_similarity=mean_similarity,
                density=density,
                supporting_fingerprints=total_support,
                top_rank_fingerprints=(
                    previous.top_rank_fingerprints + segment.top_rank_fingerprints
                ),
            )
        else:
            merged.append(segment)
    return merged


def build_alignment_segments(
    candidates: Iterable[FingerprintCandidate],
    config: AlignmentConfig,
) -> list[SearchSegment]:
    """Build non-overlapping, supported tracks scoped by video and offset."""

    proposed = [
        segment
        for run in _candidate_runs(candidates, config)
        if (segment := _segment_from_run(run, config)) is not None
    ]
    scored = [segment for segment in proposed if segment.score >= config.min_score]
    return _merge_segments(_deduplicate_segments(scored, config), config)


def unmatched_query_ranges(
    segments: Iterable[SearchSegment],
    query_duration_seconds: float,
) -> list[UnmatchedRange]:
    if not math.isfinite(query_duration_seconds) or query_duration_seconds < 0:
        raise ValueError("query_duration_seconds must be finite and non-negative")

    ranges: list[UnmatchedRange] = []
    cursor = 0.0
    for segment in sorted(segments, key=lambda item: item.query_start):
        segment_start = min(query_duration_seconds, max(0.0, segment.query_start))
        segment_end = min(query_duration_seconds, max(segment_start, segment.query_end))
        if segment_start > cursor + 1e-6:
            ranges.append(UnmatchedRange(query_start=cursor, query_end=segment_start))
        cursor = max(cursor, segment_end)
    if query_duration_seconds > cursor + 1e-6:
        ranges.append(
            UnmatchedRange(
                query_start=cursor,
                query_end=query_duration_seconds,
            )
        )
    return ranges


class AlignmentService:
    def __init__(
        self,
        config: AlignmentConfig | None = None,
    ):
        self.config = config or AlignmentConfig()

    def align_candidates(
        self,
        candidates: Iterable[FingerprintCandidate],
        *,
        query_duration_seconds: float,
    ) -> AlignmentResult:
        """Align ranked NMFP candidates into multiple cut-aware VOD segments."""

        if not math.isfinite(query_duration_seconds) or query_duration_seconds < 0:
            raise ValueError("query_duration_seconds must be finite and non-negative")

        candidate_list = list(candidates)
        segments = build_alignment_segments(candidate_list, self.config)
        unmatched = unmatched_query_ranges(segments, query_duration_seconds)
        primary = max(
            segments,
            key=lambda segment: (
                segment.ranking_score,
                segment.duration_seconds,
                segment.supporting_fingerprints,
            ),
            default=None,
        )
        if primary is None:
            return AlignmentResult(
                found=False,
                reason=(
                    "No candidate track met the configured support, duration, and "
                    "density thresholds and minimum score"
                    if candidate_list
                    else "No fingerprint candidates"
                ),
                segments=[],
                unmatched_ranges=unmatched,
                query_duration_seconds=query_duration_seconds,
            )

        return AlignmentResult(
            found=True,
            video_id=primary.video_id,
            timestamp_seconds=int(round(primary.vod_start)),
            score=primary.score,
            ranking_score=primary.ranking_score,
            reason=(
                f"Accepted {len(segments)} supported segment"
                f"{'s' if len(segments) != 1 else ''}; primary has "
                f"{primary.supporting_fingerprints} fingerprints"
            ),
            segments=segments,
            unmatched_ranges=unmatched,
            query_duration_seconds=query_duration_seconds,
        )
