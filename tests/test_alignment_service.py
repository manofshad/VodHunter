import numpy as np
import pytest

from search.alignment_service import (
    AlignmentConfig,
    AlignmentService,
    build_alignment_segments,
    unmatched_query_ranges,
)
from search.models import FingerprintCandidate, UnmatchedRange


def _candidate(
    query_time: float,
    offset: float,
    *,
    video_id: int = 1,
    similarity: float = 0.9,
    rank: int = 0,
    fingerprint_id: int | None = None,
) -> FingerprintCandidate:
    query_index = round(query_time / 0.5)
    return FingerprintCandidate(
        query_index=query_index,
        query_time=query_time,
        fingerprint_id=(
            fingerprint_id
            if fingerprint_id is not None
            else video_id * 100_000 + round((query_time + offset) * 2)
        ),
        video_id=video_id,
        vod_time=query_time + offset,
        similarity=similarity,
        rank=rank,
    )


def _track(
    query_start: float,
    count: int,
    offset: float,
    *,
    video_id: int = 1,
    similarity: float = 0.9,
    rank: int = 0,
) -> list[FingerprintCandidate]:
    return [
        _candidate(
            query_start + index * 0.5,
            offset,
            video_id=video_id,
            similarity=similarity,
            rank=rank,
        )
        for index in range(count)
    ]


def test_alignment_defaults_match_tuned_experiment() -> None:
    config = AlignmentConfig()

    assert config.top_k == 10
    assert config.fingerprint_hop_seconds == 0.5
    assert config.offset_bin_seconds == 0.5
    assert config.offset_tolerance_seconds == 1.0
    assert config.max_unmatched_gap_seconds == 2.0
    assert config.min_support == 6
    assert config.min_segment_duration_seconds == 4.0
    assert config.min_density == 0.4
    assert config.merge_query_gap_seconds == 1.0
    assert config.merge_offset_tolerance_seconds == 4.0
    assert config.max_segments == 12


def test_fingerprint_candidate_computes_offset() -> None:
    candidate = _candidate(7.5, 100.25)

    assert candidate.offset == pytest.approx(100.25)


def test_cut_alignment_keeps_track_rejects_isolated_candidate_and_returns_unmatched() -> None:
    service = AlignmentService(config=AlignmentConfig())
    candidates = _track(0.0, 20, 100.0)
    candidates.append(_candidate(4.5, 500.0, video_id=2, similarity=0.99))

    result = service.align_candidates(candidates, query_duration_seconds=12.0)

    assert result.found
    assert result.video_id == 1
    assert result.timestamp_seconds == 100
    assert len(result.segments) == 1
    segment = result.segments[0]
    assert (segment.query_start, segment.query_end) == (0.0, 10.0)
    assert (segment.vod_start, segment.vod_end) == (100.0, 110.0)
    assert segment.supporting_fingerprints == 20
    assert result.unmatched_ranges == [UnmatchedRange(10.0, 12.0)]


def test_tracks_are_scoped_by_video_before_minimum_support() -> None:
    config = AlignmentConfig(min_support=6, min_segment_duration_seconds=3.0)
    candidates = _track(0.0, 3, 100.0, video_id=1)
    candidates += _track(1.5, 3, 100.0, video_id=2)

    result = AlignmentService(config=config).align_candidates(
        candidates,
        query_duration_seconds=3.0,
    )

    assert not result.found
    assert result.segments == []
    assert result.unmatched_ranges == [UnmatchedRange(0.0, 3.0)]


def test_nearby_tracks_merge_within_one_video() -> None:
    config = AlignmentConfig(
        min_support=6,
        min_segment_duration_seconds=3.0,
        offset_tolerance_seconds=0.1,
        max_unmatched_gap_seconds=0.0,
    )
    candidates = _track(0.0, 6, 100.0, video_id=1)
    candidates += _track(3.5, 6, 102.0, video_id=1)

    segments = build_alignment_segments(candidates, config)

    assert len(segments) == 1
    assert segments[0].video_id == 1
    assert segments[0].query_start == 0.0
    assert segments[0].query_end == 6.5
    assert segments[0].offset_seconds == pytest.approx(101.0)
    assert segments[0].supporting_fingerprints == 12


def test_nearby_tracks_never_merge_across_videos() -> None:
    config = AlignmentConfig(
        min_support=6,
        min_segment_duration_seconds=3.0,
        offset_tolerance_seconds=0.1,
        max_unmatched_gap_seconds=0.0,
    )
    candidates = _track(0.0, 6, 100.0, video_id=1)
    candidates += _track(3.5, 6, 102.0, video_id=2)

    segments = build_alignment_segments(candidates, config)

    assert [(segment.video_id, segment.query_start) for segment in segments] == [
        (1, 0.0),
        (2, 3.5),
    ]


def test_support_duration_density_and_gap_thresholds_reject_weak_tracks() -> None:
    assert build_alignment_segments(
        _track(0.0, 5, 100.0),
        AlignmentConfig(min_support=6, min_segment_duration_seconds=1.0),
    ) == []

    assert build_alignment_segments(
        _track(0.0, 6, 100.0),
        AlignmentConfig(min_support=4, min_segment_duration_seconds=4.0),
    ) == []

    sparse = [
        _candidate(0.0, 100.0),
        _candidate(0.5, 100.0),
        _candidate(4.0, 100.0),
        _candidate(4.5, 100.0),
    ]
    assert build_alignment_segments(
        sparse,
        AlignmentConfig(
            min_support=4,
            min_segment_duration_seconds=1.0,
            min_density=0.5,
            max_unmatched_gap_seconds=10.0,
        ),
    ) == []

    gapped = [
        _candidate(0.0, 100.0),
        _candidate(0.5, 100.0),
        _candidate(2.0, 100.0),
        _candidate(2.5, 100.0),
    ]
    assert build_alignment_segments(
        gapped,
        AlignmentConfig(
            min_support=4,
            min_segment_duration_seconds=1.0,
            max_unmatched_gap_seconds=0.5,
        ),
    ) == []


def test_max_segments_keeps_highest_ranked_tracks_then_orders_by_query() -> None:
    config = AlignmentConfig(
        min_support=6,
        min_segment_duration_seconds=3.0,
        max_unmatched_gap_seconds=0.0,
        max_segments=2,
    )
    candidates = _track(0.0, 6, 100.0, video_id=1, similarity=0.8)
    candidates += _track(4.0, 6, 200.0, video_id=2, similarity=0.9)
    candidates += _track(8.0, 6, 300.0, video_id=3, similarity=0.7)

    segments = build_alignment_segments(candidates, config)

    assert [segment.video_id for segment in segments] == [1, 2]


def test_primary_uses_unbounded_ranking_score_not_bounded_confidence() -> None:
    config = AlignmentConfig(
        min_support=6,
        min_segment_duration_seconds=3.0,
        max_unmatched_gap_seconds=0.0,
    )
    # The shorter track has higher bounded confidence, but the longer rank-1
    # track has more total evidence and therefore the larger ranking score.
    candidates = _track(0.0, 8, 100.0, video_id=1, similarity=0.6, rank=0)
    candidates += _track(5.0, 12, 200.0, video_id=2, similarity=1.0, rank=1)

    result = AlignmentService(config=config).align_candidates(
        candidates,
        query_duration_seconds=12.0,
    )

    assert result.found
    assert result.video_id == 2
    assert result.score == pytest.approx(0.5)
    assert result.ranking_score == pytest.approx(6.0)
    assert all(0.0 <= segment.score <= 1.0 for segment in result.segments)
    assert max(segment.ranking_score for segment in result.segments) > 1.0


def test_unmatched_ranges_cover_internal_leading_and_trailing_gaps() -> None:
    config = AlignmentConfig(
        min_support=6,
        min_segment_duration_seconds=3.0,
        max_unmatched_gap_seconds=0.0,
    )
    candidates = _track(1.0, 6, 100.0, video_id=1)
    candidates += _track(6.0, 6, 200.0, video_id=2)
    segments = build_alignment_segments(candidates, config)

    assert unmatched_query_ranges(segments, 10.0) == [
        UnmatchedRange(0.0, 1.0),
        UnmatchedRange(4.0, 6.0),
        UnmatchedRange(9.0, 10.0),
    ]


def test_no_candidates_returns_fully_unmatched_duration() -> None:
    result = AlignmentService().align_candidates([], query_duration_seconds=7.25)

    assert not result.found
    assert result.segments == []
    assert result.unmatched_ranges == [UnmatchedRange(0.0, 7.25)]

    zero_duration = AlignmentService().align_candidates([], query_duration_seconds=0.0)
    assert zero_duration.unmatched_ranges == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("top_k", 0),
        ("fingerprint_hop_seconds", 0.0),
        ("offset_bin_seconds", 0.0),
        ("offset_tolerance_seconds", 0.0),
        ("max_unmatched_gap_seconds", -0.1),
        ("min_support", 0),
        ("min_segment_duration_seconds", 0.0),
        ("min_density", 0.0),
        ("min_density", 1.1),
        ("merge_query_gap_seconds", -0.1),
        ("merge_offset_tolerance_seconds", -0.1),
        ("max_segments", 0),
    ],
)
def test_alignment_config_rejects_invalid_thresholds(field: str, value: float) -> None:
    with pytest.raises(ValueError):
        AlignmentConfig(**{field: value})


@pytest.mark.parametrize("duration", [-1.0, float("nan"), float("inf")])
def test_align_candidates_rejects_invalid_duration(duration: float) -> None:
    with pytest.raises(ValueError):
        AlignmentService().align_candidates([], query_duration_seconds=duration)
