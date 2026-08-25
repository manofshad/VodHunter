from search.models import SearchResult, SearchSegment, UnmatchedRange


def test_search_result_round_trips_nested_cut_results() -> None:
    result = SearchResult(
        found=True,
        streamer="jason",
        video_id=42,
        timestamp_seconds=110,
        score=0.82,
        segments=[
            SearchSegment(
                query_start=10.0,
                query_end=15.0,
                video_id=42,
                vod_start=110.0,
                vod_end=115.0,
                score=0.82,
                ranking_score=8.2,
                offset_seconds=100.0,
                mean_similarity=0.91,
                density=1.0,
                supporting_fingerprints=10,
                top_rank_fingerprints=8,
                video_url_at_timestamp="https://example.test/video?t=1m50s",
            )
        ],
        unmatched_ranges=[UnmatchedRange(query_start=0.0, query_end=10.0)],
        query_duration_seconds=15.0,
    )

    serialized = result.to_dict()
    restored = SearchResult.from_dict(serialized)

    assert restored == result
    assert isinstance(restored.segments[0], SearchSegment)
    assert isinstance(restored.unmatched_ranges[0], UnmatchedRange)
    assert serialized["segments"][0]["ranking_score"] == 8.2


def test_search_result_from_legacy_dict_defaults_nested_ranges() -> None:
    restored = SearchResult.from_dict(
        {
            "found": False,
            "streamer": "jason",
            "reason": "No match",
        }
    )

    assert restored.segments == []
    assert restored.unmatched_ranges == []
    assert restored.query_duration_seconds is None
