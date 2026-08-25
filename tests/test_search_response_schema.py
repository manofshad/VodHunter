from backend.schemas import SearchResponse
from search.models import SearchResult, SearchSegment, UnmatchedRange


def test_search_response_maps_multi_video_segments_and_primary_compatibility() -> None:
    result = SearchResult(
        found=True,
        streamer="jason",
        video_id=7,
        video_url="https://www.twitch.tv/videos/7",
        video_url_at_timestamp="https://www.twitch.tv/videos/7?t=1m40s",
        timestamp_seconds=100,
        score=0.91,
        reason="Accepted 2 supported segments",
        query_duration_seconds=16.0,
        segments=[
            SearchSegment(
                query_start=0.0,
                query_end=5.0,
                video_id=7,
                vod_start=100.0,
                vod_end=105.0,
                score=0.91,
                ranking_score=8.0,
                offset_seconds=100.0,
                mean_similarity=0.94,
                density=1.0,
                supporting_fingerprints=10,
                top_rank_fingerprints=8,
                video_url_at_timestamp="https://www.twitch.tv/videos/7?t=1m40s",
            ),
            SearchSegment(
                query_start=8.0,
                query_end=14.0,
                video_id=8,
                vod_start=500.0,
                vod_end=506.0,
                score=0.87,
                ranking_score=9.0,
                offset_seconds=492.0,
                mean_similarity=0.90,
                density=0.9,
                supporting_fingerprints=11,
                top_rank_fingerprints=7,
                video_url_at_timestamp="https://www.twitch.tv/videos/8?t=8m20s",
            ),
        ],
        unmatched_ranges=[
            UnmatchedRange(query_start=5.0, query_end=8.0),
            UnmatchedRange(query_start=14.0, query_end=16.0),
        ],
    )

    response = SearchResponse.from_result(result).model_dump()

    assert response["video_id"] == 7
    assert response["timestamp_seconds"] == 100
    assert response["video_url_at_timestamp"] == "https://www.twitch.tv/videos/7?t=1m40s"
    assert response["score"] == 0.91
    assert response["segments"] == [
        {
            "query_start": 0.0,
            "query_end": 5.0,
            "video_id": 7,
            "vod_start": 100.0,
            "vod_end": 105.0,
            "video_url_at_timestamp": "https://www.twitch.tv/videos/7?t=1m40s",
            "score": 0.91,
        },
        {
            "query_start": 8.0,
            "query_end": 14.0,
            "video_id": 8,
            "vod_start": 500.0,
            "vod_end": 506.0,
            "video_url_at_timestamp": "https://www.twitch.tv/videos/8?t=8m20s",
            "score": 0.87,
        },
    ]
    assert response["unmatched_ranges"] == [
        {"query_start": 5.0, "query_end": 8.0},
        {"query_start": 14.0, "query_end": 16.0},
    ]
    assert response["query_duration_seconds"] == 16.0
