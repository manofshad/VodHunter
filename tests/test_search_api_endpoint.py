from datetime import datetime, timezone

from fastapi.testclient import TestClient

from backend.apps.public import create_public_app
from search.models import (
    SearchDateRange,
    SearchExecutionMetadata,
    SearchJobRecord,
    SearchRequestOutcome,
    SearchResult,
)
from storage.records import SearchableStreamer


class StubSearchManager:
    def __init__(self):
        self.url_calls = 0
        self.last_streamer: str | None = None
        self.last_date_range: SearchDateRange | None = None
        self.raise_url: Exception | None = None

    def search_tiktok_url(
        self,
        url: str,
        streamer: str,
        date_range: SearchDateRange | None = None,
        on_stage_change=None,
    ) -> SearchRequestOutcome:
        self.url_calls += 1
        self.last_streamer = streamer
        self.last_date_range = date_range
        if self.raise_url is not None:
            raise self.raise_url
        return SearchRequestOutcome(
            result=SearchResult(
                found=False,
                streamer=streamer,
                profile_image_url="https://cdn/profile.png",
                reason="url test",
                thumbnail_url=None,
                video_url_at_timestamp=None,
            ),
            execution_metadata=SearchExecutionMetadata(result_reason="url test", found_match=False),
            input_type="tiktok_url",
            download_source="tiktok",
            download_host="www.tiktok.com",
            date_range=date_range,
        )


class StubStore:
    def __init__(self, streamers: list[SearchableStreamer] | None = None):
        self.streamers = streamers or [
            SearchableStreamer(name="xqc", profile_image_url="https://cdn/xqc.png"),
            SearchableStreamer(name="jason", profile_image_url=None),
        ]
    def list_searchable_streamers(self) -> list[SearchableStreamer]:
        return list(self.streamers)

    def get_creator_id_by_name(self, name: str) -> int | None:
        normalized_name = (name or "").strip().lower()
        mapping = {"xqc": 1, "jason": 2}
        return mapping.get(normalized_name)

class StubSearchJobService:
    def __init__(self):
        self.created_jobs: list[dict[str, object]] = []
        self.jobs: dict[int, SearchJobRecord] = {}

    def create_public_search_job(
        self,
        *,
        tiktok_url: str,
        streamer: str,
        creator_id: int | None,
        date_range: SearchDateRange | None = None,
    ) -> int:
        self.created_jobs.append(
            {
                "tiktok_url": tiktok_url,
                "streamer": streamer,
                "creator_id": creator_id,
                "date_range": date_range,
            }
        )
        return 101

    def get_public_search_job(self, search_id: int) -> SearchJobRecord | None:
        return self.jobs.get(search_id)


def build_client(app_factory):
    app = app_factory(enable_lifespan=False)
    app.state.videos = StubStore()
    app.state.search_manager = StubSearchManager()
    app.state.search_job_service = StubSearchJobService()
    return app, TestClient(app)


def test_searchable_streamers_are_browser_cacheable() -> None:
    _, client = build_client(create_public_app)

    with client:
        response = client.get("/api/search/streamers")

    assert response.status_code == 200
    assert response.headers["cache-control"] == (
        "public, max-age=300, stale-while-revalidate=3600"
    )
    assert response.json() == [
        {"name": "xqc", "profile_image_url": "https://cdn/xqc.png"},
        {"name": "jason", "profile_image_url": None},
    ]


def test_public_search_endpoint_accepts_tiktok_url_only() -> None:
    app, client = build_client(create_public_app)

    with client:
        response = client.post(
            "/api/search/clip",
            data={"tiktok_url": "https://www.tiktok.com/@u/video/1", "streamer": "jason"},
        )

    assert response.status_code == 202
    assert response.json() == {"search_id": 101, "status": "queued", "stage": "validating"}
    assert app.state.search_manager.url_calls == 0
    assert app.state.search_job_service.created_jobs == [
        {
            "tiktok_url": "https://www.tiktok.com/@u/video/1",
            "streamer": "jason",
            "creator_id": 2,
            "date_range": None,
        }
    ]


def test_public_search_endpoint_accepts_tiktok_short_share_url() -> None:
    app, client = build_client(create_public_app)

    with client:
        response = client.post(
            "/api/search/clip",
            data={"tiktok_url": "https://www.tiktok.com/t/ZP8ctwC2V/", "streamer": "jason"},
        )

    assert response.status_code == 202
    assert app.state.search_job_service.created_jobs[0]["tiktok_url"] == "https://www.tiktok.com/t/ZP8ctwC2V/"


def test_public_search_endpoint_rejects_profile_url_before_queueing() -> None:
    app, client = build_client(create_public_app)

    with client:
        response = client.post(
            "/api/search/clip",
            data={"tiktok_url": "https://www.tiktok.com/@jasontheween", "streamer": "jason"},
        )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "INVALID_TIKTOK_URL"
    assert app.state.search_job_service.created_jobs == []


def test_public_search_endpoint_requires_tiktok_url() -> None:
    app, client = build_client(create_public_app)

    with client:
        response = client.post(
            "/api/search/clip",
            data={"streamer": "xqc"},
        )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "INVALID_SEARCH_INPUT"
    assert app.state.search_job_service.created_jobs == []


def test_public_search_endpoint_validates_streamer() -> None:
    app, client = build_client(create_public_app)

    with client:
        response = client.post(
            "/api/search/clip",
            data={"tiktok_url": "https://www.tiktok.com/@u/video/1", "streamer": "ronaldo"},
        )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "INVALID_STREAMER"
    assert app.state.search_job_service.created_jobs == []


def test_public_search_endpoint_accepts_date_range() -> None:
    app, client = build_client(create_public_app)

    with client:
        response = client.post(
            "/api/search/clip",
            data={
                "tiktok_url": "https://www.tiktok.com/@u/video/1",
                "streamer": "jason",
                "streamed_from": "2026-04-01",
                "streamed_to": "2026-04-07",
            },
        )

    assert response.status_code == 202
    date_range = app.state.search_job_service.created_jobs[0]["date_range"]
    assert date_range == SearchDateRange(
        streamed_from=datetime(2026, 4, 1, tzinfo=timezone.utc),
        streamed_to=datetime(2026, 4, 8, tzinfo=timezone.utc),
    )


def test_public_search_endpoint_rejects_invalid_date_range() -> None:
    app, client = build_client(create_public_app)

    with client:
        response = client.post(
            "/api/search/clip",
            data={
                "tiktok_url": "https://www.tiktok.com/@u/video/1",
                "streamer": "jason",
                "streamed_from": "2026-04-08",
                "streamed_to": "2026-04-07",
            },
        )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "INVALID_DATE_RANGE"
    assert app.state.search_job_service.created_jobs == []


def test_public_search_job_endpoint_returns_job_status() -> None:
    app, client = build_client(create_public_app)
    app.state.search_job_service.jobs[101] = SearchJobRecord(
        id=101,
        status="completed",
        stage=None,
        created_at="2026-04-15T00:00:00Z",
        started_at="2026-04-15T00:00:01Z",
        finished_at="2026-04-15T00:00:02Z",
        result=SearchResult(found=False, streamer="jason", reason="url test"),
        error_code=None,
        error_message=None,
    )

    with client:
        response = client.get("/api/search/clip/101")

    assert response.status_code == 200
    assert response.json()["search_id"] == 101
    assert response.json()["status"] == "completed"
    assert response.json()["result"]["reason"] == "url test"


def test_public_search_job_endpoint_returns_restored_multi_segment_payload() -> None:
    app, client = build_client(create_public_app)
    restored_result = SearchResult.from_dict(
        {
            "found": True,
            "streamer": "jason",
            "video_id": 7,
            "video_url": "https://www.twitch.tv/videos/7",
            "video_url_at_timestamp": "https://www.twitch.tv/videos/7?t=1m40s",
            "timestamp_seconds": 100,
            "score": 0.91,
            "reason": "Accepted 2 supported segments",
            "segments": [
                {
                    "query_start": 0.0,
                    "query_end": 5.0,
                    "video_id": 7,
                    "vod_start": 100.0,
                    "vod_end": 105.0,
                    "score": 0.91,
                    "ranking_score": 8.0,
                    "offset_seconds": 100.0,
                    "mean_similarity": 0.94,
                    "density": 1.0,
                    "supporting_fingerprints": 10,
                    "top_rank_fingerprints": 8,
                    "video_url_at_timestamp": "https://www.twitch.tv/videos/7?t=1m40s",
                },
                {
                    "query_start": 8.0,
                    "query_end": 14.0,
                    "video_id": 8,
                    "vod_start": 500.0,
                    "vod_end": 506.0,
                    "score": 0.87,
                    "ranking_score": 9.0,
                    "offset_seconds": 492.0,
                    "mean_similarity": 0.90,
                    "density": 0.9,
                    "supporting_fingerprints": 11,
                    "top_rank_fingerprints": 7,
                    "video_url_at_timestamp": "https://www.twitch.tv/videos/8?t=8m20s",
                },
            ],
            "unmatched_ranges": [
                {"query_start": 5.0, "query_end": 8.0},
                {"query_start": 14.0, "query_end": 16.0},
            ],
            "query_duration_seconds": 16.0,
        }
    )
    app.state.search_job_service.jobs[101] = SearchJobRecord(
        id=101,
        status="completed",
        stage=None,
        created_at="2026-08-24T00:00:00Z",
        started_at="2026-08-24T00:00:01Z",
        finished_at="2026-08-24T00:00:02Z",
        result=restored_result,
        error_code=None,
        error_message=None,
    )

    with client:
        response = client.get("/api/search/clip/101")

    assert response.status_code == 200
    payload = response.json()["result"]
    assert payload["timestamp_seconds"] == 100
    assert payload["video_url_at_timestamp"] == "https://www.twitch.tv/videos/7?t=1m40s"
    assert [segment["video_id"] for segment in payload["segments"]] == [7, 8]
    assert payload["segments"][1] == {
        "query_start": 8.0,
        "query_end": 14.0,
        "video_id": 8,
        "vod_start": 500.0,
        "vod_end": 506.0,
        "video_url_at_timestamp": "https://www.twitch.tv/videos/8?t=8m20s",
        "score": 0.87,
    }
    assert payload["unmatched_ranges"] == [
        {"query_start": 5.0, "query_end": 8.0},
        {"query_start": 14.0, "query_end": 16.0},
    ]
    assert payload["query_duration_seconds"] == 16.0


def test_public_search_job_endpoint_returns_404_for_unknown_job() -> None:
    app, client = build_client(create_public_app)

    with client:
        response = client.get("/api/search/clip/999")

    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "SEARCH_NOT_FOUND"
