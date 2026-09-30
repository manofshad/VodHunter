from types import SimpleNamespace

from fastapi.testclient import TestClient

from backend.apps.public import create_public_app
from backend.observability import HTTP_REQUESTS_TOTAL, SEARCH_SUBMISSIONS_TOTAL


def sample(metric, name, labels):
    for family in metric.collect():
        for item in family.samples:
            if item.name == name and item.labels == labels:
                return item.value
    return 0


def test_dynamic_paths_are_normalized_and_polling_does_not_count_as_submission():
    app = create_public_app(enable_lifespan=False)
    app.state.search_job_service = SimpleNamespace(get_public_search_job=lambda _: None)
    labels = {"route": "/api/search/clip/{search_id}", "method": "GET", "status_class": "4xx"}
    before = sample(HTTP_REQUESTS_TOTAL, "vodhunter_http_requests_total", labels)
    accepted = sample(SEARCH_SUBMISSIONS_TOTAL, "vodhunter_search_submissions_total", {"result": "accepted"})
    with TestClient(app) as client:
        assert client.get("/api/search/clip/124").status_code == 404
        assert client.get("/api/search/clip/999").status_code == 404
    assert sample(HTTP_REQUESTS_TOTAL, "vodhunter_http_requests_total", labels) == before + 2
    assert sample(SEARCH_SUBMISSIONS_TOTAL, "vodhunter_search_submissions_total", {"result": "accepted"}) == accepted


def test_invalid_submission_is_measured_before_job_creation():
    app = create_public_app(enable_lifespan=False)
    before = sample(SEARCH_SUBMISSIONS_TOTAL, "vodhunter_search_submissions_total", {"result": "invalid"})
    with TestClient(app) as client:
        assert client.post("/api/search/clip", data={}).status_code == 400
    assert sample(SEARCH_SUBMISSIONS_TOTAL, "vodhunter_search_submissions_total", {"result": "invalid"}) == before + 1


def test_observation_failure_does_not_change_api_response(monkeypatch):
    app = create_public_app(enable_lifespan=False)
    app.state.videos = SimpleNamespace(list_searchable_streamers=lambda: [])
    def fail(*_args):
        raise RuntimeError("collector unavailable")
    monkeypatch.setattr("backend.apps.public.observe_http_request", fail)
    with TestClient(app) as client:
        response = client.get("/api/search/streamers")
    assert response.status_code == 200
    assert response.json() == []
