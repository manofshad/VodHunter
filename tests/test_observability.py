from __future__ import annotations

import json
from types import SimpleNamespace

import backend.observability as observability


def test_terminal_search_event_contains_result_identity_and_timings(monkeypatch) -> None:
    events: list[dict[str, object]] = []
    monkeypatch.setattr(
        observability._SEARCH_EVENT_LOGGER,
        "info",
        lambda message: events.append(json.loads(message)),
    )

    result = SimpleNamespace(
        video_id=417,
        video_url="https://www.twitch.tv/videos/123",
        video_url_at_timestamp="https://www.twitch.tv/videos/123?t=1h2m3s",
        timestamp_seconds=3723,
        score=0.91,
        reason="Aligned NMFP evidence",
        title="A stream",
    )
    observability.observe_terminal_search(
        search_id=1842,
        streamer="jasontheween",
        outcome="match",
        status="completed",
        total_duration_ms=8342,
        stage_durations_ms={"download": 2110, "alignment": 46, "missing": None},
        result=result,
        diagnostics={"candidate_count": 318},
    )

    assert len(events) == 1
    event = events[0]
    assert event["event"] == "search_finished"
    assert event["search_id"] == 1842
    assert event["outcome"] == "match"
    assert event["total_duration_ms"] == 8342
    assert event["stages_ms"] == {"alignment": 46, "download": 2110}
    assert event["result_link"] == "https://www.twitch.tv/videos/123?t=1h2m3s"
    assert event["result_timestamp_seconds"] == 3723
    assert event["score"] == 0.91
    assert event["result"]["video_id"] == 417
    assert "error" not in event


def test_terminal_no_match_event_omits_error_field(monkeypatch) -> None:
    events: list[dict[str, object]] = []
    monkeypatch.setattr(
        observability._SEARCH_EVENT_LOGGER,
        "info",
        lambda message: events.append(json.loads(message)),
    )

    observability.observe_terminal_search(
        search_id=1844,
        streamer="xqc",
        outcome="no_match",
        status="completed",
        total_duration_ms=420,
    )

    assert len(events) == 1
    assert events[0]["outcome"] == "no_match"
    assert events[0]["found_match"] is False
    assert "error" not in events[0]


def test_terminal_error_event_has_no_result_and_keeps_error_code(monkeypatch) -> None:
    events: list[dict[str, object]] = []
    monkeypatch.setattr(
        observability._SEARCH_EVENT_LOGGER,
        "info",
        lambda message: events.append(json.loads(message)),
    )

    observability.observe_terminal_search(
        search_id=1843,
        streamer="xqc",
        outcome="error",
        status="failed",
        total_duration_ms=120,
        stage_durations_ms={"download": 80},
        error_code="DOWNLOAD_ERROR",
        http_status=400,
    )

    assert events == [
        {
            "diagnostics": {},
            "error": {"code": "DOWNLOAD_ERROR", "http_status": 400},
            "event": "search_finished",
            "found_match": None,
            "outcome": "error",
            "result": None,
            "result_link": None,
            "result_timestamp_seconds": None,
            "schema_version": 1,
            "score": None,
            "search_id": 1843,
            "stages_ms": {"download": 80},
            "status": "failed",
            "streamer": "xqc",
            "timestamp": events[0]["timestamp"],
            "total_duration_ms": 120,
        }
    ]


def test_metrics_endpoint_payload_contains_search_metrics() -> None:
    payload, content_type = observability.metrics_response()

    assert content_type.startswith("text/plain")
    assert b"vodhunter_searches_total" in payload
    assert b"vodhunter_search_duration_seconds" in payload
    assert b"vodhunter_search_stage_duration_seconds" in payload
