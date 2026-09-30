"""Search metrics and terminal events.

The public search worker is asynchronous, so the job service is the only
place that can reliably associate a completed or failed operation with its
``search_id``.  This module keeps the exported metric surface small and
emits one JSON event for each terminal search state.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import math
import sys
from collections.abc import Mapping
from typing import Any

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest


_SEARCH_OUTCOMES = ("match", "no_match", "error")
_TOTAL_BUCKETS = (0.25, 0.5, 1, 2, 5, 10, 20, 30, 60, 120, 180, 300)
_STAGE_BUCKETS = (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 180)


SEARCHES_TOTAL = Counter(
    "vodhunter_searches_total",
    "Number of accepted public searches reaching a terminal state.",
    labelnames=("outcome",),
)
SEARCH_DURATION_SECONDS = Histogram(
    "vodhunter_search_duration_seconds",
    "Elapsed accepted public search time through terminal persistence.",
    labelnames=("outcome",),
    buckets=_TOTAL_BUCKETS,
)
SEARCH_STAGE_DURATION_SECONDS = Histogram(
    "vodhunter_search_stage_duration_seconds",
    "Duration of a public search pipeline stage.",
    labelnames=("stage", "outcome"),
    buckets=_STAGE_BUCKETS,
)
SEARCH_FAILURES_TOTAL = Counter(
    "vodhunter_search_failures_total",
    "Number of public searches ending with an application error.",
    labelnames=("error_code",),
)

# Initialize bounded outcomes so a lack of errors is zero, rather than a
# missing series. Histogram quantiles still remain unknown without samples.
for _outcome in _SEARCH_OUTCOMES:
    SEARCHES_TOTAL.labels(outcome=_outcome).inc(0)
    SEARCH_DURATION_SECONDS.labels(outcome=_outcome)

HTTP_REQUESTS_TOTAL = Counter(
    "vodhunter_http_requests_total", "Public API requests, including status polling.",
    labelnames=("route", "method", "status_class"),
)
HTTP_DURATION_SECONDS = Histogram(
    "vodhunter_http_request_duration_seconds", "Public API response duration.",
    labelnames=("route", "method"), buckets=_STAGE_BUCKETS,
)
SEARCH_SUBMISSIONS_TOTAL = Counter(
    "vodhunter_search_submissions_total", "Search creation responses by acceptance category.",
    labelnames=("result",),
)
SEARCH_QUEUE_WAIT_SECONDS = Histogram(
    "vodhunter_search_queue_wait_seconds", "Search creation through executor start.",
    buckets=_TOTAL_BUCKETS,
)
for _submission_result in ("accepted", "invalid", "rate_limited", "server_error", "rejected"):
    SEARCH_SUBMISSIONS_TOTAL.labels(result=_submission_result).inc(0)


def observe_http_request(route: str, method: str, status: int, elapsed: float) -> None:
    """Use route templates and a finite method vocabulary, never raw paths."""
    normalized_method = method if method in {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"} else "OTHER"
    HTTP_REQUESTS_TOTAL.labels(route=route, method=normalized_method, status_class=f"{status // 100}xx").inc()
    HTTP_DURATION_SECONDS.labels(route=route, method=normalized_method).observe(max(0, elapsed))
    if route == "/api/search/clip" and method == "POST":
        result = (
            "accepted" if status == 202 else "rate_limited" if status == 429
            else "server_error" if status >= 500 else "invalid" if status in {400, 422}
            else "rejected"
        )
        SEARCH_SUBMISSIONS_TOTAL.labels(result=result).inc()


def observe_queue_wait(elapsed: float) -> None:
    SEARCH_QUEUE_WAIT_SECONDS.observe(max(0, elapsed))


def _build_event_logger() -> logging.Logger:
    """Create a logger whose message is a raw JSON line.

    Uvicorn's error logger prefixes messages with a level marker.  A separate
    non-propagating logger keeps the search event parseable by Alloy's JSON
    stage while leaving the existing diagnostic logs unchanged.
    """

    logger = logging.getLogger("vodhunter.search_event")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    return logger


_SEARCH_EVENT_LOGGER = _build_event_logger()


def metrics_response() -> tuple[bytes, str]:
    """Return the current Prometheus exposition payload and content type."""

    return generate_latest(), CONTENT_TYPE_LATEST


def _safe_float(value: object | None) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _result_payload(result: Any | None) -> dict[str, Any] | None:
    if result is None:
        return None
    return {
        "video_id": getattr(result, "video_id", None),
        "video_url": getattr(result, "video_url", None),
        "video_url_at_timestamp": getattr(result, "video_url_at_timestamp", None),
        "timestamp_seconds": getattr(result, "timestamp_seconds", None),
        "score": _safe_float(getattr(result, "score", None)),
        "reason": getattr(result, "reason", None),
        "title": getattr(result, "title", None),
    }


def observe_terminal_search(
    *,
    search_id: int,
    streamer: str | None,
    outcome: str,
    status: str,
    total_duration_ms: int | None,
    stage_durations_ms: Mapping[str, int | None] | None = None,
    result: Any | None = None,
    diagnostics: Mapping[str, Any] | None = None,
    error_code: str | None = None,
    http_status: int | None = None,
) -> None:
    """Record one terminal search observation and emit its JSON event."""

    normalized_outcome = outcome if outcome in _SEARCH_OUTCOMES else "error"
    SEARCHES_TOTAL.labels(outcome=normalized_outcome).inc()
    if total_duration_ms is not None:
        SEARCH_DURATION_SECONDS.labels(outcome=normalized_outcome).observe(
            max(float(total_duration_ms), 0.0) / 1000.0
        )

    normalized_stages: dict[str, int] = {}
    for stage, duration_ms in (stage_durations_ms or {}).items():
        if duration_ms is None:
            continue
        normalized_duration_ms = max(int(duration_ms), 0)
        normalized_stages[str(stage)] = normalized_duration_ms
        SEARCH_STAGE_DURATION_SECONDS.labels(
            stage=str(stage), outcome=normalized_outcome
        ).observe(normalized_duration_ms / 1000.0)

    if error_code:
        SEARCH_FAILURES_TOTAL.labels(error_code=str(error_code)).inc()

    event: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": "error" if normalized_outcome == "error" else "info",
        "logger": "vodhunter.search_event",
        "message": "Public search reached a terminal state",
        "event": "search_finished",
        "schema_version": 1,
        "search_id": int(search_id),
        "status": status,
        "outcome": normalized_outcome,
        "found_match": (
            bool(getattr(result, "found", False))
            if result is not None
            else (False if normalized_outcome == "no_match" else None)
        ),
        "streamer": streamer.strip().lower() if isinstance(streamer, str) else streamer,
        "total_duration_ms": total_duration_ms,
        "stages_ms": normalized_stages,
        "result": _result_payload(result),
        "diagnostics": dict(diagnostics or {}),
        "result_link": (
            getattr(result, "video_url_at_timestamp", None) if result is not None else None
        ),
        "result_timestamp_seconds": (
            getattr(result, "timestamp_seconds", None) if result is not None else None
        ),
        "score": _safe_float(getattr(result, "score", None)) if result is not None else None,
    }
    if error_code:
        event["error"] = {"code": str(error_code), "http_status": http_status}

    _SEARCH_EVENT_LOGGER.info(
        json.dumps(event, separators=(",", ":"), sort_keys=True, ensure_ascii=False)
    )
