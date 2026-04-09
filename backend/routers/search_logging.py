import logging
from urllib.parse import urlparse

from fastapi import Request

from search.models import SearchRequestLog, SearchRequestOutcome


logger = logging.getLogger("uvicorn.error")
SEARCH_ROUTE = "/api/search/clip"


def infer_input_type(has_file: bool, has_url: bool) -> str | None:
    if has_file and has_url:
        return "both"
    if has_file:
        return "file"
    if has_url:
        return "tiktok_url"
    return None


def normalize_streamer_value(streamer: str | None) -> str | None:
    normalized = (streamer or "").strip().lower()
    return normalized or None


def extract_download_host(url: str | None) -> str | None:
    hostname = urlparse((url or "").strip()).hostname
    if not hostname:
        return None
    return hostname.lower()


def build_log_from_outcome(
    *,
    source_app: str,
    streamer: str | None,
    creator_id: int | None,
    outcome: SearchRequestOutcome,
    http_status: int = 200,
) -> SearchRequestLog:
    return SearchRequestLog(
        source_app=source_app,
        route=SEARCH_ROUTE,
        input_type=outcome.input_type,
        streamer=streamer,
        creator_id=creator_id,
        success=True,
        http_status=http_status,
        result_reason=outcome.execution_metadata.result_reason or outcome.result.reason,
        found_match=outcome.execution_metadata.found_match if outcome.execution_metadata.found_match is not None else outcome.result.found,
        matched_video_id=outcome.execution_metadata.matched_video_id if outcome.execution_metadata.matched_video_id is not None else outcome.result.video_id,
        matched_timestamp_seconds=outcome.execution_metadata.matched_timestamp_seconds if outcome.execution_metadata.matched_timestamp_seconds is not None else outcome.result.timestamp_seconds,
        score=outcome.execution_metadata.score if outcome.execution_metadata.score is not None else outcome.result.score,
        clip_filename=outcome.clip_filename,
        download_source=outcome.download_source,
        download_host=outcome.download_host,
        input_duration_seconds=outcome.input_duration_seconds,
        total_duration_ms=outcome.total_duration_ms,
        preprocess_duration_ms=outcome.execution_metadata.preprocess_duration_ms,
        embed_duration_ms=outcome.execution_metadata.embed_duration_ms,
        vector_query_duration_ms=outcome.execution_metadata.vector_query_duration_ms,
        alignment_duration_ms=outcome.execution_metadata.alignment_duration_ms,
    )


def persist_search_log(request: Request, log: SearchRequestLog) -> None:
    try:
        request.app.state.store.log_search_request(log)
    except Exception:
        logger.exception(
            "Failed to persist search request log source_app=%s route=%s",
            log.source_app,
            log.route,
        )
