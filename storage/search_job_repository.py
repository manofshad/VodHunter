"""Persistence operations for asynchronous public search jobs."""

from __future__ import annotations

from dataclasses import asdict, fields, is_dataclass
from datetime import datetime, timezone
import json
from typing import Any

from search.models import (
    SearchDateRange,
    SearchJobRecord,
    SearchRequestOutcome,
    SearchResult,
)
from search.twitch_time import build_twitch_timestamp_url
from storage.database import PostgresDatabase
from storage.video_repository import VideoRepository


class SearchJobRepository:
    """Store job state and lossless search-result payloads."""

    def __init__(self, database: PostgresDatabase, videos: VideoRepository) -> None:
        self.database = database
        self.videos = videos

    def create_public_search_job(
        self,
        *,
        tiktok_url: str,
        streamer: str,
        creator_id: int | None,
        date_range: SearchDateRange | None = None,
    ) -> int:
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO search_requests (
                        source_app,
                        route,
                        input_type,
                        streamer,
                        creator_id,
                        success,
                        job_status,
                        job_stage,
                        tiktok_url,
                        streamed_from,
                        streamed_to,
                        model_version,
                        preprocessing_version
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        "public",
                        "/api/search/clip",
                        "tiktok_url",
                        streamer,
                        creator_id,
                        False,
                        "queued",
                        "validating",
                        tiktok_url,
                        date_range.streamed_from if date_range is not None else None,
                        date_range.streamed_to if date_range is not None else None,
                        self.database.model_version,
                        self.database.preprocessing_version,
                    ),
                )
                row = cur.fetchone()
        if row is None:
            raise RuntimeError("Failed to create public search job")
        return int(row[0])

    def update_search_job_status(
        self,
        search_id: int,
        *,
        status: str | None = None,
        stage: str | None = None,
        started: bool = False,
    ) -> None:
        assignments: list[str] = []
        values: list[Any] = []
        if status is not None:
            assignments.append("job_status = %s")
            values.append(status)
        if stage is not None:
            assignments.append("job_stage = %s")
            values.append(stage)
        if started:
            assignments.append("started_at = COALESCE(started_at, NOW())")
        if not assignments:
            return

        values.append(int(search_id))
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE search_requests
                    SET {', '.join(assignments)}
                    WHERE id = %s
                    """,
                    values,
                )

    def complete_search_job(self, search_id: int, outcome: SearchRequestOutcome) -> None:
        metadata = outcome.execution_metadata
        result = outcome.result
        result_payload = self._serialize_search_result(result)
        result_segments = getattr(result, "segments", None)
        segment_count = getattr(metadata, "segment_count", None)
        if segment_count is None and result_segments is not None:
            segment_count = len(result_segments)

        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE search_requests
                    SET success = TRUE,
                        http_status = 200,
                        error_code = NULL,
                        error_message = NULL,
                        result_reason = %s,
                        found_match = %s,
                        matched_video_id = %s,
                        matched_timestamp_seconds = %s,
                        score = %s,
                        download_source = %s,
                        download_host = %s,
                        input_duration_seconds = %s,
                        total_duration_ms = %s,
                        preprocess_duration_ms = %s,
                        embed_duration_ms = %s,
                        vector_query_duration_ms = %s,
                        alignment_duration_ms = %s,
                        result_payload = %s::jsonb,
                        model_version = %s,
                        preprocessing_version = %s,
                        model_startup_duration_ms = %s,
                        model_cold_start = %s,
                        fingerprint_preprocessing_duration_ms = %s,
                        fingerprint_inference_duration_ms = %s,
                        fingerprint_duration_ms = %s,
                        query_fingerprint_count = %s,
                        candidate_count = %s,
                        segment_count = %s,
                        job_status = 'completed',
                        job_stage = NULL,
                        finished_at = NOW()
                    WHERE id = %s
                    """,
                    (
                        metadata.result_reason or result.reason,
                        metadata.found_match if metadata.found_match is not None else result.found,
                        metadata.matched_video_id
                        if metadata.matched_video_id is not None
                        else result.video_id,
                        metadata.matched_timestamp_seconds
                        if metadata.matched_timestamp_seconds is not None
                        else result.timestamp_seconds,
                        metadata.score if metadata.score is not None else result.score,
                        outcome.download_source,
                        outcome.download_host,
                        outcome.input_duration_seconds,
                        outcome.total_duration_ms,
                        metadata.preprocess_duration_ms,
                        metadata.embed_duration_ms,
                        metadata.vector_query_duration_ms,
                        metadata.alignment_duration_ms,
                        result_payload,
                        getattr(metadata, "model_version", None) or self.database.model_version,
                        getattr(metadata, "preprocessing_version", None)
                        or self.database.preprocessing_version,
                        getattr(metadata, "model_startup_duration_ms", None),
                        getattr(metadata, "model_cold_start", None),
                        getattr(metadata, "fingerprint_preprocessing_duration_ms", None),
                        getattr(metadata, "fingerprint_inference_duration_ms", None),
                        getattr(metadata, "fingerprint_duration_ms", None),
                        getattr(metadata, "query_fingerprint_count", None),
                        getattr(metadata, "candidate_count", None),
                        segment_count,
                        int(search_id),
                    ),
                )

    def fail_search_job(
        self,
        search_id: int,
        *,
        error_code: str,
        error_message: str,
        http_status: int,
        input_duration_seconds: float | None = None,
    ) -> None:
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE search_requests
                    SET success = FALSE,
                        http_status = %s,
                        error_code = %s,
                        error_message = %s,
                        input_duration_seconds = COALESCE(%s, input_duration_seconds),
                        job_status = 'failed',
                        job_stage = NULL,
                        finished_at = NOW()
                    WHERE id = %s
                    """,
                    (
                        int(http_status),
                        error_code,
                        error_message,
                        input_duration_seconds,
                        int(search_id),
                    ),
                )

    def get_public_search_job(self, search_id: int) -> SearchJobRecord | None:
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        id,
                        job_status,
                        job_stage,
                        created_at,
                        started_at,
                        finished_at,
                        found_match,
                        streamer,
                        matched_video_id,
                        matched_timestamp_seconds,
                        score,
                        result_reason,
                        error_code,
                        error_message,
                        result_payload
                    FROM search_requests
                    WHERE id = %s
                      AND source_app = 'public'
                    LIMIT 1
                    """,
                    (int(search_id),),
                )
                row = cur.fetchone()
        if row is None:
            return None

        (
            record_id,
            status,
            stage,
            created_at,
            started_at,
            finished_at,
            found_match,
            streamer,
            matched_video_id,
            matched_timestamp_seconds,
            score,
            result_reason,
            error_code,
            error_message,
            result_payload,
        ) = row

        result: SearchResult | None = None
        if status == "completed":
            if result_payload is not None:
                result = self._deserialize_search_result(result_payload)
            else:
                # Preserve read access to jobs written before result_payload
                # was introduced. New jobs always use the lossless JSONB path.
                result = SearchResult(
                    found=bool(found_match),
                    streamer=str(streamer) if streamer else None,
                    video_id=int(matched_video_id) if matched_video_id is not None else None,
                    score=float(score) if score is not None else None,
                    reason=str(result_reason) if result_reason else None,
                )
                if matched_video_id is not None:
                    video_row = self.videos.get_video_with_creator(int(matched_video_id))
                    if video_row is not None:
                        result.streamer = video_row.creator_name
                        result.profile_image_url = video_row.creator_profile_image_url
                        result.video_id = video_row.id
                        result.video_url = video_row.url
                        result.thumbnail_url = video_row.thumbnail_url
                        result.title = video_row.title
                        timestamp = (
                            int(matched_timestamp_seconds)
                            if matched_timestamp_seconds is not None
                            else None
                        )
                        result.timestamp_seconds = timestamp
                        if timestamp is not None:
                            result.video_url_at_timestamp = build_twitch_timestamp_url(
                                video_row.url,
                                timestamp,
                            )
                else:
                    result.profile_image_url = self.videos.get_profile_image_for_streamer(
                        str(streamer) if streamer else None
                    )

        return SearchJobRecord(
            id=int(record_id),
            status=str(status),
            stage=str(stage) if stage else None,
            created_at=self._isoformat(created_at),
            started_at=self._isoformat(started_at),
            finished_at=self._isoformat(finished_at),
            result=result,
            error_code=str(error_code) if error_code else None,
            error_message=str(error_message) if error_message else None,
        )

    def fail_incomplete_public_search_jobs(
        self,
        *,
        error_code: str,
        error_message: str,
    ) -> None:
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE search_requests
                    SET success = FALSE,
                        http_status = 500,
                        error_code = %s,
                        error_message = %s,
                        job_status = 'failed',
                        job_stage = NULL,
                        finished_at = COALESCE(finished_at, NOW())
                    WHERE source_app = 'public'
                      AND job_status IN ('queued', 'running')
                    """,
                    (error_code, error_message),
                )

    @staticmethod
    def _serialize_search_result(result: SearchResult) -> str:
        to_dict = getattr(result, "to_dict", None)
        if callable(to_dict):
            payload = to_dict()
        elif is_dataclass(result):
            payload = asdict(result)
        else:  # pragma: no cover - SearchResult is a dataclass in production
            payload = dict(vars(result))
        return json.dumps(payload, separators=(",", ":"), sort_keys=True)

    @staticmethod
    def _deserialize_search_result(payload: Any) -> SearchResult:
        if isinstance(payload, str):
            value = json.loads(payload)
        elif isinstance(payload, dict):
            value = payload
        else:
            raise ValueError("Stored search result payload must be a JSON object")

        from_dict = getattr(SearchResult, "from_dict", None)
        if callable(from_dict):
            return from_dict(value)

        known_fields = {field.name for field in fields(SearchResult)}
        return SearchResult(**{key: item for key, item in value.items() if key in known_fields})

    @staticmethod
    def _isoformat(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        return str(value)
