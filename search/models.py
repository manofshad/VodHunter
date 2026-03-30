from dataclasses import dataclass


@dataclass
class SearchResult:
    found: bool
    streamer: str | None = None
    profile_image_url: str | None = None
    video_id: int | None = None
    video_url: str | None = None
    video_url_at_timestamp: str | None = None
    thumbnail_url: str | None = None
    title: str | None = None
    timestamp_seconds: int | None = None
    score: float | None = None
    reason: str | None = None


@dataclass
class AlignmentResult:
    found: bool
    video_id: int | None = None
    timestamp_seconds: int | None = None
    score: float | None = None
    reason: str | None = None


@dataclass
class SearchExecutionMetadata:
    preprocess_duration_ms: int | None = None
    embed_duration_ms: int | None = None
    vector_query_duration_ms: int | None = None
    alignment_duration_ms: int | None = None
    result_reason: str | None = None
    found_match: bool | None = None
    matched_video_id: int | None = None
    matched_timestamp_seconds: int | None = None
    score: float | None = None


@dataclass
class SearchExecutionResult:
    result: SearchResult
    metadata: SearchExecutionMetadata


@dataclass
class SearchRequestOutcome:
    result: SearchResult
    execution_metadata: SearchExecutionMetadata
    input_type: str
    clip_filename: str | None = None
    download_source: str | None = None
    download_host: str | None = None
    input_duration_seconds: float | None = None
    total_duration_ms: int | None = None


@dataclass
class SearchRequestLog:
    source_app: str
    route: str
    input_type: str | None = None
    streamer: str | None = None
    success: bool = False
    http_status: int | None = None
    error_code: str | None = None
    error_message: str | None = None
    result_reason: str | None = None
    found_match: bool | None = None
    matched_video_id: int | None = None
    matched_timestamp_seconds: int | None = None
    score: float | None = None
    clip_filename: str | None = None
    download_source: str | None = None
    download_host: str | None = None
    input_duration_seconds: float | None = None
    total_duration_ms: int | None = None
    preprocess_duration_ms: int | None = None
    embed_duration_ms: int | None = None
    vector_query_duration_ms: int | None = None
    alignment_duration_ms: int | None = None
