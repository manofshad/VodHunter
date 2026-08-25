from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class FingerprintCandidate:
    """One ranked VOD neighbor for a query fingerprint."""

    query_index: int
    query_time: float
    fingerprint_id: int
    video_id: int
    vod_time: float
    similarity: float
    rank: int

    @property
    def offset(self) -> float:
        return self.vod_time - self.query_time


@dataclass(frozen=True)
class SearchSegment:
    """A supported query range mapped to one continuous range in one VOD."""

    query_start: float
    query_end: float
    video_id: int
    vod_start: float
    vod_end: float
    score: float
    ranking_score: float
    offset_seconds: float
    mean_similarity: float
    density: float
    supporting_fingerprints: int
    top_rank_fingerprints: int
    video_url_at_timestamp: str | None = None

    @property
    def duration_seconds(self) -> float:
        return self.query_end - self.query_start

    @property
    def confidence(self) -> float:
        """The bounded confidence exposed to API clients as ``score``."""

        return self.score

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SearchSegment":
        return cls(**value)


@dataclass(frozen=True)
class UnmatchedRange:
    query_start: float
    query_end: float

    @property
    def duration_seconds(self) -> float:
        return self.query_end - self.query_start

    def to_dict(self) -> dict[str, float]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "UnmatchedRange":
        return cls(**value)


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
    segments: list[SearchSegment] = field(default_factory=list)
    unmatched_ranges: list[UnmatchedRange] = field(default_factory=list)
    query_duration_seconds: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SearchResult":
        fields = dict(value)
        fields["segments"] = [
            item if isinstance(item, SearchSegment) else SearchSegment.from_dict(item)
            for item in fields.get("segments") or []
        ]
        fields["unmatched_ranges"] = [
            item if isinstance(item, UnmatchedRange) else UnmatchedRange.from_dict(item)
            for item in fields.get("unmatched_ranges") or []
        ]
        return cls(**fields)


@dataclass
class AlignmentResult:
    found: bool
    video_id: int | None = None
    timestamp_seconds: int | None = None
    score: float | None = None
    ranking_score: float | None = None
    reason: str | None = None
    segments: list[SearchSegment] = field(default_factory=list)
    unmatched_ranges: list[UnmatchedRange] = field(default_factory=list)
    query_duration_seconds: float | None = None


@dataclass
class SearchExecutionMetadata:
    preprocess_duration_ms: int | None = None
    embed_duration_ms: int | None = None
    model_startup_duration_ms: int | None = None
    fingerprint_preprocessing_duration_ms: int | None = None
    fingerprint_inference_duration_ms: int | None = None
    fingerprint_duration_ms: int | None = None
    model_cold_start: bool | None = None
    vector_query_duration_ms: int | None = None
    alignment_duration_ms: int | None = None
    query_fingerprint_count: int | None = None
    candidate_count: int | None = None
    segment_count: int | None = None
    model_version: str | None = None
    preprocessing_version: str | None = None
    result_reason: str | None = None
    found_match: bool | None = None
    matched_video_id: int | None = None
    matched_timestamp_seconds: int | None = None
    score: float | None = None


@dataclass
class SearchExecutionResult:
    result: SearchResult
    metadata: SearchExecutionMetadata


@dataclass(frozen=True)
class SearchDateRange:
    streamed_from: datetime | None = None
    streamed_to: datetime | None = None

    @property
    def has_bounds(self) -> bool:
        return self.streamed_from is not None or self.streamed_to is not None


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
    date_range: SearchDateRange | None = None


@dataclass
class SearchRequestLog:
    source_app: str
    route: str
    input_type: str | None = None
    streamer: str | None = None
    creator_id: int | None = None
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
    model_startup_duration_ms: int | None = None
    fingerprint_preprocessing_duration_ms: int | None = None
    fingerprint_inference_duration_ms: int | None = None
    fingerprint_duration_ms: int | None = None
    model_cold_start: bool | None = None
    vector_query_duration_ms: int | None = None
    alignment_duration_ms: int | None = None
    query_fingerprint_count: int | None = None
    candidate_count: int | None = None
    segment_count: int | None = None
    model_version: str | None = None
    preprocessing_version: str | None = None
    result_payload: dict[str, Any] | None = None
    streamed_from: datetime | None = None
    streamed_to: datetime | None = None


@dataclass
class SearchJobRecord:
    id: int
    status: str
    stage: str | None
    created_at: str
    started_at: str | None
    finished_at: str | None
    result: SearchResult | None
    error_code: str | None
    error_message: str | None
