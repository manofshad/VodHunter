from typing import Literal

from pydantic import BaseModel, Field

from search.models import SearchResult


class ErrorResponse(BaseModel):
    code: str
    message: str


class SearchResponse(BaseModel):
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
    sources: list["SearchSourceResponse"] = Field(default_factory=list)
    segments: list["SearchSegmentResponse"] = Field(default_factory=list)
    unmatched_ranges: list["UnmatchedRangeResponse"] = Field(default_factory=list)
    query_duration_seconds: float | None = None

    @classmethod
    def from_result(cls, result: SearchResult) -> "SearchResponse":
        return cls(
            found=result.found,
            streamer=result.streamer,
            profile_image_url=result.profile_image_url,
            video_id=result.video_id,
            video_url=result.video_url,
            video_url_at_timestamp=result.video_url_at_timestamp,
            thumbnail_url=result.thumbnail_url,
            title=result.title,
            timestamp_seconds=result.timestamp_seconds,
            score=result.score,
            reason=result.reason,
            sources=[SearchSourceResponse.from_source(source) for source in result.sources],
            segments=[SearchSegmentResponse.from_segment(segment) for segment in result.segments],
            unmatched_ranges=[UnmatchedRangeResponse.from_range(value) for value in result.unmatched_ranges],
            query_duration_seconds=result.query_duration_seconds,
        )


class SearchSegmentResponse(BaseModel):
    query_start: float
    query_end: float
    video_id: int
    vod_start: float
    vod_end: float
    video_url_at_timestamp: str | None = None
    score: float

    @classmethod
    def from_segment(cls, segment) -> "SearchSegmentResponse":
        return cls(
            query_start=segment.query_start,
            query_end=segment.query_end,
            video_id=segment.video_id,
            vod_start=segment.vod_start,
            vod_end=segment.vod_end,
            video_url_at_timestamp=segment.video_url_at_timestamp,
            score=segment.score,
        )


class SearchSourceResponse(BaseModel):
    video_id: int
    video_url: str | None = None
    video_url_at_timestamp: str | None = None
    thumbnail_url: str | None = None
    title: str | None = None
    streamer: str | None = None
    profile_image_url: str | None = None
    segments: list[SearchSegmentResponse] = Field(default_factory=list)

    @classmethod
    def from_source(cls, source) -> "SearchSourceResponse":
        return cls(
            video_id=source.video_id,
            video_url=source.video_url,
            video_url_at_timestamp=source.video_url_at_timestamp,
            thumbnail_url=source.thumbnail_url,
            title=source.title,
            streamer=source.streamer,
            profile_image_url=source.profile_image_url,
            segments=[SearchSegmentResponse.from_segment(segment) for segment in source.segments],
        )


class UnmatchedRangeResponse(BaseModel):
    query_start: float
    query_end: float

    @classmethod
    def from_range(cls, value) -> "UnmatchedRangeResponse":
        return cls(query_start=value.query_start, query_end=value.query_end)


if hasattr(SearchResponse, "model_rebuild"):
    SearchResponse.model_rebuild()
else:  # Pydantic 1, pinned by the TensorFlow 2.13 API runtime.
    SearchResponse.update_forward_refs(
        SearchSegmentResponse=SearchSegmentResponse,
        SearchSourceResponse=SearchSourceResponse,
        UnmatchedRangeResponse=UnmatchedRangeResponse,
    )


class StreamerListItem(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    profile_image_url: str | None = None


SearchJobStatus = Literal["queued", "running", "completed", "failed"]


class SearchJobError(BaseModel):
    code: str
    message: str


class SearchJobCreatedResponse(BaseModel):
    search_id: int
    status: SearchJobStatus
    stage: str | None = None


class SearchJobResponse(BaseModel):
    search_id: int
    status: SearchJobStatus
    stage: str | None = None
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    result: SearchResponse | None = None
    error: SearchJobError | None = None


class InternalVideoMutationRequest(BaseModel):
    actor_creator_id: int = Field(gt=0)


class InternalVideoMutationResponse(BaseModel):
    video_id: int
    status: Literal["deleted", "reindex_requested"]
