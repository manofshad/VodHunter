from typing import Literal

from pydantic import BaseModel, Field

from search.models import SearchResult


class ErrorResponse(BaseModel):
    code: str
    message: str


class LiveStartRequest(BaseModel):
    streamer: str = Field(min_length=1, max_length=100)


class LiveStatusResponse(BaseModel):
    state: Literal["idle", "polling", "ingesting", "error"]
    streamer: str | None
    is_live: bool | None
    started_at: str | None
    last_check_at: str | None
    last_error: str | None
    current_video_id: int | None
    current_vod_url: str | None
    ingest_cursor_seconds: int | None
    lag_seconds: int | None
    eventsub_enabled: bool | None = None
    eventsub_health: Literal["healthy", "degraded", "unsubscribed"] | None = None
    eventsub_last_event_at: str | None = None
    eventsub_last_error: str | None = None


class LiveStartResponse(BaseModel):
    status: LiveStatusResponse


class LiveStopResponse(BaseModel):
    stopped: bool
    status: LiveStatusResponse


class LiveSessionItem(BaseModel):
    video_id: int
    creator_name: str
    url: str
    title: str
    processed: bool


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
