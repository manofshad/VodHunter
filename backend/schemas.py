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
    video_id: int | None = None
    video_url: str | None = None
    title: str | None = None
    timestamp_seconds: int | None = None
    score: float | None = None
    reason: str | None = None

    @classmethod
    def from_result(cls, result: SearchResult) -> "SearchResponse":
        return cls(
            found=result.found,
            streamer=result.streamer,
            video_id=result.video_id,
            video_url=result.video_url,
            title=result.title,
            timestamp_seconds=result.timestamp_seconds,
            score=result.score,
            reason=result.reason,
        )
