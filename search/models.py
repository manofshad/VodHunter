from dataclasses import dataclass


@dataclass
class SearchResult:
    found: bool
    streamer: str | None = None
    video_id: int | None = None
    video_url: str | None = None
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
