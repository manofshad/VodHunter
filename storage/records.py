"""Typed records returned by the storage layer.

These records are read-only snapshots of persisted data.  Raw database rows and
serialized API payloads should stay inside their respective boundaries.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class VideoStatus(StrEnum):
    """Lifecycle states persisted for an indexed video."""

    INDEXING = "indexing"
    SEARCHABLE = "searchable"
    DELETED = "deleted"
    REINDEX_REQUESTED = "reindex_requested"


@dataclass(frozen=True, slots=True)
class VideoRecord:
    """A video row, optionally enriched with its creator's display data.

    ``status`` is optional only for compatibility with rows from before the
    lifecycle column was populated. Current production rows always include it.
    """

    id: int
    creator_id: int
    url: str
    title: str
    thumbnail_url: str | None
    status: VideoStatus | None
    processed: bool
    streamed_at: datetime | None
    creator_name: str | None = None
    creator_profile_image_url: str | None = None


@dataclass(frozen=True, slots=True)
class VodIngestStateRecord:
    """The resumable ingest cursor for one Twitch VOD."""

    vod_platform_id: str
    video_id: int
    streamer: str
    last_ingested_seconds: int
    last_seen_duration_seconds: int
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class SearchableStreamer:
    """A creator with at least one searchable fingerprint."""

    name: str
    profile_image_url: str | None
