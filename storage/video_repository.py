"""Persistence operations for creators, videos, and video lifecycle state."""

from __future__ import annotations

from typing import Any

from storage.database import PostgresDatabase
from storage.errors import (
    InvalidVideoStateTransitionError,
    VideoNotFoundError,
    VideoOwnerMismatchError,
)
from storage.records import SearchableStreamer, VideoRecord, VideoStatus


class VideoRepository:
    """Read and mutate creator/video records."""

    def __init__(self, database: PostgresDatabase) -> None:
        self.database = database

    @staticmethod
    def _normalize_video_status(status: str | VideoStatus) -> VideoStatus:
        normalized_status = (
            status.value if isinstance(status, VideoStatus) else str(status).strip().lower()
        )
        try:
            return VideoStatus(normalized_status)
        except ValueError as exc:
            raise ValueError(f"Invalid video status: {status}") from exc

    @classmethod
    def _status_from_processed(cls, processed: bool) -> VideoStatus:
        return VideoStatus.SEARCHABLE if bool(processed) else VideoStatus.INDEXING

    @classmethod
    def _processed_from_status(cls, status: str | VideoStatus) -> bool:
        return cls._normalize_video_status(status) is not VideoStatus.INDEXING

    @classmethod
    def _video_from_row(cls, row: Any, *, with_creator: bool = False) -> VideoRecord:
        creator_name = None
        creator_profile_image_url = None
        if with_creator:
            creator_name = str(row[8]) if row[8] is not None else None
            creator_profile_image_url = str(row[9]) if row[9] else None

        status = None
        if row[5] is not None:
            status = cls._normalize_video_status(str(row[5]))

        return VideoRecord(
            id=int(row[0]),
            creator_id=int(row[1]),
            url=str(row[2]),
            title=str(row[3]),
            thumbnail_url=str(row[4]) if row[4] else None,
            status=status,
            processed=bool(row[6]),
            streamed_at=row[7],
            creator_name=creator_name,
            creator_profile_image_url=creator_profile_image_url,
        )

    def get_video_by_url(self, url: str) -> VideoRecord | None:
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, creator_id, url, title, thumbnail_url,
                           status, processed, streamed_at
                    FROM videos
                    WHERE url = %s
                    LIMIT 1
                    """,
                    (url,),
                )
                row = cur.fetchone()
        if row is None:
            return None
        return self._video_from_row(row)

    def create_or_get_creator(
        self,
        name: str,
        url: str,
        profile_image_url: str | None = None,
    ) -> int:
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO creators (name, url, profile_image_url)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (url)
                    DO UPDATE SET
                        name = excluded.name,
                        profile_image_url = COALESCE(excluded.profile_image_url, creators.profile_image_url)
                    RETURNING id
                    """,
                    (name, url, profile_image_url),
                )
                row = cur.fetchone()
        if row is None:
            raise RuntimeError("Failed to resolve creator id")
        return int(row[0])

    def update_creator_metadata(
        self,
        creator_id: int,
        *,
        name: str | None = None,
        profile_image_url: str | None = None,
    ) -> None:
        assignments: list[str] = []
        values: list[Any] = []
        if name is not None:
            assignments.append("name = %s")
            values.append(name)
        if profile_image_url is not None:
            assignments.append("profile_image_url = %s")
            values.append(profile_image_url)
        if not assignments:
            return

        values.append(int(creator_id))
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE creators SET {', '.join(assignments)} WHERE id = %s",
                    values,
                )

    def get_creator_id_by_name(self, name: str) -> int | None:
        normalized_name = (name or "").strip().lower()
        if not normalized_name:
            raise ValueError("streamer is required")

        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id
                    FROM creators
                    WHERE LOWER(name) = %s
                    LIMIT 1
                    """,
                    (normalized_name,),
                )
                row = cur.fetchone()
        if row is None:
            return None
        return int(row[0])

    def get_profile_image_for_streamer(self, streamer: str | None) -> str | None:
        normalized_streamer = (streamer or "").strip().lower()
        if not normalized_streamer:
            return None
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT profile_image_url
                    FROM creators
                    WHERE LOWER(name) = %s
                    LIMIT 1
                    """,
                    (normalized_streamer,),
                )
                row = cur.fetchone()
        if row is None or not row[0]:
            return None
        return str(row[0])

    def create_video(
        self,
        creator_id: int,
        url: str,
        title: str,
        processed: bool,
        thumbnail_url: str | None = None,
        streamed_at: Any = None,
        status: str | VideoStatus | None = None,
    ) -> int:
        resolved_status = (
            self._normalize_video_status(status)
            if status is not None
            else self._status_from_processed(processed)
        )
        resolved_processed = self._processed_from_status(resolved_status)
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO videos (creator_id, url, title, thumbnail_url, processed, streamed_at, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        int(creator_id),
                        url,
                        title,
                        thumbnail_url,
                        resolved_processed,
                        streamed_at,
                        resolved_status.value,
                    ),
                )
                row = cur.fetchone()
        if row is None:
            raise RuntimeError("Failed to create video")
        return int(row[0])

    def update_video_metadata(
        self,
        video_id: int,
        *,
        title: str | None = None,
        thumbnail_url: str | None = None,
        processed: bool | None = None,
        streamed_at: Any = None,
        status: str | VideoStatus | None = None,
    ) -> None:
        assignments: list[str] = []
        values: list[Any] = []
        if title is not None:
            assignments.append("title = %s")
            values.append(title)
        if thumbnail_url is not None:
            assignments.append("thumbnail_url = %s")
            values.append(thumbnail_url)
        if status is not None:
            resolved_status = self._normalize_video_status(status)
            assignments.append("status = %s")
            values.append(resolved_status.value)
            assignments.append("processed = %s")
            values.append(self._processed_from_status(resolved_status))
        elif processed is not None:
            resolved_status = self._status_from_processed(processed)
            assignments.append("processed = %s")
            values.append(bool(processed))
            assignments.append("status = %s")
            values.append(resolved_status.value)
        if streamed_at is not None:
            assignments.append("streamed_at = %s")
            values.append(streamed_at)
        if not assignments:
            return

        values.append(int(video_id))
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE videos SET {', '.join(assignments)} WHERE id = %s",
                    values,
                )

    def mark_video_processed(self, video_id: int, processed: bool = True) -> None:
        resolved_status = self._status_from_processed(processed)
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE videos SET processed = %s, status = %s WHERE id = %s",
                    (bool(processed), resolved_status.value, int(video_id)),
                )

    def get_video_status(self, video_id: int) -> VideoStatus | None:
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status
                    FROM videos
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (int(video_id),),
                )
                row = cur.fetchone()
        if row is None or row[0] is None:
            return None
        return self._normalize_video_status(str(row[0]))

    def update_video_status(self, video_id: int, status: str | VideoStatus) -> None:
        resolved_status = self._normalize_video_status(status)
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE videos SET status = %s, processed = %s WHERE id = %s",
                    (
                        resolved_status.value,
                        self._processed_from_status(resolved_status),
                        int(video_id),
                    ),
                )

    def _lock_video_for_mutation(self, cur, video_id: int) -> tuple[int, VideoStatus]:
        cur.execute(
            """
            SELECT creator_id, status
            FROM videos
            WHERE id = %s
            LIMIT 1
            FOR UPDATE
            """,
            (int(video_id),),
        )
        row = cur.fetchone()
        if row is None:
            raise VideoNotFoundError()
        return int(row[0]), self._normalize_video_status(str(row[1]))

    def delete_video_index(self, video_id: int, actor_creator_id: int) -> VideoStatus:
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                owner_creator_id, current_status = self._lock_video_for_mutation(cur, video_id)
                if owner_creator_id != int(actor_creator_id):
                    raise VideoOwnerMismatchError()
                if current_status is VideoStatus.DELETED:
                    return VideoStatus.DELETED
                if current_status is not VideoStatus.SEARCHABLE:
                    raise InvalidVideoStateTransitionError(current_status.value)

                cur.execute(
                    """
                    DELETE FROM fingerprint_embeddings
                    WHERE creator_id = %s
                      AND fingerprint_id IN (
                        SELECT id
                        FROM fingerprints
                        WHERE video_id = %s
                    )
                    """,
                    (int(owner_creator_id), int(video_id)),
                )
                cur.execute(
                    "DELETE FROM fingerprints WHERE video_id = %s",
                    (int(video_id),),
                )
                cur.execute(
                    "DELETE FROM vod_ingest_state WHERE video_id = %s",
                    (int(video_id),),
                )
                cur.execute(
                    "UPDATE videos SET status = %s, processed = %s WHERE id = %s",
                    (
                        VideoStatus.DELETED.value,
                        self._processed_from_status(VideoStatus.DELETED),
                        int(video_id),
                    ),
                )
        return VideoStatus.DELETED

    def request_video_reindex(self, video_id: int, actor_creator_id: int) -> VideoStatus:
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                owner_creator_id, current_status = self._lock_video_for_mutation(cur, video_id)
                if owner_creator_id != int(actor_creator_id):
                    raise VideoOwnerMismatchError()
                if current_status is VideoStatus.REINDEX_REQUESTED:
                    return VideoStatus.REINDEX_REQUESTED
                if current_status is not VideoStatus.DELETED:
                    raise InvalidVideoStateTransitionError(current_status.value)

                cur.execute(
                    "DELETE FROM vod_ingest_state WHERE video_id = %s",
                    (int(video_id),),
                )
                cur.execute(
                    "UPDATE videos SET status = %s, processed = %s WHERE id = %s",
                    (
                        VideoStatus.REINDEX_REQUESTED.value,
                        self._processed_from_status(VideoStatus.REINDEX_REQUESTED),
                        int(video_id),
                    ),
                )
        return VideoStatus.REINDEX_REQUESTED

    def get_video_with_creator(self, video_id: int) -> VideoRecord | None:
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        videos.id,
                        videos.creator_id,
                        videos.url,
                        videos.title,
                        videos.thumbnail_url,
                        videos.status,
                        videos.processed,
                        videos.streamed_at,
                        creators.name,
                        creators.profile_image_url
                    FROM videos
                    JOIN creators ON creators.id = videos.creator_id
                    WHERE videos.id = %s
                    """,
                    (int(video_id),),
                )
                row = cur.fetchone()
        if row is None:
            return None
        return self._video_from_row(row, with_creator=True)

    def list_searchable_streamers(self) -> list[SearchableStreamer]:
        # An existence check can stop at the first indexed embedding for each
        # creator instead of aggregating every fingerprint in the catalog.
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT c.name, c.profile_image_url
                    FROM creators c
                    WHERE c.name IS NOT NULL AND BTRIM(c.name) <> ''
                      AND EXISTS (
                          SELECT 1
                          FROM fingerprint_embeddings fe
                          WHERE fe.creator_id = c.id
                      )
                    ORDER BY LOWER(c.name), c.name
                    """
                )
                rows = cur.fetchall()
        return [
            SearchableStreamer(
                name=str(row[0]),
                profile_image_url=str(row[1]) if row[1] else None,
            )
            for row in rows
        ]
