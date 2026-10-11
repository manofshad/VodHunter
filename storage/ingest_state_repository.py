"""Persistence for resumable Twitch VOD ingestion cursors."""

from __future__ import annotations

from datetime import datetime, timezone

from storage.database import PostgresDatabase
from storage.records import VodIngestStateRecord


class IngestStateRepository:
    """Read and mutate the cursor for an in-progress VOD."""

    def __init__(self, database: PostgresDatabase) -> None:
        self.database = database

    def get(self, vod_platform_id: str) -> VodIngestStateRecord | None:
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT vod_platform_id, video_id, streamer, last_ingested_seconds,
                           last_seen_duration_seconds, updated_at
                    FROM vod_ingest_state
                    WHERE vod_platform_id = %s
                    LIMIT 1
                    """,
                    (vod_platform_id,),
                )
                row = cur.fetchone()
        if row is None:
            return None
        return self._from_row(row)

    def upsert(
        self,
        *,
        vod_platform_id: str,
        video_id: int,
        streamer: str,
        last_ingested_seconds: int,
        last_seen_duration_seconds: int,
    ) -> None:
        updated_at = datetime.now(timezone.utc)
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO vod_ingest_state (
                        vod_platform_id,
                        video_id,
                        streamer,
                        last_ingested_seconds,
                        last_seen_duration_seconds,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (vod_platform_id) DO UPDATE SET
                        video_id = excluded.video_id,
                        streamer = excluded.streamer,
                        last_ingested_seconds = excluded.last_ingested_seconds,
                        last_seen_duration_seconds = excluded.last_seen_duration_seconds,
                        updated_at = excluded.updated_at
                    """,
                    (
                        vod_platform_id,
                        int(video_id),
                        streamer,
                        int(last_ingested_seconds),
                        int(last_seen_duration_seconds),
                        updated_at,
                    ),
                )

    def list_pending(self, streamer: str) -> list[VodIngestStateRecord]:
        normalized_streamer = streamer.strip().lower()
        if not normalized_streamer:
            raise ValueError("streamer is required")
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT state.vod_platform_id, state.video_id, state.streamer,
                           state.last_ingested_seconds, state.last_seen_duration_seconds,
                           state.updated_at
                    FROM vod_ingest_state AS state
                    JOIN videos AS video ON video.id = state.video_id
                    JOIN creators AS creator ON creator.id = video.creator_id
                    WHERE video.status = 'indexing'
                      AND LOWER(BTRIM(state.streamer)) = %s
                      AND LOWER(BTRIM(creator.name)) = %s
                    ORDER BY state.updated_at, state.vod_platform_id
                    """,
                    (normalized_streamer, normalized_streamer),
                )
                rows = cur.fetchall()
        return [self._from_row(row) for row in rows]

    def delete(self, vod_platform_id: str) -> None:
        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM vod_ingest_state WHERE vod_platform_id = %s",
                    (vod_platform_id,),
                )

    @staticmethod
    def _from_row(row) -> VodIngestStateRecord:
        return VodIngestStateRecord(
            vod_platform_id=str(row[0]),
            video_id=int(row[1]),
            streamer=str(row[2]),
            last_ingested_seconds=int(row[3]),
            last_seen_duration_seconds=int(row[4]),
            updated_at=row[5],
        )
