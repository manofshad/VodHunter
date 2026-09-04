"""Transactional cleanup for expired local VOD search data."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from backend.db_url import normalize_database_url


RETENTION_CANDIDATE_SQL = """
SELECT v.id
FROM videos AS v
WHERE v.streamed_at IS NOT NULL
  AND v.streamed_at < NOW() - make_interval(days => %s)
  -- The status and cursor are the ingest worker's active-claim markers.
  AND v.status IS DISTINCT FROM 'indexing'
  AND NOT EXISTS (
      SELECT 1
      FROM vod_ingest_state AS ingest_state
      WHERE ingest_state.video_id = v.id
  )
ORDER BY v.streamed_at ASC, v.id ASC
FOR UPDATE OF v SKIP LOCKED
"""


@dataclass(frozen=True)
class VodRetentionResult:
    retention_days: int
    deleted_video_ids: tuple[int, ...]

    @property
    def deleted_count(self) -> int:
        return len(self.deleted_video_ids)


def _connect(database_url: str) -> Any:
    try:
        import psycopg  # type: ignore
    except Exception as exc:  # pragma: no cover - depends on the runtime image
        raise RuntimeError("VOD retention requires psycopg to be installed") from exc
    return psycopg.connect(database_url)


def _validate_retention_days(retention_days: int) -> int:
    try:
        normalized_days = int(retention_days)
    except (TypeError, ValueError) as exc:
        raise ValueError("retention_days must be an integer") from exc
    if normalized_days < 1:
        raise ValueError("retention_days must be at least 1")
    return normalized_days


def _ensure_video_was_deleted(cursor: Any, video_id: int) -> None:
    """Fail closed if the locked candidate disappears before the final delete."""

    rowcount = getattr(cursor, "rowcount", None)
    if rowcount is not None and rowcount not in (-1, 1):
        raise RuntimeError(f"Expected to delete video_id={video_id}, deleted_rows={rowcount}")


def purge_expired_vods(
    database_url: str,
    retention_days: int,
    *,
    connect: Callable[[str], Any] | None = None,
) -> VodRetentionResult:
    """Delete expired, non-indexing VODs in one PostgreSQL transaction.

    The cutoff is intentionally computed by PostgreSQL from ``NOW()``. A
    ``TIMESTAMPTZ`` comparison is absolute, and the connection timezone is set
    to UTC so operational timestamps are unambiguous. The strict ``<`` keeps a
    VOD exactly on the retention boundary, while the explicit NULL predicate
    makes the fail-closed behavior clear.
    """

    normalized_url = normalize_database_url(database_url)
    if not normalized_url:
        raise ValueError("DATABASE_URL is required")
    normalized_days = _validate_retention_days(retention_days)
    connection_factory = connect or _connect

    deleted_video_ids: list[int] = []
    with connection_factory(normalized_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC'")
            cursor.execute(RETENTION_CANDIDATE_SQL, (normalized_days,))
            candidate_rows = cursor.fetchall()

            for row in candidate_rows:
                video_id = int(row[0])

                # Keep historical search rows, but remove their video pointer
                # before deleting the referenced video.
                cursor.execute(
                    """
                    UPDATE search_requests
                    SET matched_video_id = NULL
                    WHERE matched_video_id = %s
                    """,
                    (video_id,),
                )
                cursor.execute(
                    """
                    DELETE FROM fingerprint_embeddings
                    WHERE fingerprint_id IN (
                        SELECT id
                        FROM fingerprints
                        WHERE video_id = %s
                    )
                    """,
                    (video_id,),
                )
                cursor.execute(
                    "DELETE FROM fingerprints WHERE video_id = %s",
                    (video_id,),
                )
                cursor.execute(
                    "DELETE FROM vod_ingest_state WHERE video_id = %s",
                    (video_id,),
                )
                cursor.execute(
                    "DELETE FROM videos WHERE id = %s",
                    (video_id,),
                )
                _ensure_video_was_deleted(cursor, video_id)
                deleted_video_ids.append(video_id)

    return VodRetentionResult(
        retention_days=normalized_days,
        deleted_video_ids=tuple(deleted_video_ids),
    )
