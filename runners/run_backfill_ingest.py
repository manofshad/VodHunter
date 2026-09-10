from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from typing import Callable

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")

from backend.bootstrap_ingest import build_ingest_state
from backend.bootstrap_shared import build_repositories
from pipeline.ingest_session import IngestSession
from services.twitch_monitor import TwitchMonitor
from storage.records import VodIngestStateRecord, VideoRecord, VideoStatus
from storage.repositories import Repositories
from sources.historical_archive_vod_source import HistoricalArchiveVODSource


INGEST_CHUNK_SECONDS = 60
SESSION_POLL_INTERVAL = 0.5
BACKFILL_TEMP_DIR = str(ROOT_DIR / "data" / "temp_backfill_chunks")


@dataclass
class BackfillResult:
    ingested: int = 0
    resumed: int = 0
    skipped: int = 0
    failed: int = 0


class FreshNMFPReindexPreconditionError(RuntimeError):
    """Raised when a guarded fresh reindex is attempted on an unprepared database."""


def _get_existing_video_status(
    existing_video: VideoRecord | None,
) -> VideoStatus | None:
    if existing_video is None:
        return None
    return existing_video.status


def _classify_backfill_candidate(
    existing_video: VideoRecord | None,
    existing_state: VodIngestStateRecord | None,
) -> tuple[bool, VideoStatus | str | None, bool]:
    if existing_video is None:
        return True, None, False

    existing_status = _get_existing_video_status(existing_video)
    if existing_status is VideoStatus.REINDEX_REQUESTED:
        return True, existing_status, True
    if existing_status is VideoStatus.INDEXING:
        if existing_state is not None:
            return True, existing_status, False
        return False, existing_status, False
    if existing_status in {VideoStatus.SEARCHABLE, VideoStatus.DELETED}:
        return False, existing_status, False

    if existing_video.processed:
        return False, "processed", False
    return True, None, False


def _validate_fresh_nmfp_reindex(
    videos: object,
    ingest_states: object,
    vods: list[dict[str, object]],
) -> None:
    """Fail closed unless the schema migration prepared every retained VOD.

    The NMFP schema migration deletes incompatible fingerprints/cursors and marks
    active videos ``reindex_requested``. This preflight makes the first backfill
    after that migration explicit: it must not skip a legacy searchable row or
    resume an ambiguous ``indexing`` cursor. Deleted videos remain intentionally
    excluded, and VODs not yet present in the database are safe to ingest.
    """

    problems: list[str] = []
    for vod in vods:
        vod_id = str(vod.get("id") or "").strip() or "<missing>"
        vod_url = str(vod.get("url") or "").strip()
        existing_video = videos.get_video_by_url(vod_url)
        existing_state = ingest_states.get(vod_id)

        if existing_video is None:
            if existing_state is not None:
                problems.append(f"vod={vod_id} status=missing_video has_saved_cursor=true")
            continue

        existing_status = _get_existing_video_status(existing_video)
        if existing_status in {VideoStatus.REINDEX_REQUESTED, VideoStatus.DELETED}:
            continue

        fallback_status = existing_status.value if existing_status is not None else None
        if fallback_status is None:
            fallback_status = "processed" if existing_video.processed else "unknown"
        problems.append(
            f"vod={vod_id} status={fallback_status} "
            f"has_saved_cursor={'true' if existing_state is not None else 'false'}"
        )

    if problems:
        details = "; ".join(problems)
        raise FreshNMFPReindexPreconditionError(
            "Fresh NMFP reindex preflight failed. Apply the NMFP schema migration first; "
            "it must clear incompatible fingerprints/cursors and mark active videos "
            f"reindex_requested. Refusing to skip or resume ambiguous rows: {details}"
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill Twitch archive VODs for a streamer.")
    parser.add_argument("--streamer", required=True, help="Twitch login name")
    parser.add_argument("--days", required=True, type=int, help="Number of past days to ingest")
    parser.add_argument(
        "--fresh-nmfp-reindex",
        action="store_true",
        help=(
            "Guard the first full NMFP backfill after migration: require active existing "
            "videos to be reindex_requested and never resume a saved cursor"
        ),
    )
    return parser


def run_backfill_ingest(
    streamer: str,
    days: int,
    *,
    fresh_nmfp_reindex: bool = False,
    monitor: TwitchMonitor | None = None,
    build_storage: Callable[[], Repositories] = build_repositories,
    build_ingest: Callable[[], dict[str, object]] = build_ingest_state,
    source_factory: Callable[..., HistoricalArchiveVODSource] = HistoricalArchiveVODSource,
    session_factory: Callable[..., IngestSession] = IngestSession,
    out: Callable[[str], None] = print,
) -> BackfillResult:
    normalized_streamer = streamer.strip().lower()
    if not normalized_streamer:
        raise ValueError("streamer is required")
    if int(days) < 1:
        raise ValueError("days must be >= 1")

    repositories = build_storage()
    videos = repositories.videos
    ingest_states = repositories.ingest_states
    fingerprints = repositories.fingerprints

    twitch_monitor = monitor or TwitchMonitor.from_env()
    creator_metadata = twitch_monitor.get_user_profile(normalized_streamer)
    user_id = str(creator_metadata["id"])
    cutoff = datetime.now(timezone.utc) - timedelta(days=int(days))
    vods = twitch_monitor.list_archive_vods_since(user_id=user_id, created_after=cutoff)

    if fresh_nmfp_reindex:
        _validate_fresh_nmfp_reindex(videos, ingest_states, vods)
        out(
            "fresh_nmfp_reindex preflight=passed "
            f"streamer={normalized_streamer} vod_count={len(vods)} resume_allowed=false"
        )

    ingest_state = build_ingest()
    embedder = ingest_state["embedder"]

    result = BackfillResult()
    total_vods = len(vods)
    for index, vod in enumerate(vods, start=1):
        existing_video = videos.get_video_by_url(str(vod["url"]))
        existing_state = ingest_states.get(str(vod["id"]))
        is_eligible, skip_reason, restart_from_scratch = _classify_backfill_candidate(
            existing_video,
            existing_state,
        )
        if not is_eligible:
            result.skipped += 1
            out(f"starting vod {index}/{total_vods} vod={vod['id']} status=skip_check")
            out(f"skip {skip_reason} vod={vod['id']} url={vod['url']}")
            continue

        if restart_from_scratch and existing_state is not None:
            ingest_states.delete(str(vod["id"]))
            existing_state = None

        starting_cursor = 0
        if existing_state is not None and existing_state.last_ingested_seconds > 0:
            result.resumed += 1
            starting_cursor = existing_state.last_ingested_seconds
            out(f"resume vod={vod['id']} cursor={existing_state.last_ingested_seconds}")

        out(
            f"starting vod {index}/{total_vods} vod={vod['id']} "
            f"url={vod['url']} cursor={starting_cursor}"
        )

        def emit_progress(event: dict[str, object]) -> None:
            event_type = str(event.get("event") or "")
            if event_type == "chunk_start":
                out(
                    "processing "
                    f"vod={event['vod_id']} chunk={int(event['start_seconds'])}-{int(event['end_seconds'])} "
                    f"progress={float(event['percent_complete']):.1f}% "
                    f"overall={index}/{total_vods}"
                )
                return
            if event_type == "vod_complete":
                out(f"completed vod={event['vod_id']} progress=100.0% overall={index}/{total_vods}")

        source = source_factory(
            streamer=normalized_streamer,
            vod_metadata=vod,
            creator_metadata=creator_metadata,
            videos=videos,
            ingest_states=ingest_states,
            chunk_seconds=INGEST_CHUNK_SECONDS,
            temp_dir=BACKFILL_TEMP_DIR,
            progress_callback=emit_progress,
        )

        session = session_factory(
            source=source,
            embedder=embedder,
            fingerprints=fingerprints,
            poll_interval=SESSION_POLL_INTERVAL,
        )

        try:
            session.run()
            result.ingested += 1
            out(f"ingested vod={vod['id']} url={vod['url']}")
        except Exception as exc:
            result.failed += 1
            out(f"failed vod={vod['id']} url={vod['url']} error={exc}")

    out(
        "summary "
        f"streamer={normalized_streamer} ingested={result.ingested} resumed={result.resumed} "
        f"skipped={result.skipped} failed={result.failed}"
    )
    return result


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_backfill_ingest(
        args.streamer,
        args.days,
        fresh_nmfp_reindex=args.fresh_nmfp_reindex,
    )
    return 1 if result.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
