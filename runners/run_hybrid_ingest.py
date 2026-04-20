from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from threading import Thread
import time
from typing import Callable

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")

from backend import config
from backend.bootstrap_ingest import build_ingest_state
from backend.bootstrap_shared import build_store_state, prepare_runtime_dirs
from pipeline.ingest_session import IngestSession
from services.twitch_monitor import TwitchMonitor
from sources.historical_archive_vod_source import HistoricalArchiveVODSource
from sources.live_archive_vod_source import LiveArchiveVODSource


@dataclass
class HybridIngestResult:
    backlog_ingested: int = 0
    resumed: int = 0
    skipped: int = 0
    failed: int = 0
    live_sessions_started: int = 0
    handoffs_to_live: int = 0
    handoffs_to_backlog: int = 0
    watch_cycles: int = 0


@dataclass
class SessionRunHandle:
    session: IngestSession
    source: object
    thread: Thread
    error: Exception | None = None
    interrupted: bool = False


@dataclass
class BacklogCandidate:
    vod: dict[str, object]
    existing_state: dict[str, object] | None


@dataclass
class BacklogRunOutcome:
    preempted_for_live: bool = False
    interrupted: bool = False
    error: Exception | None = None


VIDEO_STATUS_INDEXING = "indexing"
VIDEO_STATUS_SEARCHABLE = "searchable"
VIDEO_STATUS_DELETED = "deleted"
VIDEO_STATUS_REINDEX_REQUESTED = "reindex_requested"


def _get_existing_video_status(
    store: object,
    existing_video: tuple[object, ...] | None,
) -> str | None:
    if existing_video is None:
        return None

    get_video_status = getattr(store, "get_video_status", None)
    if not callable(get_video_status):
        return None

    status = get_video_status(int(existing_video[0]))
    if status is None:
        return None
    return str(status).strip().lower() or None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Hybrid Twitch ingest with backlog catch-up and live priority.")
    parser.add_argument("--streamer", required=True, help="Twitch login name")
    parser.add_argument("--days", type=int, default=30, help="Number of past days to scan for backlog VODs")
    return parser


def run_hybrid_ingest(
    streamer: str,
    days: int = 30,
    *,
    monitor: TwitchMonitor | None = None,
    build_store: Callable[[], dict[str, object]] = build_store_state,
    build_ingest: Callable[[], dict[str, object]] = build_ingest_state,
    historical_source_factory: Callable[..., HistoricalArchiveVODSource] = HistoricalArchiveVODSource,
    live_source_factory: Callable[..., LiveArchiveVODSource] = LiveArchiveVODSource,
    session_factory: Callable[..., IngestSession] = IngestSession,
    out: Callable[[str], None] = print,
    should_stop: Callable[[], bool] | None = None,
    watch_poll_seconds: float = config.MONITOR_POLL_SECONDS,
    backlog_live_poll_seconds: float = config.LIVE_ARCHIVE_POLL_SECONDS,
    session_wait_seconds: float = config.SESSION_POLL_INTERVAL,
    retry_seconds: float = config.MONITOR_RETRY_SECONDS,
) -> HybridIngestResult:
    normalized_streamer = streamer.strip().lower()
    if not normalized_streamer:
        raise ValueError("streamer is required")
    if int(days) < 1:
        raise ValueError("days must be >= 1")

    should_stop = should_stop or (lambda: False)

    prepare_runtime_dirs()
    store_state = build_store()
    ingest_state = build_ingest()
    store = store_state["store"]
    embedder = ingest_state["embedder"]
    twitch_monitor = monitor or TwitchMonitor.from_env()

    creator_metadata = twitch_monitor.get_user_profile(normalized_streamer)
    user_id = str(creator_metadata["id"])

    result = HybridIngestResult()
    logged_skipped_vods: set[str] = set()
    last_mode: str | None = None
    should_log_watch = True
    force_live_handoff = False

    while not should_stop():
        if force_live_handoff:
            is_live = True
            force_live_handoff = False
        else:
            try:
                is_live = twitch_monitor.is_live(normalized_streamer)
            except Exception as exc:
                out(f"mode=watch streamer={normalized_streamer} error=live_check_failed detail={exc}")
                _sleep_interruptibly(retry_seconds, should_stop)
                continue

        if is_live:
            if last_mode != "live":
                out(f"mode=live streamer={normalized_streamer} reason=stream_live")
            live_handle = _start_live_session(
                streamer=normalized_streamer,
                store=store,
                embedder=embedder,
                twitch_monitor=twitch_monitor,
                session_factory=session_factory,
                live_source_factory=live_source_factory,
            )
            result.live_sessions_started += 1
            last_mode = "live"
            should_log_watch = True

            live_error = _wait_for_session(
                handle=live_handle,
                should_stop=should_stop,
                wait_seconds=session_wait_seconds,
            )
            if live_error is not None:
                result.failed += 1
                out(f"failed mode=live streamer={normalized_streamer} error={live_error}")
                _sleep_interruptibly(retry_seconds, should_stop)
                continue
            if live_handle.interrupted:
                break

            result.handoffs_to_backlog += 1
            out(f"handoff event=live_to_backlog streamer={normalized_streamer}")
            last_mode = "backlog"
            continue

        backlog = _build_backlog(
            twitch_monitor=twitch_monitor,
            store=store,
            user_id=user_id,
            days=days,
            skipped_vods_logged=logged_skipped_vods,
            out=out,
        )

        if backlog:
            candidate = backlog[0]
            vod = candidate.vod
            vod_id = str(vod["id"])
            if last_mode != "backlog":
                out(
                    f"mode=backlog streamer={normalized_streamer} backlog={len(backlog)} "
                    f"vod={vod_id} url={vod['url']}"
                )
            elif should_log_watch:
                out(
                    f"mode=backlog streamer={normalized_streamer} backlog={len(backlog)} "
                    f"vod={vod_id} url={vod['url']}"
                )

            if candidate.existing_state is not None and int(candidate.existing_state.get("last_ingested_seconds", 0)) > 0:
                result.resumed += 1
                out(
                    f"resume mode=backlog vod={vod_id} "
                    f"cursor={int(candidate.existing_state['last_ingested_seconds'])}"
                )

            backlog_outcome = _run_backlog_session(
                streamer=normalized_streamer,
                vod=vod,
                existing_state=candidate.existing_state,
                store=store,
                embedder=embedder,
                twitch_monitor=twitch_monitor,
                session_factory=session_factory,
                historical_source_factory=historical_source_factory,
                should_stop=should_stop,
                out=out,
                backlog_size=len(backlog),
                backlog_live_poll_seconds=backlog_live_poll_seconds,
            )
            last_mode = "backlog"
            should_log_watch = True

            if backlog_outcome.preempted_for_live:
                if should_stop():
                    break
                result.handoffs_to_live += 1
                out(f"handoff event=backlog_to_live streamer={normalized_streamer} vod={vod_id}")
                force_live_handoff = True
                continue
            if backlog_outcome.interrupted:
                break
            if backlog_outcome.error is not None:
                result.failed += 1
                out(f"failed mode=backlog vod={vod_id} url={vod['url']} error={backlog_outcome.error}")
                continue

            result.backlog_ingested += 1
            out(f"completed mode=backlog vod={vod_id} url={vod['url']}")
            continue

        result.watch_cycles += 1
        if last_mode != "watch" or should_log_watch:
            out(f"mode=watch streamer={normalized_streamer} backlog=0 is_live=false")
        last_mode = "watch"
        should_log_watch = False
        _sleep_interruptibly(watch_poll_seconds, should_stop)

    return result


def _build_backlog(
    *,
    twitch_monitor: TwitchMonitor,
    store: object,
    user_id: str,
    days: int,
    skipped_vods_logged: set[str],
    out: Callable[[str], None],
) -> list[BacklogCandidate]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=int(days))
    vods = twitch_monitor.list_archive_vods_since(user_id=user_id, created_after=cutoff)
    backlog: list[BacklogCandidate] = []

    for vod in vods:
        vod_id = str(vod["id"])
        existing_video = store.get_video_by_url(str(vod["url"]))
        existing_status = _get_existing_video_status(store, existing_video)
        existing_state = store.get_vod_ingest_state(vod_id)

        if existing_status == VIDEO_STATUS_REINDEX_REQUESTED:
            if existing_state is not None:
                delete_vod_ingest_state = getattr(store, "delete_vod_ingest_state", None)
                if callable(delete_vod_ingest_state):
                    delete_vod_ingest_state(vod_id)
            backlog.append(BacklogCandidate(vod=vod, existing_state=None))
            continue

        if existing_status == VIDEO_STATUS_INDEXING:
            if existing_state is not None:
                backlog.append(BacklogCandidate(vod=vod, existing_state=existing_state))
                continue
            if vod_id not in skipped_vods_logged:
                skipped_vods_logged.add(vod_id)
                out(f"skip {existing_status} vod={vod_id} url={vod['url']}")
            continue

        if existing_status in {VIDEO_STATUS_DELETED, VIDEO_STATUS_SEARCHABLE}:
            if vod_id not in skipped_vods_logged:
                skipped_vods_logged.add(vod_id)
                out(f"skip {existing_status} vod={vod_id} url={vod['url']}")
            continue

        if existing_video is not None and bool(existing_video[5]):
            if vod_id not in skipped_vods_logged:
                skipped_vods_logged.add(vod_id)
                out(f"skip processed vod={vod_id} url={vod['url']}")
            continue

        if existing_state is not None:
            backlog.append(BacklogCandidate(vod=vod, existing_state=existing_state))
            continue

        if existing_video is None or not bool(existing_video[5]):
            backlog.append(BacklogCandidate(vod=vod, existing_state=None))
            continue

    return backlog


def _start_live_session(
    *,
    streamer: str,
    store: object,
    embedder: object,
    twitch_monitor: TwitchMonitor,
    session_factory: Callable[..., IngestSession],
    live_source_factory: Callable[..., LiveArchiveVODSource],
) -> SessionRunHandle:
    source = live_source_factory(
        streamer=streamer,
        store=store,
        twitch_monitor=twitch_monitor,
        chunk_seconds=config.INGEST_CHUNK_SECONDS,
        lag_seconds=config.LIVE_ARCHIVE_LAG_SECONDS,
        poll_seconds=config.LIVE_ARCHIVE_POLL_SECONDS,
        finalize_checks=config.LIVE_ARCHIVE_FINALIZE_CHECKS,
        temp_dir=config.TEMP_LIVE_DIR,
    )
    session = session_factory(
        source=source,
        embedder=embedder,
        store=store,
        poll_interval=config.SESSION_POLL_INTERVAL,
    )
    return _spawn_session(session=session, source=source)


def _run_backlog_session(
    *,
    streamer: str,
    vod: dict[str, object],
    existing_state: dict[str, object] | None,
    store: object,
    embedder: object,
    twitch_monitor: TwitchMonitor,
    session_factory: Callable[..., IngestSession],
    historical_source_factory: Callable[..., HistoricalArchiveVODSource],
    should_stop: Callable[[], bool],
    out: Callable[[str], None],
    backlog_size: int,
    backlog_live_poll_seconds: float,
) -> BacklogRunOutcome:
    creator_metadata = twitch_monitor.get_user_profile(streamer, force_refresh=True)
    starting_cursor = 0 if existing_state is None else int(existing_state.get("last_ingested_seconds", 0))
    vod_id = str(vod["id"])
    out(f"starting mode=backlog vod={vod_id} url={vod['url']} cursor={starting_cursor} backlog={backlog_size}")

    def emit_progress(event: dict[str, object]) -> None:
        event_type = str(event.get("event") or "")
        if event_type == "chunk_start":
            out(
                "processing "
                f"vod={event['vod_id']} chunk={int(event['start_seconds'])}-{int(event['end_seconds'])} "
                f"progress={float(event['percent_complete']):.1f}% "
                f"backlog={backlog_size}"
            )
            return
        if event_type == "vod_complete":
            out(f"completed vod={event['vod_id']} progress=100.0% backlog={backlog_size}")

    source = historical_source_factory(
        streamer=streamer,
        vod_metadata=vod,
        creator_metadata=creator_metadata,
        store=store,
        chunk_seconds=config.INGEST_CHUNK_SECONDS,
        temp_dir=config.TEMP_BACKFILL_DIR,
        progress_callback=emit_progress,
    )
    session = session_factory(
        source=source,
        embedder=embedder,
        store=store,
        poll_interval=config.SESSION_POLL_INTERVAL,
    )
    handle = _spawn_session(session=session, source=source)

    preempted_for_live = False
    while handle.thread.is_alive():
        if should_stop():
            handle.interrupted = True
            handle.session.stop()
            break
        try:
            if twitch_monitor.is_live(streamer):
                if not handle.thread.is_alive():
                    break
                preempted_for_live = True
                out(f"handoff_requested event=backlog_to_live vod={vod_id}")
                handle.session.stop()
                break
        except Exception as exc:
            out(f"mode=backlog vod={vod_id} error=live_check_failed detail={exc}")
        _sleep_interruptibly(backlog_live_poll_seconds, should_stop)

    handle.thread.join()
    return BacklogRunOutcome(
        preempted_for_live=preempted_for_live,
        interrupted=handle.interrupted,
        error=handle.error,
    )


def _spawn_session(*, session: IngestSession, source: object) -> SessionRunHandle:
    handle: SessionRunHandle | None = None

    def _run() -> None:
        assert handle is not None
        try:
            session.run()
        except Exception as exc:
            handle.error = exc

    thread = Thread(target=_run, daemon=True)
    handle = SessionRunHandle(session=session, source=source, thread=thread)
    thread.start()
    return handle


def _wait_for_session(
    *,
    handle: SessionRunHandle,
    should_stop: Callable[[], bool],
    wait_seconds: float,
) -> Exception | None:
    while handle.thread.is_alive():
        if should_stop():
            handle.interrupted = True
            handle.session.stop()
            break
        _sleep_interruptibly(wait_seconds, should_stop)
    handle.thread.join()
    return handle.error


def _sleep_interruptibly(duration: float, should_stop: Callable[[], bool]) -> None:
    remaining = max(float(duration), 0.0)
    if remaining == 0:
        return
    while remaining > 0 and not should_stop():
        step = min(remaining, 0.25)
        time.sleep(step)
        remaining -= step


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        run_hybrid_ingest(args.streamer, args.days)
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
