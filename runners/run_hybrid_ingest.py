from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from threading import Event, Thread
import time
from typing import Callable

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")

from backend.bootstrap_ingest import build_ingest_state
from backend.bootstrap_shared import build_repositories
from pipeline.ingest_session import IngestSession
from pipeline.scheduled_embedder import (
    BACKLOG_PRIORITY,
    LIVE_PRIORITY,
    ScheduledEmbedder,
)
from services.twitch_monitor import CachedMultiStreamerTwitchMonitor, TwitchMonitor
from sources.historical_archive_vod_source import HistoricalArchiveVODSource
from sources.live_archive_vod_source import LiveArchiveVODSource
from storage.records import VodIngestStateRecord, VideoRecord, VideoStatus
from storage.repositories import Repositories


INGEST_CHUNK_SECONDS = 60
MONITOR_POLL_SECONDS = 30.0
SESSION_POLL_INTERVAL = 0.5
MONITOR_RETRY_SECONDS = 5.0
LIVE_ARCHIVE_LAG_SECONDS = 120
LIVE_ARCHIVE_POLL_SECONDS = 15.0
LIVE_ARCHIVE_FINALIZE_CHECKS = 3
LIVE_TEMP_DIR = str(ROOT_DIR / "data" / "temp_live_chunks")
BACKFILL_TEMP_DIR = str(ROOT_DIR / "data" / "temp_backfill_chunks")
DEFAULT_STREAMERS = ("jasontheween", "stableronaldo")


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
class MultiStreamerIngestResult:
    streamers: tuple[str, ...]
    results: dict[str, HybridIngestResult]


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
    existing_state: VodIngestStateRecord | None


@dataclass
class BacklogRunOutcome:
    preempted_for_live: bool = False
    interrupted: bool = False
    error: Exception | None = None


def _get_existing_video_status(
    existing_video: VideoRecord | None,
) -> VideoStatus | None:
    if existing_video is None:
        return None
    return existing_video.status


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Hybrid Twitch ingest with backlog catch-up and live priority.")
    parser.add_argument(
        "--streamer",
        action="append",
        default=[],
        help="Twitch login name; repeat for multiple streamers",
    )
    parser.add_argument(
        "--streamers",
        default="",
        help="Comma-separated Twitch login names",
    )
    parser.add_argument("--days", type=int, default=30, help="Number of past days to scan for backlog VODs")
    return parser


def _normalize_streamers(streamers: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_value in streamers:
        for candidate in str(raw_value).split(","):
            streamer = candidate.strip().lower()
            if streamer and streamer not in seen:
                seen.add(streamer)
                normalized.append(streamer)
    if not normalized:
        raise ValueError("at least one streamer is required")
    if len(normalized) > 100:
        raise ValueError("one worker supports at most 100 configured streamers")
    return normalized


def _provision_streamer(
    *,
    streamer: str,
    repositories: object,
    twitch_monitor: object,
) -> dict[str, object]:
    creator_metadata = twitch_monitor.get_user_profile(streamer)
    partition_repository = getattr(repositories, "embedding_partitions", None)
    if partition_repository is None:
        return creator_metadata

    creator_id = repositories.videos.create_or_get_creator(
        streamer,
        f"https://twitch.tv/{streamer}",
        profile_image_url=creator_metadata.get("profile_image_url"),
    )
    partition_name = partition_repository.ensure_creator_partition(creator_id)
    return {**creator_metadata, "creator_id": creator_id, "partition_name": partition_name}


def run_hybrid_ingest(
    streamer: str,
    days: int = 30,
    *,
    monitor: TwitchMonitor | None = None,
    build_storage: Callable[[], Repositories] = build_repositories,
    build_ingest: Callable[[], dict[str, object]] = build_ingest_state,
    historical_source_factory: Callable[..., HistoricalArchiveVODSource] = HistoricalArchiveVODSource,
    live_source_factory: Callable[..., LiveArchiveVODSource] = LiveArchiveVODSource,
    session_factory: Callable[..., IngestSession] = IngestSession,
    out: Callable[[str], None] = print,
    should_stop: Callable[[], bool] | None = None,
    watch_poll_seconds: float = MONITOR_POLL_SECONDS,
    backlog_live_poll_seconds: float = LIVE_ARCHIVE_POLL_SECONDS,
    session_wait_seconds: float = SESSION_POLL_INTERVAL,
    retry_seconds: float = MONITOR_RETRY_SECONDS,
    provision_streamer: bool = True,
) -> HybridIngestResult:
    normalized_streamer = streamer.strip().lower()
    if not normalized_streamer:
        raise ValueError("streamer is required")
    if int(days) < 1:
        raise ValueError("days must be >= 1")

    should_stop = should_stop or (lambda: False)

    repositories = build_storage()
    ingest_state = build_ingest()
    videos = repositories.videos
    ingest_states = repositories.ingest_states
    fingerprints = repositories.fingerprints
    embedder = ingest_state["embedder"]
    live_embedder = ingest_state.get("live_embedder", embedder)
    backlog_embedder = ingest_state.get("backlog_embedder", embedder)
    twitch_monitor = monitor or TwitchMonitor.from_env()

    if provision_streamer:
        creator_metadata = _provision_streamer(
            streamer=normalized_streamer,
            repositories=repositories,
            twitch_monitor=twitch_monitor,
        )
    else:
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
                videos=videos,
                ingest_states=ingest_states,
                fingerprints=fingerprints,
                embedder=live_embedder,
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
            videos=videos,
            ingest_states=ingest_states,
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

            if candidate.existing_state is not None and candidate.existing_state.last_ingested_seconds > 0:
                result.resumed += 1
                out(
                    f"resume mode=backlog vod={vod_id} "
                    f"cursor={candidate.existing_state.last_ingested_seconds}"
                )

            backlog_outcome = _run_backlog_session(
                streamer=normalized_streamer,
                vod=vod,
                existing_state=candidate.existing_state,
                videos=videos,
                ingest_states=ingest_states,
                fingerprints=fingerprints,
                embedder=backlog_embedder,
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
    videos: object,
    ingest_states: object,
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
        existing_video = videos.get_video_by_url(str(vod["url"]))
        existing_status = _get_existing_video_status(existing_video)
        existing_state = ingest_states.get(vod_id)

        if existing_status is VideoStatus.REINDEX_REQUESTED:
            if existing_state is not None:
                ingest_states.delete(vod_id)
            backlog.append(BacklogCandidate(vod=vod, existing_state=None))
            continue

        if existing_status is VideoStatus.INDEXING:
            if existing_state is not None:
                backlog.append(BacklogCandidate(vod=vod, existing_state=existing_state))
                continue
            if vod_id not in skipped_vods_logged:
                skipped_vods_logged.add(vod_id)
                out(f"skip {existing_status} vod={vod_id} url={vod['url']}")
            continue

        if existing_status in {VideoStatus.DELETED, VideoStatus.SEARCHABLE}:
            if vod_id not in skipped_vods_logged:
                skipped_vods_logged.add(vod_id)
                out(f"skip {existing_status} vod={vod_id} url={vod['url']}")
            continue

        if existing_video is not None and existing_video.processed:
            if vod_id not in skipped_vods_logged:
                skipped_vods_logged.add(vod_id)
                out(f"skip processed vod={vod_id} url={vod['url']}")
            continue

        if existing_state is not None:
            backlog.append(BacklogCandidate(vod=vod, existing_state=existing_state))
            continue

        if existing_video is None or not existing_video.processed:
            backlog.append(BacklogCandidate(vod=vod, existing_state=None))
            continue

    backlog.sort(
        key=lambda candidate: TwitchMonitor.parse_twitch_datetime(
            str(candidate.vod.get("created_at") or "")
        )
    )
    return backlog


def _start_live_session(
    *,
    streamer: str,
    videos: object,
    ingest_states: object,
    fingerprints: object,
    embedder: object,
    twitch_monitor: TwitchMonitor,
    session_factory: Callable[..., IngestSession],
    live_source_factory: Callable[..., LiveArchiveVODSource],
) -> SessionRunHandle:
    source = live_source_factory(
        streamer=streamer,
        videos=videos,
        ingest_states=ingest_states,
        twitch_monitor=twitch_monitor,
        chunk_seconds=INGEST_CHUNK_SECONDS,
        lag_seconds=LIVE_ARCHIVE_LAG_SECONDS,
        poll_seconds=LIVE_ARCHIVE_POLL_SECONDS,
        finalize_checks=LIVE_ARCHIVE_FINALIZE_CHECKS,
        temp_dir=str(Path(LIVE_TEMP_DIR) / streamer),
    )
    session = session_factory(
        source=source,
        embedder=embedder,
        fingerprints=fingerprints,
        poll_interval=SESSION_POLL_INTERVAL,
    )
    return _spawn_session(session=session, source=source)


def _run_backlog_session(
    *,
    streamer: str,
    vod: dict[str, object],
    existing_state: VodIngestStateRecord | None,
    videos: object,
    ingest_states: object,
    fingerprints: object,
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
    starting_cursor = 0 if existing_state is None else existing_state.last_ingested_seconds
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
        videos=videos,
        ingest_states=ingest_states,
        chunk_seconds=INGEST_CHUNK_SECONDS,
        temp_dir=str(Path(BACKFILL_TEMP_DIR) / streamer / vod_id),
        progress_callback=emit_progress,
    )
    session = session_factory(
        source=source,
        embedder=embedder,
        fingerprints=fingerprints,
        poll_interval=SESSION_POLL_INTERVAL,
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


def run_multi_streamer_ingest(
    streamers: list[str],
    days: int = 30,
    *,
    monitor: TwitchMonitor | None = None,
    build_storage: Callable[[], Repositories] = build_repositories,
    build_ingest: Callable[[], dict[str, object]] = build_ingest_state,
    out: Callable[[str], None] = print,
    should_stop: Callable[[], bool] | None = None,
    controller: Callable[..., HybridIngestResult] = run_hybrid_ingest,
) -> MultiStreamerIngestResult:
    normalized_streamers = _normalize_streamers(streamers)
    if int(days) < 1:
        raise ValueError("days must be >= 1")

    repositories = build_storage()
    ingest_state = build_ingest()
    base_embedder = ingest_state["embedder"]
    raw_monitor = monitor or TwitchMonitor.from_env()

    for streamer in normalized_streamers:
        provisioned = _provision_streamer(
            streamer=streamer,
            repositories=repositories,
            twitch_monitor=raw_monitor,
        )
        out(
            f"provisioned streamer={streamer} user_id={provisioned['id']} "
            f"partition={provisioned.get('partition_name', 'unmanaged')}"
        )

    model_load_ms = base_embedder.load()
    scheduler = ScheduledEmbedder(base_embedder)
    shared_monitor = CachedMultiStreamerTwitchMonitor(
        raw_monitor,
        normalized_streamers,
        cache_seconds=LIVE_ARCHIVE_POLL_SECONDS,
    )
    stop_event = Event()
    external_should_stop = should_stop or (lambda: False)
    results: dict[str, HybridIngestResult] = {}
    errors: dict[str, Exception] = {}
    threads: list[Thread] = []

    def stopping() -> bool:
        return stop_event.is_set() or external_should_stop()

    def run_controller(streamer: str) -> None:
        try:
            live_client = scheduler.client(
                priority=LIVE_PRIORITY,
                source_name=f"{streamer}:live",
            )
            backlog_client = scheduler.client(
                priority=BACKLOG_PRIORITY,
                source_name=f"{streamer}:backlog",
            )
            results[streamer] = controller(
                streamer,
                days,
                monitor=shared_monitor,
                build_storage=lambda: repositories,
                build_ingest=lambda: {
                    "embedder": base_embedder,
                    "live_embedder": live_client,
                    "backlog_embedder": backlog_client,
                },
                out=out,
                should_stop=stopping,
                provision_streamer=False,
            )
        except Exception as exc:
            errors[streamer] = exc
            out(f"failed mode=controller streamer={streamer} error={exc}")

    out(
        f"worker mode=multi_streamer streamers={','.join(normalized_streamers)} "
        f"model_load_ms={model_load_ms} inference_consumers=1"
    )
    try:
        for streamer in normalized_streamers:
            thread = Thread(
                target=run_controller,
                args=(streamer,),
                name=f"twitch-{streamer}",
                daemon=True,
            )
            threads.append(thread)
            thread.start()

        while any(thread.is_alive() for thread in threads) and not stopping():
            time.sleep(0.25)
    finally:
        stop_event.set()
        for thread in threads:
            thread.join()
        scheduler.close()

    if errors and not external_should_stop():
        details = "; ".join(f"{streamer}: {error}" for streamer, error in errors.items())
        raise RuntimeError(f"streamer controllers failed: {details}")

    return MultiStreamerIngestResult(
        streamers=tuple(normalized_streamers),
        results=results,
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configured_streamers = [*args.streamer, args.streamers]
    streamers = _normalize_streamers(
        configured_streamers
        if any(str(value).strip() for value in configured_streamers)
        else list(DEFAULT_STREAMERS)
    )
    try:
        if len(streamers) == 1:
            run_hybrid_ingest(streamers[0], args.days)
        else:
            run_multi_streamer_ingest(streamers, args.days)
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
