from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import random
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
from services.twitch_monitor import CachedMultiStreamerTwitchMonitor, TwitchMonitor, TwitchRequestError
from sources.historical_archive_vod_source import HistoricalArchiveVODSource
from sources.live_archive_vod_source import LiveArchiveVODSource
from storage.records import VodIngestStateRecord, VideoRecord, VideoStatus
from storage.repositories import Repositories


INGEST_CHUNK_SECONDS = 60
MONITOR_POLL_SECONDS = 30.0
SESSION_POLL_INTERVAL = 0.5
MONITOR_RETRY_SECONDS = 5.0
MAX_RETRY_SECONDS = 60.0
CONTROLLER_HEARTBEAT_SECONDS = 60.0
CONTROLLER_DEGRADED_FAILURES = 3
LIVE_ARCHIVE_LAG_SECONDS = 120
LIVE_ARCHIVE_POLL_SECONDS = 15.0
LIVE_ARCHIVE_FINALIZE_CHECKS = 3
SAVED_CURSOR_STALE_SECONDS = 10 * 60
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


@dataclass
class RecoveryFailure:
    attempts: int
    twitch_request: bool


class ControllerRecovery:
    def __init__(
        self, streamer: str, out: Callable[[str], None],
        should_stop: Callable[[], bool], retry_seconds: float,
    ) -> None:
        self.streamer = streamer
        self.out = out
        self.should_stop = should_stop
        self.retry_seconds = max(0.0, float(retry_seconds))
        self.failures: dict[str, RecoveryFailure] = {}

    def succeeded(self, phase: str, *, twitch_only: bool = False) -> None:
        failure = self.failures.get(phase)
        if failure is not None and (not twitch_only or failure.twitch_request):
            del self.failures[phase]
            self.out(f"event=controller_recovered streamer={self.streamer} phase={phase}")

    def wait(self, error: Exception, phase: str) -> None:
        if isinstance(error, TwitchRequestError) and not error.retryable:
            raise error
        previous = self.failures.get(phase)
        attempts = (previous.attempts if previous else 0) + 1
        self.failures[phase] = RecoveryFailure(attempts, isinstance(error, TwitchRequestError))
        ceiling = min(MAX_RETRY_SECONDS, self.retry_seconds * 2 ** min(attempts - 1, 16))
        delay = random.uniform(0.8 * ceiling, ceiling)
        details = f"error={type(error).__name__} error_class={type(error).__name__}"
        if isinstance(error, TwitchRequestError):
            delay = max(delay, (error.retry_at or 0.0) - time.time())
            details = f"error={error.error_class} {error}"
        self.out(
            f"event=controller_retry streamer={self.streamer} phase={phase} "
            f"{details} retry_seconds={delay:.3f} failures={attempts} "
            f"degraded={str(attempts >= CONTROLLER_DEGRADED_FAILURES).lower()}"
        )
        _sleep_interruptibly(delay, self.should_stop)


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

    result = HybridIngestResult()
    logged_skipped_vods: set[str] = set()
    last_mode: str | None = None
    should_log_watch = True
    force_live_handoff = False
    creator_metadata = None
    recovery = ControllerRecovery(normalized_streamer, out, should_stop, retry_seconds)
    heartbeats: dict[str, float] = {}

    def heartbeat(event: str, phase: str, detail: str = "") -> None:
        now = time.monotonic()
        if now - heartbeats.get(event, float("-inf")) >= CONTROLLER_HEARTBEAT_SECONDS:
            out(f"event={event} streamer={normalized_streamer} phase={phase} {detail}".rstrip())
            heartbeats[event] = now

    def live_progress(cursor: int) -> None:
        recovery.succeeded("live_session")
        heartbeat("controller_progress", "live_session", f"cursor={cursor}")

    def live_poll() -> None:
        recovery.succeeded("live_session", twitch_only=True)
        recovery.succeeded("archive_discovery", twitch_only=True)
        heartbeat("controller_poll", "live_session")

    def backlog_progress(cursor: int) -> None:
        recovery.succeeded("backlog_session")
        heartbeat("controller_progress", "backlog_session", f"cursor={cursor}")

    out(f"event=controller_started streamer={normalized_streamer} phase=provision")

    while not should_stop():
        phase = "provision"
        try:
            if creator_metadata is None:
                if provision_streamer:
                    creator_metadata = _provision_streamer(
                        streamer=normalized_streamer,
                        repositories=repositories,
                        twitch_monitor=twitch_monitor,
                    )
                else:
                    creator_metadata = twitch_monitor.get_user_profile(normalized_streamer)
                out(
                    f"provisioned streamer={normalized_streamer} user_id={creator_metadata['id']} "
                    f"partition={creator_metadata.get('partition_name', 'unmanaged')}"
                )
                recovery.succeeded(phase)
            user_id = str(creator_metadata["id"])
            phase = "live_check"
            if force_live_handoff:
                is_live = True
                force_live_handoff = False
            else:
                is_live = twitch_monitor.is_live(normalized_streamer)
                recovery.succeeded(phase)

            if is_live:
                phase = "live_session"
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
                    progress=live_progress,
                    poll_succeeded=live_poll,
                )
                if live_error is not None:
                    result.failed += 1
                    recovery.wait(live_error, phase)
                    continue
                recovery.succeeded(phase)
                if live_handle.interrupted:
                    break

                result.handoffs_to_backlog += 1
                out(f"handoff event=live_to_backlog streamer={normalized_streamer}")
                last_mode = "backlog"
                continue

            phase = "archive_discovery"
            backlog = _build_backlog(
                twitch_monitor=twitch_monitor,
                videos=videos,
                ingest_states=ingest_states,
                streamer=normalized_streamer,
                user_id=user_id,
                days=days,
                skipped_vods_logged=logged_skipped_vods,
                out=out,
            )
            recovery.succeeded(phase)

            if backlog:
                phase = "backlog_session"
                candidate = backlog[0]
                vod = candidate.vod
                vod_id = str(vod["id"])
                if last_mode != "backlog" or should_log_watch:
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
                    progress=backlog_progress,
                    poll_succeeded=lambda: heartbeat("controller_poll", "backlog_session"),
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
                    recovery.wait(backlog_outcome.error, phase)
                    continue

                recovery.succeeded(phase)
                result.backlog_ingested += 1
                out(f"completed mode=backlog vod={vod_id} url={vod['url']}")
                heartbeat("controller_progress", phase)
                continue

            result.watch_cycles += 1
            if last_mode != "watch" or should_log_watch:
                out(f"mode=watch streamer={normalized_streamer} backlog=0 is_live=false")
            heartbeat("controller_poll", "watch")
            last_mode = "watch"
            should_log_watch = False
            _sleep_interruptibly(watch_poll_seconds, should_stop)
        except TwitchRequestError as exc:
            recovery.wait(exc, phase)

    return result


def _build_backlog(
    *,
    twitch_monitor: TwitchMonitor,
    videos: object,
    ingest_states: object,
    streamer: str,
    user_id: str,
    days: int,
    skipped_vods_logged: set[str],
    out: Callable[[str], None],
) -> list[BacklogCandidate]:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=int(days))
    vods = twitch_monitor.list_archive_vods_since(user_id=user_id, created_after=cutoff)
    by_id = {str(vod["id"]): vod for vod in vods}
    stale_before = now - timedelta(seconds=SAVED_CURSOR_STALE_SECONDS)
    for saved in ingest_states.list_pending(streamer):
        vod_id = saved.vod_platform_id
        if vod_id in by_id:
            continue
        try:
            vod = twitch_monitor.get_archive_vod_by_id(vod_id, expected_user_id=user_id)
            if vod is not None:
                if int(vod["duration_seconds"]) < saved.last_ingested_seconds:
                    raise RuntimeError("Twitch duration is smaller than the saved cursor")
                by_id[vod_id] = vod
            elif videos.mark_source_unavailable(saved, stale_before=stale_before):
                out(
                    f"event=source_unavailable streamer={streamer} vod={vod_id} "
                    f"cursor={saved.last_ingested_seconds} "
                    f"observed_seconds={saved.last_seen_duration_seconds}"
                )
        except TwitchRequestError:
            raise
        except Exception as exc:
            out(f"mode=backlog streamer={streamer} vod={vod_id} error=source_lookup_failed error_class={type(exc).__name__}")
    backlog: list[BacklogCandidate] = []

    for vod in by_id.values():
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

        if existing_status in {VideoStatus.DELETED, VideoStatus.SEARCHABLE, VideoStatus.UNAVAILABLE}:
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
        ),
        reverse=True,
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
    progress: Callable[[int], None] = lambda cursor: None,
    poll_succeeded: Callable[[], None] = lambda: None,
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
        twitch_monitor=twitch_monitor,
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
    last_cursor = starting_cursor

    def sample_progress() -> None:
        nonlocal last_cursor
        cursor = getattr(source, "ingest_cursor_seconds", starting_cursor)
        if cursor > last_cursor:
            progress(cursor)
            last_cursor = cursor

    try:
        while handle.thread.is_alive():
            if should_stop():
                handle.interrupted = True
                break
            sample_progress()
            try:
                is_live = twitch_monitor.is_live(streamer)
                poll_succeeded()
                if is_live:
                    if not handle.thread.is_alive():
                        break
                    preempted_for_live = True
                    out(f"handoff_requested event=backlog_to_live vod={vod_id}")
                    break
            except TwitchRequestError as exc:
                if not exc.retryable:
                    raise
                out(
                    f"event=controller_retry streamer={streamer} phase=backlog_live_check "
                    f"error={exc.error_class} {exc} retry_seconds={backlog_live_poll_seconds:.3f} "
                    f"rate_limit_at={exc.retry_at or 'none'}"
                )
            _sleep_interruptibly(backlog_live_poll_seconds, should_stop)
    finally:
        if handle.thread.is_alive():
            handle.session.stop()
        handle.thread.join()
    sample_progress()
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
    progress: Callable[[int], None] = lambda cursor: None,
    poll_succeeded: Callable[[], None] = lambda: None,
) -> Exception | None:
    last_cursor = None
    last_poll = None

    def sample_progress() -> None:
        nonlocal last_cursor, last_poll
        successful_poll = getattr(handle.source, "last_successful_poll_at", None)
        cursor = getattr(handle.source, "ingest_cursor_seconds", 0) or 0
        if successful_poll is not None:
            if last_cursor is not None and cursor > last_cursor:
                progress(cursor)
            last_cursor = cursor
        if successful_poll is not None and successful_poll != last_poll:
            poll_succeeded()
            last_poll = successful_poll

    try:
        while handle.thread.is_alive():
            if should_stop():
                handle.interrupted = True
                break
            sample_progress()
            _sleep_interruptibly(wait_seconds, should_stop)
    finally:
        if handle.thread.is_alive():
            handle.session.stop()
        handle.thread.join()
    sample_progress()
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
        recovery = ControllerRecovery(streamer, out, stopping, MONITOR_RETRY_SECONDS)
        live_client = scheduler.client(priority=LIVE_PRIORITY, source_name=f"{streamer}:live")
        backlog_client = scheduler.client(priority=BACKLOG_PRIORITY, source_name=f"{streamer}:backlog")
        while not stopping():
            started_at = time.monotonic()
            out(f"event=controller_start streamer={streamer} phase=controller")

            def controller_out(line: str) -> None:
                out(line)
                if (
                    line.startswith(("event=controller_poll ", "event=controller_progress "))
                    and time.monotonic() - started_at >= CONTROLLER_HEARTBEAT_SECONDS
                ):
                    recovery.succeeded("controller")

            try:
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
                    out=controller_out,
                    should_stop=stopping,
                    provision_streamer=True,
                )
                if stopping():
                    break
                error = RuntimeError("controller exited unexpectedly")
            except TwitchRequestError as exc:
                if not exc.retryable:
                    errors[streamer] = exc
                    out(f"event=controller_failed streamer={streamer} phase=controller permanent=true error={exc.error_class} {exc}")
                    return
                error = exc
            except Exception as exc:
                error = exc
            recovery.wait(error, "controller")

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
        details = "; ".join(f"{streamer}: {type(error).__name__}" for streamer, error in errors.items())
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
