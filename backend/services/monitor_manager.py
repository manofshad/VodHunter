from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from queue import Empty, Queue
from threading import Event, Lock, Thread
import time
from typing import Any

from pipeline.embedder import Embedder
from pipeline.ingest_session import IngestSession
from services.twitch_eventsub import EventSubClient
from services.twitch_monitor import TwitchMonitor
from sources.live_archive_vod_source import LiveArchiveVODSource
from storage.vector_store import VectorStore


@dataclass
class MonitorStatus:
    state: str = "idle"
    streamer: str | None = None
    is_live: bool | None = None
    started_at: str | None = None
    last_check_at: str | None = None
    last_error: str | None = None
    current_video_id: int | None = None
    current_vod_url: str | None = None
    ingest_cursor_seconds: int | None = None
    lag_seconds: int | None = None
    eventsub_enabled: bool | None = None
    eventsub_health: str | None = None
    eventsub_last_event_at: str | None = None
    eventsub_last_error: str | None = None


class MonitorConflictError(Exception):
    pass


class MonitorManager:
    def __init__(
        self,
        store: VectorStore,
        embedder: Embedder,
        chunk_seconds: int,
        monitor_poll_seconds: float,
        session_poll_interval: float,
        monitor_retry_seconds: float,
        temp_dir: str,
        archive_lag_seconds: int,
        archive_poll_seconds: float,
        archive_finalize_checks: int,
        eventsub_client: EventSubClient,
        eventsub_callback_url: str,
        eventsub_secret: str,
        eventsub_reconcile_seconds: float,
        eventsub_fallback_poll_seconds: float,
    ):
        self.store = store
        self.embedder = embedder
        self.chunk_seconds = chunk_seconds
        self.monitor_poll_seconds = monitor_poll_seconds
        self.session_poll_interval = session_poll_interval
        self.monitor_retry_seconds = monitor_retry_seconds
        self.temp_dir = temp_dir
        self.archive_lag_seconds = archive_lag_seconds
        self.archive_poll_seconds = archive_poll_seconds
        self.archive_finalize_checks = archive_finalize_checks

        self.eventsub_client = eventsub_client
        self.eventsub_callback_url = eventsub_callback_url.strip()
        self.eventsub_secret = eventsub_secret.strip()
        self.eventsub_reconcile_seconds = max(float(eventsub_reconcile_seconds), 5.0)
        self.eventsub_fallback_poll_seconds = max(float(eventsub_fallback_poll_seconds), 5.0)

        self._status = MonitorStatus()
        self._lock = Lock()
        self._thread: Thread | None = None
        self._stop_event = Event()
        self._wake_event = Event()
        self._pending_events: Queue[str] = Queue()
        self._active_session: IngestSession | None = None
        self._active_source: LiveArchiveVODSource | None = None
        self._session_thread: Thread | None = None
        self._session_started_for_streamer: str | None = None
        self._next_reconcile_at = 0.0
        self._next_fallback_poll_at = 0.0

        self._monitor = TwitchMonitor.from_env()
        self._current_user_id: str | None = None

    def get_status(self) -> MonitorStatus:
        with self._lock:
            return MonitorStatus(**self._status.__dict__)

    def is_running(self) -> bool:
        with self._lock:
            return self._thread is not None and self._thread.is_alive()

    def can_search(self) -> bool:
        return self.get_status().state == "idle"

    def start(self, streamer: str) -> MonitorStatus:
        streamer = streamer.strip().lower()
        if not streamer:
            raise ValueError("streamer is required")

        with self._lock:
            already_running = self._thread is not None and self._thread.is_alive()
            if already_running:
                if self._status.streamer == streamer:
                    return MonitorStatus(**self._status.__dict__)
                raise MonitorConflictError(
                    f"Monitor already running for {self._status.streamer}. Stop first to switch."
                )

        try:
            user_id = self._monitor.get_user_id(streamer)
        except RuntimeError as exc:
            raise ValueError(str(exc)) from exc

        self._stop_event.clear()
        self._wake_event.clear()
        while True:
            try:
                self._pending_events.get_nowait()
            except Empty:
                break
        self._current_user_id = user_id
        now = time.time()
        self._next_reconcile_at = now + self.eventsub_reconcile_seconds
        self._next_fallback_poll_at = now

        eventsub_enabled = bool(self.eventsub_secret and self.eventsub_callback_url)
        eventsub_health = "unsubscribed"
        eventsub_last_error: str | None = None
        if not eventsub_enabled:
            eventsub_health = "degraded"
            eventsub_last_error = "EventSub callback URL or secret is not configured"
        else:
            try:
                self.eventsub_client.ensure_stream_subscriptions(
                    user_id=user_id,
                    callback_url=self.eventsub_callback_url,
                    secret=self.eventsub_secret,
                )
                eventsub_health = "healthy"
            except Exception as exc:
                eventsub_health = "degraded"
                eventsub_last_error = f"EventSub subscription setup failed: {exc}"

        with self._lock:
            self._status = MonitorStatus(
                state="polling",
                streamer=streamer,
                is_live=None,
                started_at=self._utc_now_iso(),
                last_check_at=None,
                last_error=None,
                current_video_id=None,
                current_vod_url=None,
                ingest_cursor_seconds=None,
                lag_seconds=self.archive_lag_seconds,
                eventsub_enabled=eventsub_enabled,
                eventsub_health=eventsub_health,
                eventsub_last_event_at=None,
                eventsub_last_error=eventsub_last_error,
            )

            self._thread = Thread(
                target=self._run_loop,
                args=(streamer,),
                daemon=True,
            )
            self._thread.start()
            return MonitorStatus(**self._status.__dict__)

    def stop(self) -> bool:
        with self._lock:
            running = self._thread is not None and self._thread.is_alive()
            if not running and self._status.state == "idle":
                return False

            self._stop_event.set()
            self._wake_event.set()
            session = self._active_session
            thread = self._thread
            session_thread = self._session_thread
            user_id = self._current_user_id

        if session is not None:
            session.stop()

        if session_thread is not None and session_thread.is_alive():
            session_thread.join(timeout=5)

        if thread is not None and thread.is_alive():
            thread.join(timeout=5)

        if user_id:
            try:
                self.eventsub_client.cleanup_for_broadcaster(user_id)
            except Exception:
                pass

        with self._lock:
            self._active_session = None
            self._active_source = None
            self._session_thread = None
            self._thread = None
            self._current_user_id = None
            self._session_started_for_streamer = None
            self._status = MonitorStatus()

        return True

    def on_stream_online(self, streamer: str, event_payload: dict[str, Any] | None = None) -> None:
        streamer = streamer.strip().lower()
        if not streamer:
            return
        with self._lock:
            if self._status.streamer != streamer:
                return
            self._status.is_live = True
            self._status.last_check_at = self._utc_now_iso()
            self._status.eventsub_last_event_at = self._utc_now_iso()
            self._pending_events.put("online")
            self._wake_event.set()

    def on_stream_offline(self, streamer: str, event_payload: dict[str, Any] | None = None) -> None:
        streamer = streamer.strip().lower()
        if not streamer:
            return
        with self._lock:
            if self._status.streamer != streamer:
                return
            self._status.is_live = False
            self._status.last_check_at = self._utc_now_iso()
            self._status.eventsub_last_event_at = self._utc_now_iso()
            self._pending_events.put("offline")
            self._wake_event.set()

    def note_eventsub_event(self) -> None:
        with self._lock:
            if self._status.state == "idle":
                return
            self._status.eventsub_last_event_at = self._utc_now_iso()

    def mark_eventsub_healthy(self) -> None:
        with self._lock:
            if self._status.state == "idle":
                return
            self._status.eventsub_health = "healthy"
            self._status.eventsub_last_error = None

    def mark_eventsub_degraded(self, reason: str) -> None:
        with self._lock:
            if self._status.state == "idle":
                return
            self._status.eventsub_health = "degraded"
            self._status.eventsub_last_error = reason.strip() or "EventSub is degraded"
            self._status.last_check_at = self._utc_now_iso()
        self._wake_event.set()

    def _run_loop(self, streamer: str) -> None:
        while not self._stop_event.is_set():
            self._drain_events(streamer)

            now = time.time()
            self._maybe_reconcile_eventsub(now)
            self._maybe_fallback_poll(streamer, now)
            self._refresh_session_status()

            self._wake_event.wait(timeout=1.0)
            self._wake_event.clear()

        self._stop_active_session()

    def _drain_events(self, streamer: str) -> None:
        while not self._stop_event.is_set():
            try:
                event = self._pending_events.get_nowait()
            except Empty:
                return

            if event == "online":
                self._start_ingest_if_needed(streamer)
            elif event == "offline":
                with self._lock:
                    if self._status.streamer == streamer and self._status.state != "idle":
                        has_active_session = self._session_thread is not None and self._session_thread.is_alive()
                        self._status.state = "ingesting" if has_active_session else "polling"
                        self._status.is_live = False
                        self._status.last_check_at = self._utc_now_iso()
                        self._status.last_error = None

    def _maybe_reconcile_eventsub(self, now: float) -> None:
        if now < self._next_reconcile_at:
            return
        self._next_reconcile_at = now + self.eventsub_reconcile_seconds

        with self._lock:
            streamer = self._status.streamer
            user_id = self._current_user_id
            enabled = bool(self._status.eventsub_enabled)
        if not streamer or not user_id or not enabled:
            return

        try:
            self.eventsub_client.ensure_stream_subscriptions(
                user_id=user_id,
                callback_url=self.eventsub_callback_url,
                secret=self.eventsub_secret,
            )
        except Exception as exc:
            self.mark_eventsub_degraded(f"EventSub reconcile failed: {exc}")

    def _maybe_fallback_poll(self, streamer: str, now: float) -> None:
        with self._lock:
            health = self._status.eventsub_health
            active_streamer = self._status.streamer
        if active_streamer != streamer:
            return
        if health not in {"degraded", "unsubscribed"}:
            return
        if now < self._next_fallback_poll_at:
            return
        self._next_fallback_poll_at = now + self.eventsub_fallback_poll_seconds

        try:
            is_live = self._monitor.is_live(streamer)
        except Exception as exc:
            with self._lock:
                if self._status.streamer == streamer:
                    self._status.state = "error"
                    self._status.last_error = str(exc)
                    self._status.last_check_at = self._utc_now_iso()
            return

        with self._lock:
            has_active_session = self._session_thread is not None and self._session_thread.is_alive()
            if self._status.streamer == streamer and self._status.state != "idle":
                self._status.state = "ingesting" if has_active_session else "polling"
                self._status.is_live = is_live
                self._status.last_error = None
                self._status.last_check_at = self._utc_now_iso()

        if is_live:
            self._start_ingest_if_needed(streamer)

    def _start_ingest_if_needed(self, streamer: str) -> None:
        with self._lock:
            session_thread = self._session_thread
            if session_thread is not None and session_thread.is_alive():
                return
            if self._status.streamer != streamer:
                return

        source = LiveArchiveVODSource(
            streamer=streamer,
            store=self.store,
            twitch_monitor=self._monitor,
            chunk_seconds=self.chunk_seconds,
            lag_seconds=self.archive_lag_seconds,
            poll_seconds=self.archive_poll_seconds,
            finalize_checks=self.archive_finalize_checks,
            temp_dir=self.temp_dir,
        )
        session = IngestSession(
            source=source,
            embedder=self.embedder,
            store=self.store,
            poll_interval=self.session_poll_interval,
        )
        session_thread = Thread(
            target=self._run_session,
            args=(streamer, session, source),
            daemon=True,
        )

        with self._lock:
            self._active_session = session
            self._active_source = source
            self._session_thread = session_thread
            self._session_started_for_streamer = streamer
            self._status.state = "ingesting"
            self._status.is_live = True
            self._status.last_error = None
            self._status.lag_seconds = self.archive_lag_seconds
            self._status.last_check_at = self._utc_now_iso()
        session_thread.start()

    def _stop_active_session(self) -> None:
        with self._lock:
            session = self._active_session
            source = self._active_source
            thread = self._session_thread

        if session is not None:
            session.stop()
        if thread is not None and thread.is_alive():
            thread.join(timeout=5)

        with self._lock:
            if source is not None and self._status.state != "idle":
                self._status.current_video_id = source.video_id
                self._status.current_vod_url = source.current_vod_url
                self._status.ingest_cursor_seconds = source.ingest_cursor_seconds
                self._status.lag_seconds = self.archive_lag_seconds
            self._active_session = None
            self._active_source = None
            self._session_thread = None
            self._session_started_for_streamer = None

    def _run_session(
        self,
        streamer: str,
        session: IngestSession,
        source: LiveArchiveVODSource,
    ) -> None:
        try:
            session.run()
        except Exception as exc:
            with self._lock:
                if self._status.streamer == streamer and self._status.state != "idle":
                    self._status.state = "error"
                    self._status.last_error = str(exc)
                    self._status.last_check_at = self._utc_now_iso()
                    self._status.current_video_id = source.video_id
                    self._status.current_vod_url = source.current_vod_url
                    self._status.ingest_cursor_seconds = source.ingest_cursor_seconds
            return
        finally:
            with self._lock:
                if self._active_session is session:
                    self._active_session = None
                if self._active_source is source:
                    self._active_source = None
                if self._session_started_for_streamer == streamer:
                    self._session_thread = None
                    self._session_started_for_streamer = None

        with self._lock:
            if self._status.streamer == streamer and self._status.state != "idle":
                self._status.state = "polling"
                self._status.last_error = None
                self._status.last_check_at = self._utc_now_iso()
                self._status.current_video_id = source.video_id
                self._status.current_vod_url = source.current_vod_url
                self._status.ingest_cursor_seconds = source.ingest_cursor_seconds
                self._status.lag_seconds = self.archive_lag_seconds

    def _refresh_session_status(self) -> None:
        with self._lock:
            source = self._active_source
            if source is None:
                return
            self._status.current_video_id = source.video_id
            self._status.current_vod_url = source.current_vod_url
            self._status.ingest_cursor_seconds = source.ingest_cursor_seconds

    def _utc_now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()
