from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Event, Lock, Thread
import time

from pipeline.embedder import Embedder
from pipeline.ingest_session import IngestSession
from services.twitch_monitor import TwitchMonitor
from sources.live_twitch_source import LiveTwitchSource
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
    ):
        self.store = store
        self.embedder = embedder
        self.chunk_seconds = chunk_seconds
        self.monitor_poll_seconds = monitor_poll_seconds
        self.session_poll_interval = session_poll_interval
        self.monitor_retry_seconds = monitor_retry_seconds
        self.temp_dir = temp_dir

        self._status = MonitorStatus()
        self._lock = Lock()
        self._thread: Thread | None = None
        self._stop_event = Event()
        self._active_session: IngestSession | None = None

        self._monitor = TwitchMonitor.from_env()

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

            self._stop_event.clear()
            self._status = MonitorStatus(
                state="polling",
                streamer=streamer,
                is_live=None,
                started_at=self._utc_now_iso(),
                last_check_at=None,
                last_error=None,
                current_video_id=None,
            )

            self._thread = Thread(
                target=self._run_loop,
                args=(streamer,),
                daemon=True,
            )
            self._thread.start()
            return MonitorStatus(**self._status.__dict__)

    def stop(self) -> bool:
        session = None
        thread = None

        with self._lock:
            running = self._thread is not None and self._thread.is_alive()
            if not running and self._status.state == "idle":
                return False

            self._stop_event.set()
            session = self._active_session
            thread = self._thread

        if session is not None:
            session.stop()

        if thread is not None:
            thread.join(timeout=5)

        with self._lock:
            self._active_session = None
            self._thread = None
            self._status = MonitorStatus()

        return True

    def _run_loop(self, streamer: str) -> None:
        while not self._stop_event.is_set():
            try:
                is_live = self._monitor.is_live(streamer)
                self._set_status(
                    state="polling",
                    streamer=streamer,
                    is_live=is_live,
                    last_check_at=self._utc_now_iso(),
                    last_error=None,
                )
            except Exception as exc:
                self._set_status(
                    state="error",
                    streamer=streamer,
                    last_check_at=self._utc_now_iso(),
                    last_error=str(exc),
                    is_live=None,
                )
                time.sleep(self.monitor_retry_seconds)
                continue

            if not is_live:
                time.sleep(self.monitor_poll_seconds)
                continue

            source = LiveTwitchSource(
                streamer=streamer,
                chunk_seconds=self.chunk_seconds,
                temp_dir=self.temp_dir,
                db_path=self.store.db_path,
            )
            session = IngestSession(
                source=source,
                embedder=self.embedder,
                store=self.store,
                poll_interval=self.session_poll_interval,
            )

            with self._lock:
                self._active_session = session
                self._status.state = "ingesting"

            try:
                session.run()
                self._set_status(
                    state="polling",
                    streamer=streamer,
                    is_live=False,
                    current_video_id=source.video_id,
                    last_error=None,
                    last_check_at=self._utc_now_iso(),
                )
            except Exception as exc:
                self._set_status(
                    state="error",
                    streamer=streamer,
                    last_error=str(exc),
                    last_check_at=self._utc_now_iso(),
                )
            finally:
                with self._lock:
                    self._active_session = None

            if self._stop_event.is_set():
                break

            time.sleep(self.monitor_retry_seconds)

        with self._lock:
            self._thread = None
            if self._status.state != "idle":
                self._status.state = "idle"
                self._status.is_live = None
                self._status.streamer = None
                self._status.current_video_id = None

    def _set_status(self, **updates: object) -> None:
        with self._lock:
            for key, value in updates.items():
                setattr(self._status, key, value)

    def _utc_now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()
