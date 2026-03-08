from backend import config
from backend.services.eventsub_handler import EventSubHandler
from backend.services.monitor_manager import MonitorManager
from backend.services.session_query import SessionQueryService
from pipeline.embedder import Embedder
from services.twitch_eventsub import EventSubClient
from services.twitch_monitor import TwitchMonitor
from storage.vector_store import VectorStore


def build_monitor_stack(store: VectorStore, embedder: Embedder) -> dict[str, object]:
    monitor_manager = MonitorManager(
        store=store,
        embedder=embedder,
        chunk_seconds=config.INGEST_CHUNK_SECONDS,
        monitor_poll_seconds=config.MONITOR_POLL_SECONDS,
        session_poll_interval=config.SESSION_POLL_INTERVAL,
        monitor_retry_seconds=config.MONITOR_RETRY_SECONDS,
        temp_dir=config.TEMP_LIVE_DIR,
        archive_lag_seconds=config.LIVE_ARCHIVE_LAG_SECONDS,
        archive_poll_seconds=config.LIVE_ARCHIVE_POLL_SECONDS,
        archive_finalize_checks=config.LIVE_ARCHIVE_FINALIZE_CHECKS,
        eventsub_client=EventSubClient(TwitchMonitor.from_env()),
        eventsub_callback_url=config.TWITCH_EVENTSUB_CALLBACK_URL,
        eventsub_secret=config.TWITCH_EVENTSUB_SECRET,
        eventsub_reconcile_seconds=config.EVENTSUB_RECONCILE_SECONDS,
        eventsub_fallback_poll_seconds=config.EVENTSUB_FALLBACK_POLL_SECONDS,
    )
    eventsub_handler = EventSubHandler(
        monitor_manager=monitor_manager,
        secret=config.TWITCH_EVENTSUB_SECRET,
        message_ttl_seconds=config.EVENTSUB_MESSAGE_TTL_SECONDS,
        max_clock_skew_seconds=config.EVENTSUB_MAX_CLOCK_SKEW_SECONDS,
    )
    session_query = SessionQueryService(store=store)

    return {
        "monitor_manager": monitor_manager,
        "eventsub_handler": eventsub_handler,
        "session_query": session_query,
    }
