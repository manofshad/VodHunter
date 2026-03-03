import os

from backend import config
from backend.services.eventsub_handler import EventSubHandler
from backend.services.monitor_manager import MonitorManager
from backend.services.remote_clip_downloader import RemoteClipDownloader
from backend.services.search_manager import SearchManager
from backend.services.session_query import SessionQueryService
from pipeline.embedder import Embedder
from search.alignment_service import AlignmentConfig, AlignmentService
from search.query_embedder import QueryEmbedder
from search.query_preprocessor import QueryPreprocessor
from search.search_service import SearchService
from search.vector_matcher import VectorMatcher
from services.twitch_eventsub import EventSubClient
from services.twitch_monitor import TwitchMonitor
from storage.vector_store import VectorStore


def prepare_runtime_dirs() -> None:
    os.makedirs(config.DATA_DIR, exist_ok=True)
    os.makedirs(config.TEMP_SEARCH_UPLOAD_DIR, exist_ok=True)
    os.makedirs(config.TEMP_SEARCH_DOWNLOAD_DIR, exist_ok=True)


def build_common_state() -> dict[str, object]:
    config.validate_storage_config()

    store = VectorStore(
        database_url=config.DATABASE_URL,
        vector_dim=config.VECTOR_DIM,
        pgvector_probes=config.PGVECTOR_PROBES,
    )
    store.init_db()

    embedder = Embedder()
    return {
        "store": store,
        "embedder": embedder,
    }


def build_search_stack(store: VectorStore, embedder: Embedder) -> dict[str, object]:
    search_service = SearchService(
        store=store,
        preprocessor=QueryPreprocessor(temp_dir=config.TEMP_SEARCH_PREPROCESS_DIR),
        query_embedder=QueryEmbedder(embedder=embedder),
        matcher=VectorMatcher(top_k=10),
        alignment=AlignmentService(
            store=store,
            config=AlignmentConfig(min_vote_count=3, min_vote_ratio=0.08),
        ),
    )

    search_manager = SearchManager(
        search_service=search_service,
        upload_temp_dir=config.TEMP_SEARCH_UPLOAD_DIR,
        remote_downloader=RemoteClipDownloader(
            temp_dir=config.TEMP_SEARCH_DOWNLOAD_DIR,
            timeout_seconds=config.TIKTOK_DOWNLOAD_TIMEOUT_SECONDS,
            max_file_mb=config.TIKTOK_MAX_FILE_MB,
        ),
    )

    return {
        "search_service": search_service,
        "search_manager": search_manager,
    }


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
