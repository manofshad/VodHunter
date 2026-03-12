import os
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
VECTOR_DIM = int(os.getenv("VECTOR_DIM", "768"))
HNSW_EF_SEARCH = int(os.getenv("HNSW_EF_SEARCH", "40"))
TEMP_LIVE_DIR = str(DATA_DIR / "temp_live_chunks")
TEMP_BACKFILL_DIR = str(DATA_DIR / "temp_backfill_chunks")
TEMP_SEARCH_DOWNLOAD_DIR = str(DATA_DIR / "temp_search_downloads")
TEMP_SEARCH_PREPROCESS_DIR = str(DATA_DIR / "temp_search")
TIKTOK_DOWNLOAD_TIMEOUT_SECONDS = 90
TIKTOK_MAX_FILE_MB = 200
SEARCH_MAX_DURATION_SECONDS_PUBLIC = 180
SEARCH_MAX_DURATION_SECONDS_ADMIN: int | None = None

INGEST_CHUNK_SECONDS = 60
MONITOR_POLL_SECONDS = 30.0
SESSION_POLL_INTERVAL = 0.5
MONITOR_RETRY_SECONDS = 5.0

LIVE_ARCHIVE_LAG_SECONDS = 120
LIVE_ARCHIVE_POLL_SECONDS = 15.0
LIVE_ARCHIVE_FINALIZE_CHECKS = 3

LIVE_SESSIONS_DEFAULT_LIMIT = 50
LIVE_SESSIONS_MAX_LIMIT = 200

TWITCH_EVENTSUB_SECRET = os.getenv("TWITCH_EVENTSUB_SECRET", "").strip()
TWITCH_EVENTSUB_CALLBACK_URL = os.getenv("TWITCH_EVENTSUB_CALLBACK_URL", "").strip()
EVENTSUB_RECONCILE_SECONDS = float(os.getenv("EVENTSUB_RECONCILE_SECONDS", "300"))
EVENTSUB_FALLBACK_POLL_SECONDS = float(os.getenv("EVENTSUB_FALLBACK_POLL_SECONDS", "120"))
EVENTSUB_MESSAGE_TTL_SECONDS = int(os.getenv("EVENTSUB_MESSAGE_TTL_SECONDS", "600"))
EVENTSUB_MAX_CLOCK_SKEW_SECONDS = int(os.getenv("EVENTSUB_MAX_CLOCK_SKEW_SECONDS", "600"))
PUBLIC_API_ORIGIN = os.getenv("PUBLIC_API_ORIGIN", "").strip()
ADMIN_API_ORIGIN = os.getenv("ADMIN_API_ORIGIN", "").strip()
SEARCH_QUERY_EMBEDDER_BACKEND = os.getenv("SEARCH_QUERY_EMBEDDER_BACKEND", "local").strip().lower()
MODAL_SEARCH_APP_NAME = os.getenv("MODAL_SEARCH_APP_NAME", "").strip()
MODAL_SEARCH_FUNCTION_NAME = os.getenv("MODAL_SEARCH_FUNCTION_NAME", "").strip()
MODAL_SEARCH_TIMEOUT_SECONDS = float(os.getenv("MODAL_SEARCH_TIMEOUT_SECONDS", "60"))
MODAL_SEARCH_MODEL_NAME = os.getenv("MODAL_SEARCH_MODEL_NAME", "").strip()


def validate_storage_config() -> None:
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL is required")


def validate_search_embedder_config() -> None:
    if SEARCH_QUERY_EMBEDDER_BACKEND not in {"local", "modal"}:
        raise ValueError("SEARCH_QUERY_EMBEDDER_BACKEND must be one of: local, modal")

    if SEARCH_QUERY_EMBEDDER_BACKEND != "modal":
        return

    if not MODAL_SEARCH_APP_NAME:
        raise ValueError("MODAL_SEARCH_APP_NAME is required when SEARCH_QUERY_EMBEDDER_BACKEND=modal")
    if not MODAL_SEARCH_FUNCTION_NAME:
        raise ValueError("MODAL_SEARCH_FUNCTION_NAME is required when SEARCH_QUERY_EMBEDDER_BACKEND=modal")
    if MODAL_SEARCH_TIMEOUT_SECONDS <= 0:
        raise ValueError("MODAL_SEARCH_TIMEOUT_SECONDS must be greater than 0")
    if not os.getenv("MODAL_TOKEN_ID", "").strip():
        raise ValueError("MODAL_TOKEN_ID is required when SEARCH_QUERY_EMBEDDER_BACKEND=modal")
    if not os.getenv("MODAL_TOKEN_SECRET", "").strip():
        raise ValueError("MODAL_TOKEN_SECRET is required when SEARCH_QUERY_EMBEDDER_BACKEND=modal")
