import os
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
VECTOR_DIM = int(os.getenv("VECTOR_DIM", "128"))
HNSW_EF_SEARCH = int(os.getenv("HNSW_EF_SEARCH", "40"))
TEMP_LIVE_DIR = str(DATA_DIR / "temp_live_chunks")
TEMP_BACKFILL_DIR = str(DATA_DIR / "temp_backfill_chunks")
TEMP_SEARCH_UPLOAD_DIR = str(DATA_DIR / "temp_search_uploads")
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
MODAL_SEARCH_APP_NAME = os.getenv("MODAL_SEARCH_APP_NAME", "").strip()
MODAL_SEARCH_FUNCTION_NAME = os.getenv("MODAL_SEARCH_FUNCTION_NAME", "").strip()
MODAL_SEARCH_TIMEOUT_SECONDS = float(os.getenv("MODAL_SEARCH_TIMEOUT_SECONDS", "60"))
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY", "").strip()

NMFP_CANONICAL_MODEL_VERSION = (
    "nmfp-triplet@15c6f3bcdf6a6da1daddfe47a1ffa5a0d22deadc"
    "+zenodo-15719945+ckpt-100"
)
NMFP_CANONICAL_PREPROCESSING_VERSION = "nmfp-8khz-mono-1s-hop0.5-mel-v1"
NMFP_MODEL_VERSION = os.getenv(
    "NMFP_MODEL_VERSION", NMFP_CANONICAL_MODEL_VERSION
).strip()
NMFP_PREPROCESSING_VERSION = os.getenv(
    "NMFP_PREPROCESSING_VERSION", NMFP_CANONICAL_PREPROCESSING_VERSION
).strip()
NMFP_SAMPLE_RATE = int(os.getenv("NMFP_SAMPLE_RATE", "8000"))
NMFP_WINDOW_SECONDS = float(os.getenv("NMFP_WINDOW_SECONDS", "1.0"))
NMFP_HOP_SECONDS = float(os.getenv("NMFP_HOP_SECONDS", "0.5"))

SEARCH_TOP_K = int(os.getenv("SEARCH_TOP_K", "10"))
CUT_OFFSET_BIN_SECONDS = float(os.getenv("CUT_OFFSET_BIN_SECONDS", "0.5"))
CUT_OFFSET_TOLERANCE_SECONDS = float(os.getenv("CUT_OFFSET_TOLERANCE_SECONDS", "1.0"))
CUT_MAX_UNMATCHED_GAP_SECONDS = float(os.getenv("CUT_MAX_UNMATCHED_GAP_SECONDS", "2.0"))
CUT_MIN_SUPPORT = int(os.getenv("CUT_MIN_SUPPORT", "6"))
CUT_MIN_SEGMENT_DURATION_SECONDS = float(os.getenv("CUT_MIN_SEGMENT_DURATION_SECONDS", "4.0"))
CUT_MIN_DENSITY = float(os.getenv("CUT_MIN_DENSITY", "0.4"))
CUT_MERGE_QUERY_GAP_SECONDS = float(os.getenv("CUT_MERGE_QUERY_GAP_SECONDS", "1.0"))
CUT_MERGE_OFFSET_TOLERANCE_SECONDS = float(os.getenv("CUT_MERGE_OFFSET_TOLERANCE_SECONDS", "4.0"))
CUT_MAX_SEGMENTS = int(os.getenv("CUT_MAX_SEGMENTS", "12"))


def validate_storage_config() -> None:
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL is required")
    if VECTOR_DIM != 128:
        raise ValueError("VECTOR_DIM must be 128 for the pinned NMFP-triplet model")


def validate_nmfp_config() -> None:
    if NMFP_MODEL_VERSION != NMFP_CANONICAL_MODEL_VERSION:
        raise ValueError("NMFP_MODEL_VERSION does not match the production index")
    if NMFP_PREPROCESSING_VERSION != NMFP_CANONICAL_PREPROCESSING_VERSION:
        raise ValueError("NMFP_PREPROCESSING_VERSION does not match the production index")
    if NMFP_SAMPLE_RATE != 8000:
        raise ValueError("NMFP_SAMPLE_RATE must be 8000 for the pinned model")
    if NMFP_WINDOW_SECONDS != 1.0:
        raise ValueError("NMFP_WINDOW_SECONDS must be 1.0 for the pinned model")
    if NMFP_HOP_SECONDS != 0.5:
        raise ValueError("NMFP_HOP_SECONDS must be 0.5 for the production index")


def validate_modal_search_config() -> None:
    if not MODAL_SEARCH_APP_NAME:
        raise ValueError("MODAL_SEARCH_APP_NAME is required")
    if not MODAL_SEARCH_FUNCTION_NAME:
        raise ValueError("MODAL_SEARCH_FUNCTION_NAME is required")
    if MODAL_SEARCH_TIMEOUT_SECONDS <= 0:
        raise ValueError("MODAL_SEARCH_TIMEOUT_SECONDS must be greater than 0")
    if not os.getenv("MODAL_TOKEN_ID", "").strip():
        raise ValueError("MODAL_TOKEN_ID is required")
    if not os.getenv("MODAL_TOKEN_SECRET", "").strip():
        raise ValueError("MODAL_TOKEN_SECRET is required")
