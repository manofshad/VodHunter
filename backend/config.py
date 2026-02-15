from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"

DB_PATH = str(DATA_DIR / "metadata.db")
VECTOR_FILE = str(DATA_DIR / "vectors.npy")
ID_FILE = str(DATA_DIR / "ids.npy")
TEMP_LIVE_DIR = str(DATA_DIR / "temp_live_chunks")
TEMP_SEARCH_UPLOAD_DIR = str(DATA_DIR / "temp_search_uploads")
TEMP_SEARCH_PREPROCESS_DIR = str(DATA_DIR / "temp_search")

INGEST_CHUNK_SECONDS = 60
MONITOR_POLL_SECONDS = 30.0
SESSION_POLL_INTERVAL = 0.5
MONITOR_RETRY_SECONDS = 5.0

LIVE_SESSIONS_DEFAULT_LIMIT = 50
LIVE_SESSIONS_MAX_LIMIT = 200
