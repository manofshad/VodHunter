from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipeline.embedder import Embedder
from services.twitch_monitor import TwitchMonitor
from storage.vector_store import VectorStore
from sources.live_archive_vod_source import LiveArchiveVODSource
from pipeline.ingest_session import IngestSession


DATA_DIR = ROOT_DIR / "data"
DB_PATH = str(DATA_DIR / "metadata.db")
VECTOR_FILE = str(DATA_DIR / "vectors.npy")
ID_FILE = str(DATA_DIR / "ids.npy")
TEMP_DIR = str(DATA_DIR / "temp_live_chunks")


def main() -> None:
    streamer = "thesketchreal"
    chunk_seconds = 60
    lag_seconds = 120
    poll_seconds = 15.0
    finalize_checks = 3

    store = VectorStore(
        db_path=DB_PATH,
        vector_file=VECTOR_FILE,
        id_file=ID_FILE,
    )
    store.init_db()

    embedder = Embedder()
    monitor = TwitchMonitor.from_env()

    source = LiveArchiveVODSource(
        streamer=streamer,
        store=store,
        twitch_monitor=monitor,
        chunk_seconds=chunk_seconds,
        lag_seconds=lag_seconds,
        poll_seconds=poll_seconds,
        finalize_checks=finalize_checks,
        temp_dir=TEMP_DIR,
    )

    session = IngestSession(
        source=source,
        embedder=embedder,
        store=store,
        poll_interval=0.5,
    )

    print(f"Starting archive-backed live ingest for twitch.tv/{streamer}")
    print("Press Ctrl+C to stop.")

    try:
        session.run()
    except KeyboardInterrupt:
        session.stop()
        print("\nStopped by user.")


if __name__ == "__main__":
    main()
