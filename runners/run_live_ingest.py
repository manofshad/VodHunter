from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipeline.embedder import Embedder
from storage.vector_store import VectorStore
from sources.live_twitch_source import LiveTwitchSource
from pipeline.ingest_session import IngestSession


DATA_DIR = ROOT_DIR / "data"
DB_PATH = str(DATA_DIR / "metadata.db")
VECTOR_FILE = str(DATA_DIR / "vectors.npy")
ID_FILE = str(DATA_DIR / "ids.npy")
TEMP_DIR = str(DATA_DIR / "temp_live_chunks")


def main() -> None:
    # Temporary test runner for live ingest validation.
    streamer = "thesketchreal"
    chunk_seconds = 60

    store = VectorStore(
        db_path=DB_PATH,
        vector_file=VECTOR_FILE,
        id_file=ID_FILE,
    )
    store.init_db()

    embedder = Embedder()

    source = LiveTwitchSource(
        streamer=streamer,
        chunk_seconds=chunk_seconds,
        temp_dir=TEMP_DIR,
        db_path=DB_PATH,
    )

    session = IngestSession(
        source=source,
        embedder=embedder,
        store=store,
        poll_interval=0.5,
    )

    print(f"Starting live ingest for twitch.tv/{streamer}")
    print("Press Ctrl+C to stop.")

    try:
        session.run()
    except KeyboardInterrupt:
        session.stop()
        print("\nStopped by user.")


if __name__ == "__main__":
    main()
