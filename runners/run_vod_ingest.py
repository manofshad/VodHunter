from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipeline.embedder import Embedder
from storage.vector_store import VectorStore
from sources.vod_source import VODSource
from pipeline.ingest_session import IngestSession

import sqlite3

DATA_DIR = ROOT_DIR / "data"
DB_PATH = str(DATA_DIR / "metadata.db")
VECTOR_FILE = str(DATA_DIR / "vectors.npy")
ID_FILE = str(DATA_DIR / "ids.npy")



def main():
    # ---- CONFIG ----
    audio_path = str(DATA_DIR / "example.wav")  # <-- 16kHz mono WAV
    creator_name = "test_creator"
    url = "local_test_vod"
    title = "Test VOD"
    # ----------------

    store = VectorStore(
        db_path=DB_PATH,
        vector_file=VECTOR_FILE,
        id_file=ID_FILE,
    )
    store.init_db()


    embedder = Embedder()

    source = VODSource(
        audio_path=audio_path,
        creator_name="test_creator",
        video_url="local_test_vod",
        title="Test VOD",
    )

    session = IngestSession(
        source=source,
        embedder=embedder,
        store=store,
    )

    session.run()

    print("✅ VOD ingest complete")


if __name__ == "__main__":
    main()
