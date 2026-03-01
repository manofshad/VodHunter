from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipeline.embedder import Embedder
from backend import config
from storage.vector_store import VectorStore
from sources.vod_source import VODSource
from pipeline.ingest_session import IngestSession

DATA_DIR = ROOT_DIR / "data"



def main():
    # ---- CONFIG ----
    audio_path = str(DATA_DIR / "example.wav")  # <-- 16kHz mono WAV
    creator_name = "test_creator"
    url = "local_test_vod"
    title = "Test VOD"
    # ----------------

    config.validate_storage_config()
    store = VectorStore(
        database_url=config.DATABASE_URL,
        vector_dim=config.VECTOR_DIM,
        pgvector_probes=config.PGVECTOR_PROBES,
    )
    store.init_db()


    embedder = Embedder()

    source = VODSource(
        audio_path=audio_path,
        creator_name="test_creator",
        video_url="local_test_vod",
        title="Test VOD",
        store=store,
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
