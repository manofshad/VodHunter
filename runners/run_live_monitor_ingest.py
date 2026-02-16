from pathlib import Path
import sys
import time

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipeline.embedder import Embedder
from storage.vector_store import VectorStore
from sources.live_archive_vod_source import LiveArchiveVODSource
from services.twitch_monitor import TwitchMonitor
from pipeline.ingest_session import IngestSession


DATA_DIR = ROOT_DIR / "data"
DB_PATH = str(DATA_DIR / "metadata.db")
VECTOR_FILE = str(DATA_DIR / "vectors.npy")
ID_FILE = str(DATA_DIR / "ids.npy")
TEMP_DIR = str(DATA_DIR / "temp_live_chunks")
STREAMER = "thesketchreal"
INGEST_CHUNK_SECONDS = 60
MONITOR_POLL_SECONDS = 30.0
SESSION_POLL_INTERVAL = 0.5
MONITOR_RETRY_SECONDS = 5.0
LIVE_ARCHIVE_LAG_SECONDS = 120
LIVE_ARCHIVE_POLL_SECONDS = 15.0
LIVE_ARCHIVE_FINALIZE_CHECKS = 3


def main() -> None:
    store = VectorStore(
        db_path=DB_PATH,
        vector_file=VECTOR_FILE,
        id_file=ID_FILE,
    )
    store.init_db()

    embedder = Embedder()
    monitor = TwitchMonitor.from_env()

    print(f"Starting monitor for twitch.tv/{STREAMER}")
    print(
        f"Offline poll={MONITOR_POLL_SECONDS}s, chunk={INGEST_CHUNK_SECONDS}s, "
        f"session poll={SESSION_POLL_INTERVAL}s"
    )
    print("Press Ctrl+C to stop.")

    active_session: IngestSession | None = None

    try:
        while True:
            try:
                live = monitor.is_live(STREAMER)
            except Exception as exc:
                print(f"[monitor] status check failed: {exc}")
                time.sleep(MONITOR_RETRY_SECONDS)
                continue

            if not live:
                print(f"[monitor] {STREAMER} is offline. Checking again in {MONITOR_POLL_SECONDS}s.")
                time.sleep(MONITOR_POLL_SECONDS)
                continue

            print(f"[monitor] {STREAMER} is live. Starting archive-backed ingest session.")
            source = LiveArchiveVODSource(
                streamer=STREAMER,
                store=store,
                twitch_monitor=monitor,
                chunk_seconds=INGEST_CHUNK_SECONDS,
                lag_seconds=LIVE_ARCHIVE_LAG_SECONDS,
                poll_seconds=LIVE_ARCHIVE_POLL_SECONDS,
                finalize_checks=LIVE_ARCHIVE_FINALIZE_CHECKS,
                temp_dir=TEMP_DIR,
            )
            active_session = IngestSession(
                source=source,
                embedder=embedder,
                store=store,
                poll_interval=SESSION_POLL_INTERVAL,
            )

            try:
                active_session.run()
            except Exception as exc:
                print(f"[ingest] session ended with error: {exc}")
            finally:
                active_session = None

            print("[monitor] ingest stopped. Returning to live checks.")
            time.sleep(MONITOR_RETRY_SECONDS)

    except KeyboardInterrupt:
        if active_session is not None:
            active_session.stop()
        print("\nStopped by user.")


if __name__ == "__main__":
    main()
