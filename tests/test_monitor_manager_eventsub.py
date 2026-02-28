import tempfile
import time
import unittest
from pathlib import Path
import sys
from unittest.mock import patch

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from backend.services.monitor_manager import MonitorManager
from pipeline.embedder import Embedder
from storage.vector_store import VectorStore


class FakeTwitchMonitor:
    def __init__(self, is_live_value: bool = False):
        self.is_live_value = is_live_value
        self.is_live_calls = 0

    def get_user_id(self, streamer: str) -> str:
        return "user-1"

    def is_live(self, streamer: str) -> bool:
        self.is_live_calls += 1
        return self.is_live_value


class FakeEventSubClient:
    def __init__(self, fail_ensure: bool = False):
        self.fail_ensure = fail_ensure
        self.ensure_calls = 0
        self.cleanup_calls = 0

    def ensure_stream_subscriptions(self, user_id: str, callback_url: str, secret: str):
        self.ensure_calls += 1
        if self.fail_ensure:
            raise RuntimeError("ensure failed")
        return {"stream.online": "1", "stream.offline": "2"}

    def cleanup_for_broadcaster(self, user_id: str):
        self.cleanup_calls += 1


class TestMonitorManagerEventSub(unittest.TestCase):
    def _build_manager(
        self,
        tmp: str,
        monitor: FakeTwitchMonitor,
        eventsub_client: FakeEventSubClient,
        secret: str = "secret",
        callback_url: str = "https://cb.example/api/twitch/eventsub",
        fallback_poll_seconds: float = 120.0,
    ) -> MonitorManager:
        store = VectorStore(
            db_path=f"{tmp}/meta.db",
            vector_file=f"{tmp}/vectors.npy",
            id_file=f"{tmp}/ids.npy",
        )
        store.init_db()
        embedder = Embedder.__new__(Embedder)  # avoid model load in tests
        with patch("backend.services.monitor_manager.TwitchMonitor.from_env", return_value=monitor):
            manager = MonitorManager(
                store=store,
                embedder=embedder,
                chunk_seconds=60,
                monitor_poll_seconds=30.0,
                session_poll_interval=0.5,
                monitor_retry_seconds=0.1,
                temp_dir=f"{tmp}/chunks",
                archive_lag_seconds=120,
                archive_poll_seconds=15.0,
                archive_finalize_checks=3,
                eventsub_client=eventsub_client,  # type: ignore[arg-type]
                eventsub_callback_url=callback_url,
                eventsub_secret=secret,
                eventsub_reconcile_seconds=3600.0,
                eventsub_fallback_poll_seconds=fallback_poll_seconds,
            )
        return manager

    def test_healthy_eventsub_does_not_poll_live_loop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            monitor = FakeTwitchMonitor(is_live_value=False)
            eventsub = FakeEventSubClient(fail_ensure=False)
            manager = self._build_manager(tmp, monitor, eventsub)

            manager.start("alice")
            time.sleep(0.2)
            manager.stop()

            self.assertGreaterEqual(eventsub.ensure_calls, 1)
            self.assertEqual(monitor.is_live_calls, 0)
            self.assertEqual(eventsub.cleanup_calls, 1)

    def test_degraded_eventsub_uses_fallback_polling(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            monitor = FakeTwitchMonitor(is_live_value=False)
            eventsub = FakeEventSubClient(fail_ensure=True)
            manager = self._build_manager(
                tmp,
                monitor,
                eventsub,
                fallback_poll_seconds=0.05,
            )

            manager.start("alice")
            time.sleep(0.25)
            status = manager.get_status()
            manager.stop()

            self.assertEqual(status.eventsub_health, "degraded")
            self.assertGreaterEqual(monitor.is_live_calls, 1)


if __name__ == "__main__":
    unittest.main()
