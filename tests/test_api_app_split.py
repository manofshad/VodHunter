import asyncio
import importlib
import unittest
from pathlib import Path
import sys
from unittest.mock import patch

from fastapi.testclient import TestClient

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from backend.apps import admin as admin_app_module
from backend.apps import public as public_app_module


class StubMonitorManager:
    def __init__(self):
        self.stop_calls = 0

    def stop(self) -> None:
        self.stop_calls += 1


class TestApiAppSplit(unittest.TestCase):
    def test_backend_main_defaults_to_public_app(self) -> None:
        main_module = importlib.import_module("backend.main")

        self.assertIs(main_module.app, public_app_module.app)
        self.assertIs(main_module.create_public_app, public_app_module.create_public_app)

    def test_public_and_admin_route_boundaries(self) -> None:
        public_app = public_app_module.create_public_app(enable_lifespan=False)
        admin_app = admin_app_module.create_admin_app(enable_lifespan=False)

        public_paths = {route.path for route in public_app.routes}
        admin_paths = {route.path for route in admin_app.routes}

        self.assertIn("/api/health", public_paths)
        self.assertIn("/api/search/clip", public_paths)
        self.assertNotIn("/api/live/status", public_paths)
        self.assertNotIn("/api/twitch/eventsub", public_paths)

        self.assertIn("/api/health", admin_paths)
        self.assertIn("/api/search/clip", admin_paths)
        self.assertIn("/api/live/status", admin_paths)
        self.assertIn("/api/live/start", admin_paths)
        self.assertIn("/api/live/stop", admin_paths)
        self.assertIn("/api/live/sessions", admin_paths)
        self.assertIn("/api/twitch/eventsub", admin_paths)

    def test_public_returns_404_for_admin_only_routes(self) -> None:
        app = public_app_module.create_public_app(enable_lifespan=False)
        with TestClient(app) as client:
            self.assertEqual(client.get("/api/live/status").status_code, 404)
            self.assertEqual(client.post("/api/live/start", json={"streamer": "alice"}).status_code, 404)
            self.assertEqual(client.post("/api/live/stop").status_code, 404)
            self.assertEqual(client.get("/api/live/sessions").status_code, 404)
            self.assertEqual(client.post("/api/twitch/eventsub", data="{}").status_code, 404)

    def test_public_lifespan_initializes_search_only(self) -> None:
        app = public_app_module.create_public_app(enable_lifespan=True)

        with patch(
            "backend.bootstrap_shared.prepare_runtime_dirs"
        ) as prepare_dirs, patch(
            "backend.bootstrap_shared.build_common_state",
            return_value={"store": object(), "embedder": object()},
        ), patch(
            "backend.bootstrap_shared.build_search_stack",
            return_value={"search_service": object(), "search_manager": object()},
        ):

            async def run_lifespan() -> None:
                async with app.router.lifespan_context(app):
                    self.assertTrue(hasattr(app.state, "store"))
                    self.assertTrue(hasattr(app.state, "embedder"))
                    self.assertTrue(hasattr(app.state, "search_manager"))
                    self.assertFalse(hasattr(app.state, "monitor_manager"))

            asyncio.run(run_lifespan())
            prepare_dirs.assert_called_once()

    def test_public_import_does_not_require_admin_bootstrap(self) -> None:
        sys.modules.pop("backend.apps.public", None)

        def fail_build_monitor_stack(*args, **kwargs):
            raise AssertionError("public app imported admin bootstrap")

        with patch("backend.bootstrap_admin.build_monitor_stack", side_effect=fail_build_monitor_stack):
            module = importlib.import_module("backend.apps.public")

        app = module.create_public_app(enable_lifespan=False)
        self.assertEqual(app.title, "VodHunter Public API")

    def test_admin_lifespan_stops_monitor_manager(self) -> None:
        app = admin_app_module.create_admin_app(enable_lifespan=True)
        monitor = StubMonitorManager()

        with patch(
            "backend.bootstrap_shared.prepare_runtime_dirs"
        ) as prepare_dirs, patch(
            "backend.bootstrap_shared.build_common_state",
            return_value={"store": object(), "embedder": object()},
        ), patch(
            "backend.bootstrap_shared.build_search_stack",
            return_value={"search_service": object(), "search_manager": object()},
        ), patch(
            "backend.bootstrap_admin.build_monitor_stack",
            return_value={"monitor_manager": monitor, "eventsub_handler": object(), "session_query": object()},
        ):

            async def run_lifespan() -> None:
                async with app.router.lifespan_context(app):
                    self.assertTrue(hasattr(app.state, "monitor_manager"))
                    self.assertTrue(hasattr(app.state, "eventsub_handler"))
                    self.assertTrue(hasattr(app.state, "session_query"))

            asyncio.run(run_lifespan())
            prepare_dirs.assert_called_once()
            self.assertEqual(monitor.stop_calls, 1)


if __name__ == "__main__":
    unittest.main()
