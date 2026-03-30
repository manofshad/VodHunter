import asyncio
import importlib
import sys
from unittest.mock import patch
from fastapi.testclient import TestClient
from backend.apps import admin as admin_app_module
from backend.apps import public as public_app_module

class StubMonitorManager:

    def __init__(self):
        self.stop_calls = 0

    def stop(self) -> None:
        self.stop_calls += 1

class TestApiAppSplit:

    def test_backend_main_defaults_to_public_app(self) -> None:
        main_module = importlib.import_module('backend.main')
        assert main_module.app is public_app_module.app
        assert main_module.create_public_app is public_app_module.create_public_app

    def test_public_and_admin_route_boundaries(self) -> None:
        public_app = public_app_module.create_public_app(enable_lifespan=False)
        admin_app = admin_app_module.create_admin_app(enable_lifespan=False)
        public_paths = {route.path for route in public_app.routes}
        admin_paths = {route.path for route in admin_app.routes}
        assert '/api/health' in public_paths
        assert '/api/search/clip' in public_paths
        assert '/api/live/status' not in public_paths
        assert '/api/twitch/eventsub' not in public_paths
        assert '/api/health' in admin_paths
        assert '/api/search/clip' in admin_paths
        assert '/api/live/status' in admin_paths
        assert '/api/live/start' in admin_paths
        assert '/api/live/stop' in admin_paths
        assert '/api/live/sessions' in admin_paths
        assert '/api/twitch/eventsub' in admin_paths

    def test_public_returns_404_for_admin_only_routes(self) -> None:
        app = public_app_module.create_public_app(enable_lifespan=False)
        with TestClient(app) as client:
            assert client.get('/api/live/status').status_code == 404
            assert client.post('/api/live/start', json={'streamer': 'alice'}).status_code == 404
            assert client.post('/api/live/stop').status_code == 404
            assert client.get('/api/live/sessions').status_code == 404
            assert client.post('/api/twitch/eventsub', content='{}').status_code == 404

    def test_public_lifespan_initializes_search_only(self) -> None:
        app = public_app_module.create_public_app(enable_lifespan=True)
        with patch('backend.bootstrap_shared.prepare_runtime_dirs') as prepare_dirs, patch('backend.bootstrap_shared.build_store_state', return_value={'store': object()}), patch('backend.bootstrap_shared.build_search_stack', return_value={'search_service': object(), 'search_manager': object()}):

            async def run_lifespan() -> None:
                async with app.router.lifespan_context(app):
                    assert hasattr(app.state, 'store')
                    assert hasattr(app.state, 'search_manager')
                    assert not hasattr(app.state, 'embedder')
                    assert not hasattr(app.state, 'monitor_manager')
            asyncio.run(run_lifespan())
            prepare_dirs.assert_called_once()

    def test_public_import_does_not_require_admin_bootstrap(self) -> None:
        sys.modules.pop('backend.apps.public', None)

        def fail_build_monitor_stack(*args, **kwargs):
            raise AssertionError('public app imported admin bootstrap')
        with patch('backend.bootstrap_admin.build_monitor_stack', side_effect=fail_build_monitor_stack):
            module = importlib.import_module('backend.apps.public')
        app = module.create_public_app(enable_lifespan=False)
        assert app.title == 'VodHunter Public API'

    def test_admin_lifespan_stops_monitor_manager(self) -> None:
        app = admin_app_module.create_admin_app(enable_lifespan=True)
        monitor = StubMonitorManager()
        with patch('backend.bootstrap_shared.prepare_admin_runtime_dirs') as prepare_dirs, patch('backend.bootstrap_shared.build_store_state', return_value={'store': object()}), patch('backend.bootstrap_ingest.build_ingest_state', return_value={'embedder': object()}), patch('backend.bootstrap_shared.build_search_stack', return_value={'search_service': object(), 'search_manager': object()}), patch('backend.bootstrap_admin.build_monitor_stack', return_value={'monitor_manager': monitor, 'eventsub_handler': object(), 'session_query': object()}):

            async def run_lifespan() -> None:
                async with app.router.lifespan_context(app):
                    assert hasattr(app.state, 'embedder')
                    assert hasattr(app.state, 'monitor_manager')
                    assert hasattr(app.state, 'eventsub_handler')
                    assert hasattr(app.state, 'session_query')
            asyncio.run(run_lifespan())
            prepare_dirs.assert_called_once()
            assert monitor.stop_calls == 1
