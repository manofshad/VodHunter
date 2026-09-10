import asyncio
import importlib
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import patch
from fastapi.testclient import TestClient
from backend.apps import public as public_app_module


def _route_paths(routes) -> set[str]:
    paths: set[str] = set()
    for route in routes:
        path = getattr(route, 'path', None)
        if path is not None:
            paths.add(path)
        effective_candidates = getattr(route, 'effective_candidates', None)
        if callable(effective_candidates):
            nested_routes = effective_candidates()
        else:
            nested_routes = getattr(route, 'routes', None)
        if nested_routes is not None:
            paths.update(_route_paths(nested_routes))
    return paths


class StubQueryEmbedder:

    def __init__(self):
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1

class TestApiAppSplit:

    def test_route_paths_support_lazy_included_router(self) -> None:
        class IncludedRouter:

            def effective_candidates(self):
                return [type('Route', (), {'path': '/api/health'})()]

        assert _route_paths([IncludedRouter()]) == {'/api/health'}

    def test_ingest_bootstrap_does_not_import_fastapi(self) -> None:
        root = Path(__file__).resolve().parents[1]
        code = """
import builtins
real_import = builtins.__import__
def blocked_import(name, *args, **kwargs):
    if name == "fastapi" or name.startswith("fastapi."):
        raise ModuleNotFoundError("fastapi intentionally unavailable")
    return real_import(name, *args, **kwargs)
builtins.__import__ = blocked_import
import backend.bootstrap_ingest
import backend.bootstrap_shared
"""
        result = subprocess.run(
            [sys.executable, "-c", code],
            cwd=root,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr

    def test_backend_main_defaults_to_public_app(self) -> None:
        main_module = importlib.import_module('backend.main')
        assert main_module.app is public_app_module.app
        assert main_module.create_public_app is public_app_module.create_public_app

    def test_public_route_boundaries(self) -> None:
        public_app = public_app_module.create_public_app(enable_lifespan=False)
        public_paths = _route_paths(public_app.routes)
        assert '/api/health' in public_paths
        assert '/api/search/clip' in public_paths
        assert '/internal/videos/{video_id}/delete-index' in public_paths
        assert '/internal/videos/{video_id}/request-reindex' in public_paths
        assert '/api/live/status' not in public_paths
        assert '/api/twitch/eventsub' not in public_paths

    def test_public_returns_404_for_retired_admin_routes(self) -> None:
        app = public_app_module.create_public_app(enable_lifespan=False)
        with TestClient(app) as client:
            assert client.get('/api/live/status').status_code == 404
            assert client.post('/api/live/start', json={'streamer': 'alice'}).status_code == 404
            assert client.post('/api/live/stop').status_code == 404
            assert client.get('/api/live/sessions').status_code == 404
            assert client.post('/api/twitch/eventsub', content='{}').status_code == 404

    def test_health_reports_loaded_local_nmfp_identity(self) -> None:
        app = public_app_module.create_public_app(enable_lifespan=False)
        app.state.query_embedder = SimpleNamespace(
            is_loaded=True,
            embedding_dim=128,
            model_version='nmfp-model',
            preprocessing_version='nmfp-preprocessing',
        )
        with TestClient(app) as client:
            response = client.get('/api/health')

        assert response.status_code == 200
        assert response.json() == {
            'ok': True,
            'nmfp_loaded': True,
            'embedding_dim': 128,
            'model_version': 'nmfp-model',
            'preprocessing_version': 'nmfp-preprocessing',
            'artifact_identity': response.json()['artifact_identity'],
        }

    def test_public_lifespan_initializes_search_only(self) -> None:
        app = public_app_module.create_public_app(enable_lifespan=True)
        query_embedder = StubQueryEmbedder()
        with patch('backend.bootstrap_shared.prepare_runtime_dirs') as prepare_dirs, patch('backend.bootstrap_shared.build_store_state', return_value={'store': object()}), patch('backend.bootstrap_shared.build_search_stack', return_value={'query_embedder': query_embedder, 'search_service': object(), 'search_manager': object()}):

            async def run_lifespan() -> None:
                async with app.router.lifespan_context(app):
                    assert hasattr(app.state, 'store')
                    assert hasattr(app.state, 'search_manager')
                    assert not hasattr(app.state, 'embedder')
            asyncio.run(run_lifespan())
            prepare_dirs.assert_called_once()
            assert query_embedder.close_calls == 1
