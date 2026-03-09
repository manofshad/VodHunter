import unittest
from pathlib import Path
import sys
from unittest.mock import patch

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from backend import bootstrap_shared, config
from search.local_query_embedder import LocalQueryEmbedder
from search.modal_query_embedder import ModalQueryEmbedder


class TestSearchEmbedderBootstrap(unittest.TestCase):
    def test_builds_local_query_embedder_by_default(self) -> None:
        with patch.object(config, "SEARCH_QUERY_EMBEDDER_BACKEND", "local"):
            query_embedder = bootstrap_shared._build_query_embedder(object())

        self.assertIsInstance(query_embedder, LocalQueryEmbedder)

    def test_builds_modal_query_embedder(self) -> None:
        with patch.object(config, "SEARCH_QUERY_EMBEDDER_BACKEND", "modal"), patch.object(
            config, "MODAL_SEARCH_APP_NAME", "vodhunter-search-embedder"
        ), patch.object(
            config, "MODAL_SEARCH_FUNCTION_NAME", "embed_search_wav"
        ), patch.object(
            config, "MODAL_SEARCH_TIMEOUT_SECONDS", 5.0
        ), patch.object(
            config, "MODAL_SEARCH_FALLBACK_TO_LOCAL", True
        ), patch.object(
            config, "MODAL_SEARCH_MODEL_NAME", "ast"
        ), patch(
            "backend.config.os.getenv",
            side_effect=lambda name, default="": {"MODAL_TOKEN_ID": "id", "MODAL_TOKEN_SECRET": "secret"}.get(name, default),
        ), patch(
            "backend.bootstrap_shared.ModalEmbeddingClient",
            return_value=object(),
        ):
            query_embedder = bootstrap_shared._build_query_embedder(object())

        self.assertIsInstance(query_embedder, ModalQueryEmbedder)

    def test_modal_config_requires_function_name(self) -> None:
        with patch.object(config, "SEARCH_QUERY_EMBEDDER_BACKEND", "modal"), patch.object(
            config, "MODAL_SEARCH_APP_NAME", "vodhunter-search-embedder"
        ), patch.object(
            config, "MODAL_SEARCH_FUNCTION_NAME", ""
        ), patch(
            "backend.config.os.getenv",
            side_effect=lambda name, default="": {"MODAL_TOKEN_ID": "id", "MODAL_TOKEN_SECRET": "secret"}.get(name, default),
        ):
            with self.assertRaises(ValueError):
                config.validate_search_embedder_config()
