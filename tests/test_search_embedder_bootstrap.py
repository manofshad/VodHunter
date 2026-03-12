import pytest
from unittest.mock import Mock, patch
from backend import bootstrap_shared, config
from search.local_query_embedder import LocalQueryEmbedder
from search.modal_query_embedder import ModalQueryEmbedder

class TestSearchEmbedderBootstrap:

    def test_build_common_state_checks_schema_readiness_without_running_init_db(self) -> None:
        store = Mock(spec=['ensure_schema_ready'])
        embedder = object()
        with patch('backend.bootstrap_shared.VectorStore', return_value=store) as vector_store_cls, patch('backend.bootstrap_shared.Embedder', return_value=embedder), patch.object(config, 'DATABASE_URL', 'postgresql://db'), patch.object(config, 'VECTOR_DIM', 768), patch.object(config, 'HNSW_EF_SEARCH', 40):
            state = bootstrap_shared.build_common_state()
        assert state == {'store': store, 'embedder': embedder}
        vector_store_cls.return_value.ensure_schema_ready.assert_called_once_with()

    def test_builds_local_query_embedder_by_default(self) -> None:
        with patch.object(config, 'SEARCH_QUERY_EMBEDDER_BACKEND', 'local'):
            query_embedder = bootstrap_shared._build_query_embedder(object())
        assert isinstance(query_embedder, LocalQueryEmbedder)

    def test_builds_modal_query_embedder(self) -> None:
        with patch.object(config, 'SEARCH_QUERY_EMBEDDER_BACKEND', 'modal'), patch.object(config, 'MODAL_SEARCH_APP_NAME', 'vodhunter-search-embedder'), patch.object(config, 'MODAL_SEARCH_FUNCTION_NAME', 'embed_search_wav'), patch.object(config, 'MODAL_SEARCH_TIMEOUT_SECONDS', 5.0), patch.object(config, 'MODAL_SEARCH_MODEL_NAME', 'ast'), patch('backend.config.os.getenv', side_effect=lambda name, default='': {'MODAL_TOKEN_ID': 'id', 'MODAL_TOKEN_SECRET': 'secret'}.get(name, default)), patch('backend.bootstrap_shared.ModalEmbeddingClient', return_value=object()):
            query_embedder = bootstrap_shared._build_query_embedder(object())
        assert isinstance(query_embedder, ModalQueryEmbedder)

    def test_modal_config_requires_function_name(self) -> None:
        with patch.object(config, 'SEARCH_QUERY_EMBEDDER_BACKEND', 'modal'), patch.object(config, 'MODAL_SEARCH_APP_NAME', 'vodhunter-search-embedder'), patch.object(config, 'MODAL_SEARCH_FUNCTION_NAME', ''), patch('backend.config.os.getenv', side_effect=lambda name, default='': {'MODAL_TOKEN_ID': 'id', 'MODAL_TOKEN_SECRET': 'secret'}.get(name, default)):
            with pytest.raises(ValueError):
                config.validate_search_embedder_config()
