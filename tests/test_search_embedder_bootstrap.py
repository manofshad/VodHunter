import pytest
from unittest.mock import Mock, patch
from backend import bootstrap_ingest, bootstrap_shared, config
from search.local_query_embedder import LocalQueryEmbedder

class TestSearchEmbedderBootstrap:

    def test_build_store_state_checks_schema_readiness_without_running_init_db(self) -> None:
        store = Mock(spec=['ensure_schema_ready'])
        with patch('backend.bootstrap_shared.VectorStore', return_value=store) as vector_store_cls, patch.object(config, 'DATABASE_URL', 'postgresql://db'), patch.object(config, 'VECTOR_DIM', 128), patch.object(config, 'HNSW_EF_SEARCH', 40):
            state = bootstrap_shared.build_store_state()
        assert state == {'store': store}
        vector_store_cls.return_value.ensure_schema_ready.assert_called_once_with()

    def test_builds_and_preloads_local_query_embedder(self) -> None:
        embedder = Mock(
            embedding_dim=128,
            model_version=config.NMFP_MODEL_VERSION,
            preprocessing_version=config.NMFP_PREPROCESSING_VERSION,
            is_loaded=True,
        )
        embedder.load.return_value = 41
        with patch.object(config, 'VECTOR_DIM', 128):
            query_embedder = bootstrap_shared.build_local_query_embedder(embedder=embedder)
        try:
            assert isinstance(query_embedder, LocalQueryEmbedder)
            assert query_embedder.embedder is embedder
            embedder.load.assert_called_once_with()
        finally:
            query_embedder.close()

    def test_local_query_embedder_rejects_index_dimension_mismatch(self) -> None:
        embedder = Mock(
            embedding_dim=64,
            model_version=config.NMFP_MODEL_VERSION,
            preprocessing_version=config.NMFP_PREPROCESSING_VERSION,
        )
        with patch.object(config, 'VECTOR_DIM', 128):
            with pytest.raises(ValueError, match='embedding dimension'):
                bootstrap_shared.build_local_query_embedder(embedder=embedder)

    def test_nmfp_config_rejects_model_or_preprocessing_index_mismatch(self) -> None:
        with patch.object(config, 'NMFP_MODEL_VERSION', 'different-model'):
            with pytest.raises(ValueError, match='production index'):
                config.validate_nmfp_config()
        with patch.object(config, 'NMFP_PREPROCESSING_VERSION', 'different-preprocessing'):
            with pytest.raises(ValueError, match='production index'):
                config.validate_nmfp_config()

    def test_build_ingest_state_constructs_local_embedder(self) -> None:
        embedder = object()
        with patch('backend.bootstrap_ingest.Embedder', return_value=embedder):
            state = bootstrap_ingest.build_ingest_state()
        assert state == {'embedder': embedder}
