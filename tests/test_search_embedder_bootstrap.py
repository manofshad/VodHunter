import pytest
from unittest.mock import Mock, patch
from backend import bootstrap_ingest, bootstrap_shared
from pipeline.nmfp_inference import (
    NMFP_EMBEDDING_DIM,
    NMFP_MODEL_VERSION,
    NMFP_PREPROCESSING_VERSION,
)
from search.local_query_embedder import LocalQueryEmbedder

class TestSearchEmbedderBootstrap:

    def test_build_repositories_delegates_to_storage_composition(self) -> None:
        repositories = object()
        with patch('backend.bootstrap_shared._build_repositories', return_value=repositories) as build_repositories:
            state = bootstrap_shared.build_repositories('postgresql://db')
        assert state is repositories
        build_repositories.assert_called_once_with(database_url='postgresql://db')

    def test_builds_and_preloads_local_query_embedder(self) -> None:
        embedder = Mock(
            embedding_dim=NMFP_EMBEDDING_DIM,
            model_version=NMFP_MODEL_VERSION,
            preprocessing_version=NMFP_PREPROCESSING_VERSION,
            is_loaded=True,
        )
        embedder.load.return_value = 41
        with patch('pipeline.embedder.Embedder', return_value=embedder):
            query_embedder = bootstrap_shared.build_local_query_embedder()
        try:
            assert isinstance(query_embedder, LocalQueryEmbedder)
            assert query_embedder.embedder is embedder
            embedder.load.assert_called_once_with()
        finally:
            query_embedder.close()

    def test_local_query_embedder_rejects_index_dimension_mismatch(self) -> None:
        embedder = Mock(
            embedding_dim=64,
            model_version=NMFP_MODEL_VERSION,
            preprocessing_version=NMFP_PREPROCESSING_VERSION,
        )
        with patch('pipeline.embedder.Embedder', return_value=embedder):
            with pytest.raises(ValueError, match='embedding dimension'):
                bootstrap_shared.build_local_query_embedder()

    def test_build_ingest_state_constructs_local_embedder(self) -> None:
        embedder = object()
        with patch('backend.bootstrap_ingest.Embedder', return_value=embedder):
            state = bootstrap_ingest.build_ingest_state()
        assert state == {'embedder': embedder}
