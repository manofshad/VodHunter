import os

from backend import config
from backend.services.remote_clip_downloader import RemoteClipDownloader
from backend.services.search_manager import SearchManager
from pipeline.embedder import Embedder
from search.alignment_service import AlignmentConfig, AlignmentService
from search.local_query_embedder import LocalQueryEmbedder
from search.modal_embedding_client import ModalEmbeddingClient
from search.modal_query_embedder import ModalQueryEmbedder
from search.query_preprocessor import QueryPreprocessor
from search.search_service import SearchService
from search.vector_matcher import VectorMatcher
from storage.vector_store import VectorStore


def prepare_runtime_dirs() -> None:
    os.makedirs(config.DATA_DIR, exist_ok=True)
    os.makedirs(config.TEMP_SEARCH_UPLOAD_DIR, exist_ok=True)
    os.makedirs(config.TEMP_SEARCH_DOWNLOAD_DIR, exist_ok=True)


def build_common_state() -> dict[str, object]:
    config.validate_storage_config()

    store = VectorStore(
        database_url=config.DATABASE_URL,
        vector_dim=config.VECTOR_DIM,
        pgvector_probes=config.PGVECTOR_PROBES,
    )
    store.init_db()

    embedder = Embedder()
    return {
        "store": store,
        "embedder": embedder,
    }


def _build_query_embedder(embedder: Embedder):
    config.validate_search_embedder_config()

    if config.SEARCH_QUERY_EMBEDDER_BACKEND == "local":
        return LocalQueryEmbedder(embedder=embedder)

    client = ModalEmbeddingClient(
        app_name=config.MODAL_SEARCH_APP_NAME,
        function_name=config.MODAL_SEARCH_FUNCTION_NAME,
        timeout_seconds=config.MODAL_SEARCH_TIMEOUT_SECONDS,
    )
    return ModalQueryEmbedder(
        client=client,
        vector_dim=config.VECTOR_DIM,
        model_version=config.MODAL_SEARCH_MODEL_NAME,
        fallback_embedder=embedder,
        fallback_to_local=config.MODAL_SEARCH_FALLBACK_TO_LOCAL,
    )


def build_search_stack(
    store: VectorStore,
    embedder: Embedder,
    max_duration_seconds: int | None,
) -> dict[str, object]:
    search_service = SearchService(
        store=store,
        preprocessor=QueryPreprocessor(temp_dir=config.TEMP_SEARCH_PREPROCESS_DIR),
        query_embedder=_build_query_embedder(embedder=embedder),
        matcher=VectorMatcher(top_k=10),
        alignment=AlignmentService(
            store=store,
            config=AlignmentConfig(min_vote_count=3, min_vote_ratio=0.08),
        ),
    )

    search_manager = SearchManager(
        search_service=search_service,
        upload_temp_dir=config.TEMP_SEARCH_UPLOAD_DIR,
        remote_downloader=RemoteClipDownloader(
            temp_dir=config.TEMP_SEARCH_DOWNLOAD_DIR,
            timeout_seconds=config.TIKTOK_DOWNLOAD_TIMEOUT_SECONDS,
            max_file_mb=config.TIKTOK_MAX_FILE_MB,
        ),
        max_duration_seconds=max_duration_seconds,
    )

    return {
        "search_service": search_service,
        "search_manager": search_manager,
    }
