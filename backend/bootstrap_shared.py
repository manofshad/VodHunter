from __future__ import annotations

import os

from backend import config
from storage.vector_store import VectorStore


def prepare_runtime_dirs() -> None:
    os.makedirs(config.DATA_DIR, exist_ok=True)
    os.makedirs(config.TEMP_BACKFILL_DIR, exist_ok=True)
    os.makedirs(config.TEMP_SEARCH_DOWNLOAD_DIR, exist_ok=True)


def prepare_admin_runtime_dirs() -> None:
    prepare_runtime_dirs()
    os.makedirs(config.TEMP_SEARCH_UPLOAD_DIR, exist_ok=True)


def build_store_state() -> dict[str, object]:
    config.validate_storage_config()
    config.validate_nmfp_config()

    store = VectorStore(
        database_url=config.DATABASE_URL,
        vector_dim=config.VECTOR_DIM,
        hnsw_ef_search=config.HNSW_EF_SEARCH,
        model_version=config.NMFP_MODEL_VERSION,
        preprocessing_version=config.NMFP_PREPROCESSING_VERSION,
    )
    store.ensure_schema_ready()

    return {"store": store}


def build_modal_query_embedder() -> ModalQueryEmbedder:
    from search.modal_embedding_client import ModalEmbeddingClient
    from search.modal_query_embedder import ModalQueryEmbedder

    config.validate_modal_search_config()
    config.validate_nmfp_config()
    client = ModalEmbeddingClient(
        app_name=config.MODAL_SEARCH_APP_NAME,
        function_name=config.MODAL_SEARCH_FUNCTION_NAME,
        timeout_seconds=config.MODAL_SEARCH_TIMEOUT_SECONDS,
    )
    return ModalQueryEmbedder(
        client=client,
        vector_dim=config.VECTOR_DIM,
        model_version=config.NMFP_MODEL_VERSION,
        preprocessing_version=config.NMFP_PREPROCESSING_VERSION,
    )


def build_search_stack(
    store: VectorStore,
    max_duration_seconds: int | None,
    upload_temp_dir: str | None = None,
) -> dict[str, object]:
    from backend.services.remote_clip_downloader import RemoteClipDownloader
    from backend.services.search_manager import SearchManager
    from search.alignment_service import AlignmentConfig, AlignmentService
    from search.query_preprocessor import QueryPreprocessor
    from search.search_service import SearchService

    search_service = SearchService(
        store=store,
        preprocessor=QueryPreprocessor(temp_dir=config.TEMP_SEARCH_PREPROCESS_DIR),
        query_embedder=build_modal_query_embedder(),
        alignment=AlignmentService(
            config=AlignmentConfig(
                top_k=config.SEARCH_TOP_K,
                fingerprint_hop_seconds=config.NMFP_HOP_SECONDS,
                offset_bin_seconds=config.CUT_OFFSET_BIN_SECONDS,
                offset_tolerance_seconds=config.CUT_OFFSET_TOLERANCE_SECONDS,
                max_unmatched_gap_seconds=config.CUT_MAX_UNMATCHED_GAP_SECONDS,
                min_support=config.CUT_MIN_SUPPORT,
                min_segment_duration_seconds=config.CUT_MIN_SEGMENT_DURATION_SECONDS,
                min_density=config.CUT_MIN_DENSITY,
                merge_query_gap_seconds=config.CUT_MERGE_QUERY_GAP_SECONDS,
                merge_offset_tolerance_seconds=config.CUT_MERGE_OFFSET_TOLERANCE_SECONDS,
                max_segments=config.CUT_MAX_SEGMENTS,
            ),
        ),
        top_k=config.SEARCH_TOP_K,
    )

    search_manager = SearchManager(
        search_service=search_service,
        upload_temp_dir=upload_temp_dir,
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
