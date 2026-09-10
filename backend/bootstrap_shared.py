from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from pipeline.nmfp_inference import (
    NMFP_EMBEDDING_DIM,
    NMFP_MODEL_VERSION,
    NMFP_PREPROCESSING_VERSION,
    model_artifact_identity,
)
from storage.repositories import Repositories, build_repositories as _build_repositories


logger = logging.getLogger("uvicorn.error")


@dataclass(frozen=True, slots=True)
class SearchStack:
    query_embedder: object
    search_service: object
    search_manager: object


def build_repositories(database_url: str | None = None) -> Repositories:
    return _build_repositories(
        database_url=(
            database_url
            if database_url is not None
            else os.getenv("DATABASE_URL", "").strip()
        )
    )


def build_local_query_embedder():
    from pipeline.embedder import Embedder
    from search.local_query_embedder import LocalQueryEmbedder

    local_embedder = Embedder()
    if local_embedder.embedding_dim != NMFP_EMBEDDING_DIM:
        raise ValueError(
            f"Local NMFP embedding dimension {local_embedder.embedding_dim} "
            f"does not match NMFP embedding dimension {NMFP_EMBEDDING_DIM}"
        )
    if local_embedder.model_version != NMFP_MODEL_VERSION:
        raise ValueError("Local NMFP model version does not match the production index")
    if local_embedder.preprocessing_version != NMFP_PREPROCESSING_VERSION:
        raise ValueError("Local NMFP preprocessing version does not match the production index")

    startup_ms = local_embedder.load()
    logger.info(
        "timing event=nmfp_local_startup model_startup_ms=%d model_version=%s "
        "preprocessing_version=%s embedding_dim=%d artifact_identity=%s",
        startup_ms,
        local_embedder.model_version,
        local_embedder.preprocessing_version,
        local_embedder.embedding_dim,
        model_artifact_identity(),
    )
    return LocalQueryEmbedder(local_embedder)


def build_search_stack(
    repositories: Repositories,
    max_duration_seconds: int | None,
    *,
    download_temp_dir: str,
    preprocess_temp_dir: str,
) -> dict[str, object]:
    from backend.services.remote_clip_downloader import RemoteClipDownloader
    from backend.services.search_manager import SearchManager
    from search.alignment_service import AlignmentConfig, AlignmentService
    from search.query_preprocessor import QueryPreprocessor
    from search.search_service import SearchService

    query_embedder = build_local_query_embedder()
    alignment_config = AlignmentConfig()
    search_service = SearchService(
        videos=repositories.videos,
        fingerprints=repositories.fingerprints,
        preprocessor=QueryPreprocessor(temp_dir=preprocess_temp_dir),
        query_embedder=query_embedder,
        alignment=AlignmentService(config=alignment_config),
        top_k=alignment_config.top_k,
    )

    search_manager = SearchManager(
        search_service=search_service,
        remote_downloader=RemoteClipDownloader(
            temp_dir=download_temp_dir,
        ),
        max_duration_seconds=max_duration_seconds,
    )

    return SearchStack(
        query_embedder=query_embedder,
        search_service=search_service,
        search_manager=search_manager,
    )
