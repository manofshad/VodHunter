from __future__ import annotations

import logging
from pathlib import Path
from uuid import uuid4

import numpy as np

from pipeline.embedder import Embedder
from search.local_query_embedder import LocalQueryEmbedder
from search.modal_embedding_client import ModalEmbeddingClient, ModalEmbeddingError
from search.modal_types import ModalEmbeddingRequest

logger = logging.getLogger("uvicorn.error")


class ModalQueryEmbedder:
    def __init__(
        self,
        client: ModalEmbeddingClient,
        vector_dim: int,
        model_version: str = "",
        fallback_embedder: Embedder | None = None,
        fallback_to_local: bool = True,
    ):
        self.client = client
        self.vector_dim = int(vector_dim)
        self.model_version = model_version
        self.fallback_to_local = fallback_to_local
        self.local_fallback = LocalQueryEmbedder(fallback_embedder) if fallback_embedder is not None else None

    def embed(self, wav_path: str) -> tuple[np.ndarray, np.ndarray]:
        request_id = uuid4().hex
        filename = Path(wav_path).name
        try:
            with open(wav_path, "rb") as infile:
                wav_bytes = infile.read()

            response = self.client.embed(
                ModalEmbeddingRequest(
                    wav_bytes=wav_bytes,
                    request_id=request_id,
                    filename=filename,
                    offset_seconds=0.0,
                    model_version=self.model_version,
                )
            )
            embeddings = np.array(response.embeddings, dtype=np.float32)
            timestamps = np.array(response.timestamps, dtype=np.float32)
            self._validate_response(embeddings, timestamps, response.embedding_dim)
            return embeddings, timestamps
        except Exception as exc:
            if self.fallback_to_local and self.local_fallback is not None:
                logger.warning(
                    "modal query embedding failed; using local fallback request_id=%s filename=%s error=%s",
                    request_id,
                    filename,
                    exc,
                )
                return self.local_fallback.embed(wav_path)
            if isinstance(exc, ModalEmbeddingError):
                raise RuntimeError(str(exc)) from exc
            raise

    def _validate_response(
        self,
        embeddings: np.ndarray,
        timestamps: np.ndarray,
        embedding_dim: int,
    ) -> None:
        if embedding_dim != self.vector_dim:
            raise ModalEmbeddingError(
                f"Modal embedding_dim {embedding_dim} does not match VECTOR_DIM {self.vector_dim}"
            )
        if embeddings.ndim == 1 and embeddings.size == 0 and timestamps.ndim == 1 and timestamps.size == 0:
            return
        if embeddings.ndim != 2:
            raise ModalEmbeddingError("Modal embeddings must be a 2D array")
        if embeddings.shape[1] != embedding_dim:
            raise ModalEmbeddingError("Modal embeddings shape does not match embedding_dim")
        if embeddings.shape[0] != timestamps.shape[0]:
            raise ModalEmbeddingError("Modal embeddings/timestamps length mismatch")
        if timestamps.size > 1 and np.any(np.diff(timestamps) < 0):
            raise ModalEmbeddingError("Modal timestamps must be monotonic")
