from __future__ import annotations

import logging
from pathlib import Path
from uuid import uuid4

import numpy as np

from search.modal_embedding_client import ModalEmbeddingClient, ModalEmbeddingError
from search.modal_types import ModalEmbeddingRequest

logger = logging.getLogger("uvicorn.error")


class ModalQueryEmbedder:
    def __init__(
        self,
        client: ModalEmbeddingClient,
        vector_dim: int,
        model_version: str = "",
    ):
        self.client = client
        self.vector_dim = int(vector_dim)
        self.model_version = model_version

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
