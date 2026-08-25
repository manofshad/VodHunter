from __future__ import annotations

import logging
from pathlib import Path
from uuid import uuid4

import numpy as np

from pipeline.nmfp_inference import NMFP_MODEL_VERSION, NMFP_PREPROCESSING_VERSION
from search.modal_embedding_client import ModalEmbeddingClient, ModalEmbeddingError
from search.modal_types import ModalEmbeddingRequest, ModalEmbeddingResponse

logger = logging.getLogger("uvicorn.error")


class ModalQueryEmbedder:
    def __init__(
        self,
        client: ModalEmbeddingClient,
        vector_dim: int,
        model_version: str = NMFP_MODEL_VERSION,
        preprocessing_version: str = NMFP_PREPROCESSING_VERSION,
    ):
        self.client = client
        self.vector_dim = int(vector_dim)
        self.model_version = model_version
        self.preprocessing_version = preprocessing_version
        self.last_response: ModalEmbeddingResponse | None = None

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
                    preprocessing_version=self.preprocessing_version,
                )
            )
            embeddings = np.array(response.embeddings, dtype=np.float32)
            timestamps = np.array(response.timestamps, dtype=np.float32)
            self._validate_response(
                embeddings,
                timestamps,
                response.embedding_dim,
                response.model_version,
                response.preprocessing_version,
            )
            self.last_response = response
            logger.info(
                "timing event=nmfp_remote_extract request_id=%s filename=%s cold_start=%s "
                "model_load_ms=%d preprocessing_ms=%d inference_ms=%d worker_total_ms=%d "
                "fingerprint_count=%d model_version=%s preprocessing_version=%s",
                request_id,
                filename,
                response.cold_start,
                response.model_load_duration_ms,
                response.preprocessing_duration_ms,
                response.inference_duration_ms,
                response.total_duration_ms,
                len(timestamps),
                response.model_version,
                response.preprocessing_version,
            )
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
        model_version: str,
        preprocessing_version: str,
    ) -> None:
        if embedding_dim != self.vector_dim:
            raise ModalEmbeddingError(
                f"Modal embedding_dim {embedding_dim} does not match VECTOR_DIM {self.vector_dim}"
            )
        if model_version != self.model_version:
            raise ModalEmbeddingError(
                f"Modal model version {model_version!r} does not match {self.model_version!r}"
            )
        if preprocessing_version != self.preprocessing_version:
            raise ModalEmbeddingError(
                "Modal preprocessing version "
                f"{preprocessing_version!r} does not match {self.preprocessing_version!r}"
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
