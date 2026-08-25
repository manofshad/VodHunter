from __future__ import annotations

from dataclasses import dataclass

from pipeline.nmfp_inference import NMFP_MODEL_VERSION, NMFP_PREPROCESSING_VERSION


@dataclass(frozen=True)
class ModalEmbeddingRequest:
    wav_bytes: bytes
    request_id: str
    filename: str
    offset_seconds: float
    model_version: str = NMFP_MODEL_VERSION
    preprocessing_version: str = NMFP_PREPROCESSING_VERSION


@dataclass(frozen=True)
class ModalEmbeddingResponse:
    embeddings: list[list[float]]
    timestamps: list[float]
    model_name: str
    embedding_dim: int
    duration_seconds: float | None = None
    model_version: str = NMFP_MODEL_VERSION
    preprocessing_version: str = NMFP_PREPROCESSING_VERSION
    cold_start: bool = False
    model_load_duration_ms: int = 0
    preprocessing_duration_ms: int = 0
    inference_duration_ms: int = 0
    total_duration_ms: int = 0
