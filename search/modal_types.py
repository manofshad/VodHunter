from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ModalEmbeddingRequest:
    wav_bytes: bytes
    request_id: str
    filename: str
    offset_seconds: float
    model_version: str = ""


@dataclass(frozen=True)
class ModalEmbeddingResponse:
    embeddings: list[list[float]]
    timestamps: list[float]
    model_name: str
    embedding_dim: int
    duration_seconds: float | None = None
