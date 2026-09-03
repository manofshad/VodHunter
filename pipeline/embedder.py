from __future__ import annotations

from pathlib import Path

import numpy as np

from pipeline.nmfp_inference import (
    NMFP_EMBEDDING_DIM,
    NMFP_MODEL_VERSION,
    NMFP_PREPROCESSING_VERSION,
    NMFPExtractionResult,
    NMFPFingerprinter,
)


class Embedder:
    """Production NMFP embedder shared by VOD ingestion and local queries."""

    model_version = NMFP_MODEL_VERSION
    preprocessing_version = NMFP_PREPROCESSING_VERSION
    embedding_dim = NMFP_EMBEDDING_DIM

    def __init__(self, fingerprinter: NMFPFingerprinter | None = None):
        self.fingerprinter = fingerprinter or NMFPFingerprinter()
        self.last_result: NMFPExtractionResult | None = None

    @property
    def is_loaded(self) -> bool:
        return self.fingerprinter.is_loaded

    def load(self) -> int:
        """Load and warm the pinned NMFP model before serving work."""

        return self.fingerprinter.load()

    def extract(
        self,
        audio_path: str | Path,
        *,
        offset_seconds: float = 0.0,
    ) -> NMFPExtractionResult:
        result = self.fingerprinter.extract_wav(
            audio_path,
            offset_seconds=offset_seconds,
            expected_model_version=self.model_version,
            expected_preprocessing_version=self.preprocessing_version,
        )
        self.last_result = result
        return result

    def embed(
        self,
        audio_path: str,
        offset_seconds: float = 0.0,
    ) -> tuple[np.ndarray, np.ndarray]:
        result = self.extract(audio_path, offset_seconds=offset_seconds)
        return result.embeddings, result.timestamps
