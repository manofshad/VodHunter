from __future__ import annotations

from typing import Protocol

import numpy as np

from pipeline.nmfp_inference import NMFPExtractionResult


class QueryEmbedder(Protocol):
    last_result: NMFPExtractionResult | None

    def embed(self, wav_path: str) -> tuple[np.ndarray, np.ndarray]:
        ...
