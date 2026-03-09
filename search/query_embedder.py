from __future__ import annotations

from typing import Protocol

import numpy as np


class QueryEmbedder(Protocol):
    def embed(self, wav_path: str) -> tuple[np.ndarray, np.ndarray]:
        ...
