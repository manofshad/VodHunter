from typing import Optional, Tuple

import numpy as np
import torch
from transformers import ASTFeatureExtractor, ASTModel

from pipeline.ast_inference import (
    DEFAULT_AST_MODEL_NAME,
    compute_ast_embeddings,
    load_ast_model,
    load_wav_file,
    pick_torch_device,
)


class Embedder:
    def __init__(self, model_name: str = DEFAULT_AST_MODEL_NAME):
        self.model_name = model_name
        self.device = self._pick_device()
        self.feature_extractor: Optional[ASTFeatureExtractor] = None
        self.model: Optional[ASTModel] = None

    def _pick_device(self) -> torch.device:
        return pick_torch_device()

    def _ensure_loaded(self) -> None:
        if self.feature_extractor is not None and self.model is not None:
            return

        self.feature_extractor, self.model = load_ast_model(self.model_name, self.device)

    def embed(
        self,
        audio_path: str,
        offset_seconds: float = 0.0,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Parameters:
            audio_path: path to WAV file (16kHz mono)
            offset_seconds: absolute offset to add to timestamps

        Returns:
            embeddings: (N, D)
            timestamps: (N,)
        """
        self._ensure_loaded()
        assert self.feature_extractor is not None
        assert self.model is not None

        audio_data, sr = load_wav_file(audio_path)
        return compute_ast_embeddings(
            audio_data=audio_data,
            sample_rate=sr,
            feature_extractor=self.feature_extractor,
            model=self.model,
            device=self.device,
            offset_seconds=offset_seconds,
        )
