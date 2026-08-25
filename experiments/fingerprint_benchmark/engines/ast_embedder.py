from __future__ import annotations

from io import BytesIO
from typing import List, Optional

import numpy as np
import soundfile as sf
import torch
from transformers import ASTFeatureExtractor, ASTModel


DEFAULT_AST_MODEL_NAME = "MIT/ast-finetuned-audioset-10-10-0.4593"


def pick_torch_device() -> torch.device:
    if torch.backends.mps.is_available():
        return torch.device("mps")
    if torch.cuda.is_available():
        return torch.device("cuda")
    return torch.device("cpu")


def load_ast_model(model_name: str, device: torch.device) -> tuple[ASTFeatureExtractor, ASTModel]:
    feature_extractor = ASTFeatureExtractor.from_pretrained(model_name)
    model = ASTModel.from_pretrained(model_name).to(device)
    model.eval()
    return feature_extractor, model


def load_wav_file(audio_path: str) -> tuple[np.ndarray, int]:
    return sf.read(audio_path)


def load_wav_bytes(wav_bytes: bytes) -> tuple[np.ndarray, int]:
    return sf.read(BytesIO(wav_bytes))


def compute_ast_embeddings(
    audio_data: np.ndarray,
    sample_rate: int,
    feature_extractor: ASTFeatureExtractor,
    model: ASTModel,
    device: torch.device,
    offset_seconds: float = 0.0,
    batch_size: int = 8,
) -> tuple[np.ndarray, np.ndarray]:
    if sample_rate != 16000:
        raise ValueError(f"Expected 16kHz audio, got {sample_rate}")

    one_second = 16000
    total_samples = len(audio_data)
    num_chunks = int(np.ceil(total_samples / one_second))
    embeddings: List[np.ndarray] = []
    timestamps: List[float] = []

    for i in range(0, num_chunks, batch_size):
        batch_audio = []
        batch_times = []
        for j in range(i, min(i + batch_size, num_chunks)):
            start = j * one_second
            chunk = audio_data[start : start + one_second]
            if len(chunk) < one_second:
                chunk = np.pad(chunk, (0, one_second - len(chunk)))
            batch_audio.append(chunk)
            batch_times.append((start / 16000.0) + offset_seconds)

        inputs = feature_extractor(batch_audio, sampling_rate=16000, return_tensors="pt")
        inputs = {key: value.to(device) for key, value in inputs.items()}
        with torch.no_grad():
            outputs = model(**inputs)
        embeddings.append(outputs.pooler_output.cpu().numpy())
        timestamps.extend(batch_times)

    if not embeddings:
        return np.zeros((0,)), np.zeros((0,))
    return (
        np.concatenate(embeddings, axis=0),
        np.array(timestamps, dtype=np.float32),
    )


class ASTEmbedder:
    """Legacy AST adapter retained only for the benchmark experiment."""

    def __init__(self, model_name: str = DEFAULT_AST_MODEL_NAME):
        self.model_name = model_name
        self.device = pick_torch_device()
        self.feature_extractor: Optional[ASTFeatureExtractor] = None
        self.model: Optional[ASTModel] = None

    def _ensure_loaded(self) -> None:
        if self.feature_extractor is None or self.model is None:
            self.feature_extractor, self.model = load_ast_model(self.model_name, self.device)

    def embed(
        self,
        audio_path: str,
        offset_seconds: float = 0.0,
    ) -> tuple[np.ndarray, np.ndarray]:
        self._ensure_loaded()
        assert self.feature_extractor is not None
        assert self.model is not None
        audio_data, sample_rate = load_wav_file(audio_path)
        return compute_ast_embeddings(
            audio_data=audio_data,
            sample_rate=sample_rate,
            feature_extractor=self.feature_extractor,
            model=self.model,
            device=self.device,
            offset_seconds=offset_seconds,
        )
