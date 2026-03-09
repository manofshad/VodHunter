from __future__ import annotations

from io import BytesIO
from typing import List

import numpy as np
import soundfile as sf
import torch
from transformers import ASTFeatureExtractor, ASTModel

DEFAULT_AST_MODEL_NAME = "MIT/ast-finetuned-audioset-10-10-0.4593"


def pick_torch_device() -> torch.device:
    if torch.backends.mps.is_available():
        print("🚀 Using Apple Metal (GPU)")
        return torch.device("mps")
    if torch.cuda.is_available():
        print("🚀 Using CUDA (GPU)")
        return torch.device("cuda")
    print("🐢 Using CPU")
    return torch.device("cpu")


def load_ast_model(model_name: str, device: torch.device) -> tuple[ASTFeatureExtractor, ASTModel]:
    print("⏳ Loading AST model...")
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
            end = start + one_second
            chunk = audio_data[start:end]

            if len(chunk) < one_second:
                chunk = np.pad(chunk, (0, one_second - len(chunk)))

            batch_audio.append(chunk)
            batch_times.append((start / 16000.0) + offset_seconds)

        inputs = feature_extractor(
            batch_audio,
            sampling_rate=16000,
            return_tensors="pt",
        )
        inputs = {k: v.to(device) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = model(**inputs)

        batch_embs = outputs.pooler_output.cpu().numpy()
        embeddings.append(batch_embs)
        timestamps.extend(batch_times)

    if not embeddings:
        return np.zeros((0,)), np.zeros((0,))

    embeddings_np = np.concatenate(embeddings, axis=0)
    timestamps_np = np.array(timestamps, dtype=np.float32)
    return embeddings_np, timestamps_np
