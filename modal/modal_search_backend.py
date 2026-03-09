from __future__ import annotations

import os
from pathlib import Path

import modal

from pipeline.ast_inference import (
    DEFAULT_AST_MODEL_NAME,
    compute_ast_embeddings,
    load_ast_model,
    load_wav_bytes,
    pick_torch_device,
)

MODEL_NAME = os.getenv("MODAL_SEARCH_MODEL_NAME", DEFAULT_AST_MODEL_NAME)

image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("libsndfile1")
    .pip_install("numpy", "soundfile", "transformers", "torch")
    .add_local_python_source("pipeline")
)

app = modal.App("vodhunter-search-embedder")

_DEVICE = None
_FEATURE_EXTRACTOR = None
_MODEL = None


def _ensure_loaded():
    global _DEVICE, _FEATURE_EXTRACTOR, _MODEL
    if _DEVICE is not None and _FEATURE_EXTRACTOR is not None and _MODEL is not None:
        return _DEVICE, _FEATURE_EXTRACTOR, _MODEL

    _DEVICE = pick_torch_device()
    _FEATURE_EXTRACTOR, _MODEL = load_ast_model(MODEL_NAME, _DEVICE)
    return _DEVICE, _FEATURE_EXTRACTOR, _MODEL


@app.function(image=image, gpu="T4", scaledown_window=300)
def embed_search_wav(
    wav_bytes: bytes,
    request_id: str = "",
    filename: str = "",
    offset_seconds: float = 0.0,
    model_version: str = "",
):
    del request_id
    del filename

    device, feature_extractor, model = _ensure_loaded()
    audio_data, sample_rate = load_wav_bytes(wav_bytes)
    embeddings, timestamps = compute_ast_embeddings(
        audio_data=audio_data,
        sample_rate=sample_rate,
        feature_extractor=feature_extractor,
        model=model,
        device=device,
        offset_seconds=offset_seconds,
    )

    duration_seconds = 0.0
    if sample_rate > 0:
        duration_seconds = float(len(audio_data) / sample_rate)

    return {
        "embeddings": embeddings.tolist(),
        "timestamps": timestamps.tolist(),
        "model_name": model_version or MODEL_NAME,
        "embedding_dim": int(embeddings.shape[1]) if embeddings.ndim == 2 else 0,
        "duration_seconds": duration_seconds,
    }


@app.local_entrypoint()
def smoke(wav_path: str, model_version: str = ""):
    wav_bytes = Path(wav_path).read_bytes()
    result = embed_search_wav.remote(
        wav_bytes=wav_bytes,
        request_id="smoke",
        filename=Path(wav_path).name,
        offset_seconds=0.0,
        model_version=model_version,
    )
    print(
        {
            "embedding_count": len(result["embeddings"]),
            "embedding_dim": result["embedding_dim"],
            "timestamp_count": len(result["timestamps"]),
            "model_name": result["model_name"],
            "duration_seconds": result["duration_seconds"],
        }
    )
