from __future__ import annotations

from pathlib import Path

import modal

from pipeline.embedder import Embedder
from pipeline.nmfp_inference import (
    NMFP_CODE_COMMIT,
    NMFP_MODEL_ARCHIVE_MD5,
    NMFP_MODEL_VERSION,
    NMFP_PREPROCESSING_VERSION,
)


NMFP_REPOSITORY_PATH = "/opt/neural-music-fp"
NMFP_MODEL_CONFIG_PATH = (
    "/opt/neural-music-fp/pretrained_models/nmfp-triplet/config.yaml"
)
NMFP_MODEL_URL = (
    "https://zenodo.org/api/records/15719945/files/nmfp-triplet.zip/content"
)

image = (
    modal.Image.from_registry(
        "nvidia/cuda:11.8.0-cudnn8-runtime-ubuntu22.04",
        add_python="3.11",
    )
    .apt_install("libsndfile1", "git", "curl", "unzip")
    .pip_install(
        "tensorflow==2.13.0",
        "essentia==2.1b6.dev1110",
        "numpy==1.24.3",
        "pandas==2.0.3",
        "PyYAML==6.0.2",
        "scipy==1.11.4",
        "soundfile==0.13.1",
    )
    .run_commands(
        f"git clone https://github.com/raraz15/neural-music-fp.git {NMFP_REPOSITORY_PATH}",
        f"git -C {NMFP_REPOSITORY_PATH} checkout --detach {NMFP_CODE_COMMIT}",
        f"curl -L --fail --silent --show-error -o /tmp/nmfp-triplet.zip {NMFP_MODEL_URL}",
        f"echo '{NMFP_MODEL_ARCHIVE_MD5}  /tmp/nmfp-triplet.zip' | md5sum -c -",
        f"mkdir -p {NMFP_REPOSITORY_PATH}/pretrained_models",
        f"unzip -q /tmp/nmfp-triplet.zip -d {NMFP_REPOSITORY_PATH}/pretrained_models",
        "rm /tmp/nmfp-triplet.zip",
    )
    .add_local_python_source("pipeline")
)

app = modal.App("vodhunter-search-embedder")

# The object and its TensorFlow model live for the lifetime of a warm Modal container.
_EMBEDDER = Embedder()


@app.function(
    image=image,
    gpu="T4",
    min_containers=0,
    max_containers=1,
    scaledown_window=300,
)
def embed_search_wav(
    wav_bytes: bytes,
    request_id: str = "",
    filename: str = "",
    offset_seconds: float = 0.0,
    model_version: str = NMFP_MODEL_VERSION,
    preprocessing_version: str = NMFP_PREPROCESSING_VERSION,
):
    del request_id
    del filename

    result = _EMBEDDER.fingerprinter.extract_wav_bytes(
        wav_bytes,
        offset_seconds=offset_seconds,
        expected_model_version=model_version,
        expected_preprocessing_version=preprocessing_version,
    )

    return {
        "embeddings": result.embeddings.tolist(),
        "timestamps": result.timestamps.tolist(),
        "model_name": result.model_version,
        "model_version": result.model_version,
        "preprocessing_version": result.preprocessing_version,
        "embedding_dim": result.embedding_dim,
        "duration_seconds": result.metrics.audio_duration_seconds,
        "cold_start": result.metrics.cold_start,
        "model_load_duration_ms": result.metrics.model_load_duration_ms,
        "preprocessing_duration_ms": result.metrics.preprocessing_duration_ms,
        "inference_duration_ms": result.metrics.inference_duration_ms,
        "total_duration_ms": result.metrics.total_duration_ms,
    }


@app.local_entrypoint()
def smoke(
    wav_path: str,
    model_version: str = NMFP_MODEL_VERSION,
    preprocessing_version: str = NMFP_PREPROCESSING_VERSION,
):
    wav_bytes = Path(wav_path).read_bytes()
    result = embed_search_wav.remote(
        wav_bytes=wav_bytes,
        request_id="smoke",
        filename=Path(wav_path).name,
        offset_seconds=0.0,
        model_version=model_version,
        preprocessing_version=preprocessing_version,
    )
    print(
        {
            "embedding_count": len(result["embeddings"]),
            "embedding_dim": result["embedding_dim"],
            "timestamp_count": len(result["timestamps"]),
            "model_name": result["model_name"],
            "preprocessing_version": result["preprocessing_version"],
            "duration_seconds": result["duration_seconds"],
            "cold_start": result["cold_start"],
            "model_load_duration_ms": result["model_load_duration_ms"],
            "preprocessing_duration_ms": result["preprocessing_duration_ms"],
            "inference_duration_ms": result["inference_duration_ms"],
        }
    )
