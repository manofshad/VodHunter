import numpy as np

from pipeline.embedder import Embedder
from pipeline.nmfp_inference import (
    NMFP_EMBEDDING_DIM,
    NMFP_MODEL_VERSION,
    NMFP_PREPROCESSING_VERSION,
    NMFPExtractionMetrics,
    NMFPExtractionResult,
)


class FakeFingerprinter:
    def __init__(self):
        self.calls: list[dict[str, object]] = []
        self.is_loaded = True

    def extract_wav(
        self,
        audio_path,
        *,
        offset_seconds: float,
        expected_model_version: str,
        expected_preprocessing_version: str,
    ) -> NMFPExtractionResult:
        self.calls.append(
            {
                "audio_path": audio_path,
                "offset_seconds": offset_seconds,
                "expected_model_version": expected_model_version,
                "expected_preprocessing_version": expected_preprocessing_version,
            }
        )
        return NMFPExtractionResult(
            embeddings=np.ones((2, NMFP_EMBEDDING_DIM), dtype=np.float32),
            timestamps=np.array([offset_seconds, offset_seconds + 0.5], dtype=np.float32),
            model_version=NMFP_MODEL_VERSION,
            preprocessing_version=NMFP_PREPROCESSING_VERSION,
            embedding_dim=NMFP_EMBEDDING_DIM,
            metrics=NMFPExtractionMetrics(
                cold_start=False,
                model_load_duration_ms=0,
                preprocessing_duration_ms=3,
                inference_duration_ms=4,
                total_duration_ms=7,
                audio_duration_seconds=1.5,
                fingerprint_count=2,
            ),
        )


def test_embedder_exposes_pinned_nmfp_contract() -> None:
    fingerprinter = FakeFingerprinter()
    embedder = Embedder(fingerprinter=fingerprinter)

    assert embedder.model_version == NMFP_MODEL_VERSION
    assert embedder.preprocessing_version == NMFP_PREPROCESSING_VERSION
    assert embedder.embedding_dim == 128
    assert embedder.is_loaded


def test_embed_preserves_tuple_interface_and_validates_versions() -> None:
    fingerprinter = FakeFingerprinter()
    embedder = Embedder(fingerprinter=fingerprinter)

    embeddings, timestamps = embedder.embed("clip.wav", offset_seconds=3.0)

    assert embeddings.shape == (2, NMFP_EMBEDDING_DIM)
    np.testing.assert_array_equal(timestamps, np.array([3.0, 3.5], dtype=np.float32))
    assert fingerprinter.calls == [
        {
            "audio_path": "clip.wav",
            "offset_seconds": 3.0,
            "expected_model_version": NMFP_MODEL_VERSION,
            "expected_preprocessing_version": NMFP_PREPROCESSING_VERSION,
        }
    ]
    assert embedder.last_result is not None
