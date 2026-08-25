from __future__ import annotations

import numpy as np
import pytest

from pipeline.nmfp_inference import (
    NMFPConfigurationError,
    NMFPFingerprinter,
    NMFPVersionMismatchError,
    NMFP_EMBEDDING_DIM,
    NMFP_MODEL_VERSION,
    NMFP_PREPROCESSING_VERSION,
    NMFP_CODE_COMMIT,
    _read_repository_commit,
    model_artifact_identity,
    segment_audio_windows,
)


class FakeBackend:
    def __init__(self, embedding_dim: int = NMFP_EMBEDDING_DIM):
        self.embedding_dim = embedding_dim
        self.preprocess_calls = 0
        self.infer_calls = 0

    def preprocess(self, audio: np.ndarray) -> np.ndarray:
        self.preprocess_calls += 1
        return segment_audio_windows(audio)

    def infer(self, mel_segments: np.ndarray, batch_size: int) -> np.ndarray:
        self.infer_calls += 1
        assert batch_size > 0
        return np.ones((len(mel_segments), self.embedding_dim), dtype=np.float32)


def test_segment_audio_windows_uses_one_second_window_and_half_second_hop() -> None:
    audio = np.arange(16_000, dtype=np.float32)

    windows = segment_audio_windows(audio)

    assert windows.shape == (3, 8_000)
    np.testing.assert_array_equal(windows[:, 0], np.array([0, 4_000, 8_000]))


def test_segment_audio_windows_discards_short_remainder() -> None:
    assert segment_audio_windows(np.zeros(7_999, dtype=np.float32)).shape == (0, 8_000)
    assert segment_audio_windows(np.zeros(11_999, dtype=np.float32)).shape == (1, 8_000)


def test_fingerprinter_loads_once_and_reports_cold_then_warm_timings() -> None:
    backend = FakeBackend()
    factory_calls: list[tuple[object, object]] = []

    def factory(repository_path, model_config_path):
        factory_calls.append((repository_path, model_config_path))
        return backend

    fingerprinter = NMFPFingerprinter(backend_factory=factory)
    audio = np.zeros(16_000, dtype=np.float32)

    first = fingerprinter.extract_audio(audio, 8_000, offset_seconds=7.5)
    second = fingerprinter.extract_audio(audio, 8_000)

    assert len(factory_calls) == 1
    assert first.metrics.cold_start is True
    assert second.metrics.cold_start is False
    assert first.metrics.audio_duration_seconds == 2.0
    assert first.metrics.fingerprint_count == 3
    assert first.embedding_dim == NMFP_EMBEDDING_DIM
    assert first.model_version == NMFP_MODEL_VERSION
    assert first.preprocessing_version == NMFP_PREPROCESSING_VERSION
    np.testing.assert_array_equal(
        first.timestamps,
        np.array([7.5, 8.0, 8.5], dtype=np.float32),
    )
    assert first.metrics.preprocessing_duration_ms >= 0
    assert first.metrics.inference_duration_ms >= 0
    assert first.metrics.total_duration_ms >= 0


@pytest.mark.parametrize(
    ("model_version", "preprocessing_version"),
    [
        ("wrong-model", NMFP_PREPROCESSING_VERSION),
        (NMFP_MODEL_VERSION, "wrong-preprocessing"),
    ],
)
def test_version_mismatch_is_rejected_before_model_load(
    model_version: str,
    preprocessing_version: str,
) -> None:
    factory_called = False

    def factory(repository_path, model_config_path):
        nonlocal factory_called
        factory_called = True
        return FakeBackend()

    fingerprinter = NMFPFingerprinter(backend_factory=factory)

    with pytest.raises(NMFPVersionMismatchError):
        fingerprinter.extract_audio(
            np.zeros(8_000, dtype=np.float32),
            8_000,
            expected_model_version=model_version,
            expected_preprocessing_version=preprocessing_version,
        )

    assert factory_called is False


def test_rejects_wrong_sample_rate() -> None:
    fingerprinter = NMFPFingerprinter(backend_factory=lambda *_: FakeBackend())

    with pytest.raises(ValueError, match="Expected 8000Hz"):
        fingerprinter.extract_audio(np.zeros(16_000, dtype=np.float32), 16_000)


def test_rejects_wrong_embedding_dimension() -> None:
    fingerprinter = NMFPFingerprinter(backend_factory=lambda *_: FakeBackend(64))

    with pytest.raises(NMFPConfigurationError, match="128"):
        fingerprinter.extract_audio(np.zeros(8_000, dtype=np.float32), 8_000)


def test_artifact_identity_is_stable_sha256() -> None:
    identity = model_artifact_identity()
    assert len(identity) == 64
    assert identity == model_artifact_identity()


def test_repository_commit_reader_handles_detached_head(tmp_path) -> None:
    git_dir = tmp_path / ".git"
    git_dir.mkdir()
    (git_dir / "HEAD").write_text(f"{NMFP_CODE_COMMIT}\n", encoding="utf-8")

    assert _read_repository_commit(tmp_path) == NMFP_CODE_COMMIT
