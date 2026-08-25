from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from io import BytesIO
import os
from pathlib import Path
import sys
from threading import Lock
import time
from typing import Any, Callable, Protocol

import numpy as np


NMFP_CODE_COMMIT = "15c6f3bcdf6a6da1daddfe47a1ffa5a0d22deadc"
NMFP_MODEL_ARCHIVE_RECORD = "zenodo-15719945"
NMFP_MODEL_ARCHIVE_MD5 = "ee8a3358fc5e5cdd09d6d2245d395021"
NMFP_MODEL_VERSION = (
    "nmfp-triplet@15c6f3bcdf6a6da1daddfe47a1ffa5a0d22deadc"
    "+zenodo-15719945+ckpt-100"
)
NMFP_PREPROCESSING_VERSION = "nmfp-8khz-mono-1s-hop0.5-mel-v1"
NMFP_MODEL_CONFIG_NAME = "nmfp-triplet-hp_shn-Nppa_1-Na_768"
NMFP_SAMPLE_RATE = 8000
NMFP_WINDOW_SECONDS = 1.0
NMFP_HOP_SECONDS = 0.5
NMFP_EMBEDDING_DIM = 128
NMFP_CHECKPOINT_INDEX = 100


class NMFPConfigurationError(RuntimeError):
    pass


class NMFPVersionMismatchError(NMFPConfigurationError):
    pass


@dataclass(frozen=True)
class NMFPExtractionMetrics:
    cold_start: bool
    model_load_duration_ms: int
    preprocessing_duration_ms: int
    inference_duration_ms: int
    total_duration_ms: int
    audio_duration_seconds: float
    fingerprint_count: int


@dataclass(frozen=True)
class NMFPExtractionResult:
    embeddings: np.ndarray
    timestamps: np.ndarray
    model_version: str
    preprocessing_version: str
    embedding_dim: int
    metrics: NMFPExtractionMetrics


class _InferenceBackend(Protocol):
    def preprocess(self, audio: np.ndarray) -> np.ndarray:
        ...

    def infer(self, mel_segments: np.ndarray, batch_size: int) -> np.ndarray:
        ...


def segment_audio_windows(
    audio: np.ndarray,
    *,
    sample_rate: int = NMFP_SAMPLE_RATE,
    window_seconds: float = NMFP_WINDOW_SECONDS,
    hop_seconds: float = NMFP_HOP_SECONDS,
) -> np.ndarray:
    """Segment mono audio exactly like NMFP inference, discarding a short tail."""

    values = np.asarray(audio, dtype=np.float32)
    if values.ndim != 1:
        raise ValueError("NMFP audio must be mono")
    window_samples = int(round(float(sample_rate) * float(window_seconds)))
    hop_samples = int(round(float(sample_rate) * float(hop_seconds)))
    if window_samples <= 0 or hop_samples <= 0 or hop_samples > window_samples:
        raise ValueError("Invalid NMFP window/hop configuration")
    if len(values) < window_samples:
        return np.empty((0, window_samples), dtype=np.float32)

    count = ((len(values) - window_samples) // hop_samples) + 1
    starts = np.arange(count, dtype=np.int64) * hop_samples
    return np.stack([values[start : start + window_samples] for start in starts]).astype(
        np.float32,
        copy=False,
    )


class _UpstreamNMFPBackend:
    """Persistent adapter around the pinned upstream TensorFlow model."""

    def __init__(self, repository_path: Path, model_config_path: Path):
        if not repository_path.exists():
            raise NMFPConfigurationError(f"NMFP repository does not exist: {repository_path}")
        if not model_config_path.exists():
            raise NMFPConfigurationError(f"NMFP model config does not exist: {model_config_path}")
        repository_commit = _read_repository_commit(repository_path)
        if repository_commit != NMFP_CODE_COMMIT:
            raise NMFPConfigurationError(
                f"Expected NMFP source commit {NMFP_CODE_COMMIT}, got "
                f"{repository_commit or '<unverifiable>'}"
            )

        repository = str(repository_path.resolve())
        if repository not in sys.path:
            sys.path.insert(0, repository)

        try:
            import tensorflow as tf
            from nmfp.audio_processing.melspectrogram import Melspec_layer
            from nmfp.model.utils import (
                get_checkpoint_index_and_restore_model,
                get_fingerprinter,
            )
            from nmfp.utils import load_config
        except Exception as exc:  # pragma: no cover - depends on production runtime
            raise NMFPConfigurationError(
                "NMFP runtime requires TensorFlow 2.13, Essentia, and the pinned neural-music-fp source"
            ) from exc

        config = load_config(model_config_path)
        self._validate_config(config)
        model = get_fingerprinter(config, trainable=False)
        checkpoint_index = get_checkpoint_index_and_restore_model(model, str(model_config_path.parent))
        if int(checkpoint_index) != NMFP_CHECKPOINT_INDEX:
            raise NMFPConfigurationError(
                f"Expected NMFP checkpoint {NMFP_CHECKPOINT_INDEX}, loaded {checkpoint_index}"
            )

        self._melspec = Melspec_layer(
            segment_duration=NMFP_WINDOW_SECONDS,
            fs=NMFP_SAMPLE_RATE,
            n_fft=1024,
            stft_hop=256,
            n_mels=256,
            f_min=160.0,
            f_max=4000.0,
            dynamic_range=80.0,
            scale=True,
        )
        self._model = model
        self._compute = tf.function(
            model,
            experimental_relax_shapes=True,
            reduce_retracing=True,
        )

        # Build/restore all deferred variables during the measured cold-start phase.
        warm_audio = np.zeros(NMFP_SAMPLE_RATE, dtype=np.float32)
        warm_mel = self.preprocess(warm_audio)
        warm_embedding = self.infer(warm_mel, batch_size=1)
        self._validate_embeddings(warm_embedding)

    @staticmethod
    def _validate_config(config: dict[str, Any]) -> None:
        expected = {
            ("MODEL", "NAME"): NMFP_MODEL_CONFIG_NAME,
            ("MODEL", "AUDIO", "FS"): NMFP_SAMPLE_RATE,
            ("MODEL", "AUDIO", "SEGMENT_DUR"): NMFP_WINDOW_SECONDS,
            ("MODEL", "INPUT", "STFT_WIN"): 1024,
            ("MODEL", "INPUT", "STFT_HOP"): 256,
            ("MODEL", "INPUT", "F_MIN"): 160.0,
            ("MODEL", "INPUT", "F_MAX"): 4000.0,
            ("MODEL", "INPUT", "N_MELS"): 256,
            ("MODEL", "INPUT", "DYNAMIC_RANGE"): 80,
            ("MODEL", "INPUT", "SCALE"): True,
            ("MODEL", "ARCHITECTURE", "EMB_SZ"): NMFP_EMBEDDING_DIM,
            ("MODEL", "ARCHITECTURE", "BN"): "layer_norm2d",
            ("TRAIN", "AUDIO", "SEGMENT_HOP_DUR"): NMFP_HOP_SECONDS,
            ("TRAIN", "MIXED_PRECISION"): True,
        }
        for path, wanted in expected.items():
            value: Any = config
            try:
                for key in path:
                    value = value[key]
            except (KeyError, TypeError) as exc:
                raise NMFPConfigurationError(f"NMFP config is missing {'.'.join(path)}") from exc
            if value != wanted:
                raise NMFPConfigurationError(
                    f"NMFP config {'.'.join(path)} must be {wanted!r}, got {value!r}"
                )

    @staticmethod
    def _validate_embeddings(embeddings: np.ndarray) -> None:
        if embeddings.ndim != 2 or embeddings.shape[1] != NMFP_EMBEDDING_DIM:
            raise NMFPConfigurationError(
                f"NMFP checkpoint must emit (*, {NMFP_EMBEDDING_DIM}) embeddings, "
                f"got {embeddings.shape}"
            )

    def preprocess(self, audio: np.ndarray) -> np.ndarray:
        windows = segment_audio_windows(audio)
        if len(windows) == 0:
            return np.empty((0, 256, 0, 1), dtype=np.float32)
        mel = self._melspec.compute_batch(windows).astype(np.float32, copy=False)
        return np.expand_dims(mel, axis=3)

    def infer(self, mel_segments: np.ndarray, batch_size: int) -> np.ndarray:
        if len(mel_segments) == 0:
            return np.empty((0, NMFP_EMBEDDING_DIM), dtype=np.float32)
        outputs: list[np.ndarray] = []
        for start in range(0, len(mel_segments), max(1, int(batch_size))):
            values = self._compute(mel_segments[start : start + batch_size])
            outputs.append(np.asarray(values, dtype=np.float32))
        embeddings = np.concatenate(outputs, axis=0)
        self._validate_embeddings(embeddings)
        return embeddings


BackendFactory = Callable[[Path, Path], _InferenceBackend]


class NMFPFingerprinter:
    """Thread-safe, lazy, process-persistent NMFP fingerprint extractor."""

    model_version = NMFP_MODEL_VERSION
    preprocessing_version = NMFP_PREPROCESSING_VERSION
    embedding_dim = NMFP_EMBEDDING_DIM
    sample_rate = NMFP_SAMPLE_RATE
    window_seconds = NMFP_WINDOW_SECONDS
    hop_seconds = NMFP_HOP_SECONDS

    def __init__(
        self,
        *,
        repository_path: str | Path | None = None,
        model_config_path: str | Path | None = None,
        batch_size: int = 256,
        backend_factory: BackendFactory | None = None,
    ):
        self.repository_path = Path(
            repository_path or os.getenv("NMFP_REPOSITORY_PATH", "/opt/neural-music-fp")
        )
        self.model_config_path = Path(
            model_config_path
            or os.getenv(
                "NMFP_MODEL_CONFIG_PATH",
                "/opt/neural-music-fp/pretrained_models/nmfp-triplet/config.yaml",
            )
        )
        self.batch_size = max(1, int(batch_size))
        self._backend_factory = backend_factory or _UpstreamNMFPBackend
        self._backend: _InferenceBackend | None = None
        self._load_lock = Lock()
        self._inference_lock = Lock()

    @property
    def is_loaded(self) -> bool:
        return self._backend is not None

    def _ensure_loaded(self) -> tuple[_InferenceBackend, bool, int]:
        if self._backend is not None:
            return self._backend, False, 0
        with self._load_lock:
            if self._backend is not None:
                return self._backend, False, 0
            started_at = time.perf_counter()
            backend = self._backend_factory(self.repository_path, self.model_config_path)
            load_ms = _duration_ms(time.perf_counter() - started_at)
            self._backend = backend
            return backend, True, load_ms

    def extract_wav(
        self,
        wav_path: str | Path,
        *,
        offset_seconds: float = 0.0,
        expected_model_version: str = NMFP_MODEL_VERSION,
        expected_preprocessing_version: str = NMFP_PREPROCESSING_VERSION,
    ) -> NMFPExtractionResult:
        path = Path(wav_path)
        if not path.exists():
            raise FileNotFoundError(path)
        return self._extract_from_reader(
            lambda: _read_wav(path),
            offset_seconds=offset_seconds,
            expected_model_version=expected_model_version,
            expected_preprocessing_version=expected_preprocessing_version,
        )

    def extract_wav_bytes(
        self,
        wav_bytes: bytes,
        *,
        offset_seconds: float = 0.0,
        expected_model_version: str = NMFP_MODEL_VERSION,
        expected_preprocessing_version: str = NMFP_PREPROCESSING_VERSION,
    ) -> NMFPExtractionResult:
        return self._extract_from_reader(
            lambda: _read_wav(BytesIO(wav_bytes)),
            offset_seconds=offset_seconds,
            expected_model_version=expected_model_version,
            expected_preprocessing_version=expected_preprocessing_version,
        )

    def extract_audio(
        self,
        audio: np.ndarray,
        sample_rate: int,
        *,
        offset_seconds: float = 0.0,
        expected_model_version: str = NMFP_MODEL_VERSION,
        expected_preprocessing_version: str = NMFP_PREPROCESSING_VERSION,
    ) -> NMFPExtractionResult:
        return self._extract_from_reader(
            lambda: (np.asarray(audio, dtype=np.float32), int(sample_rate)),
            offset_seconds=offset_seconds,
            expected_model_version=expected_model_version,
            expected_preprocessing_version=expected_preprocessing_version,
        )

    def _extract_from_reader(
        self,
        reader: Callable[[], tuple[np.ndarray, int]],
        *,
        offset_seconds: float,
        expected_model_version: str,
        expected_preprocessing_version: str,
    ) -> NMFPExtractionResult:
        self._validate_versions(expected_model_version, expected_preprocessing_version)
        total_started_at = time.perf_counter()
        backend, cold_start, load_ms = self._ensure_loaded()

        preprocess_started_at = time.perf_counter()
        audio, sample_rate = reader()
        audio = np.asarray(audio, dtype=np.float32)
        if audio.ndim != 1:
            raise ValueError("NMFP input must be mono audio")
        if int(sample_rate) != NMFP_SAMPLE_RATE:
            raise ValueError(f"Expected {NMFP_SAMPLE_RATE}Hz NMFP audio, got {sample_rate}")
        mel_segments = backend.preprocess(audio)
        preprocess_ms = _duration_ms(time.perf_counter() - preprocess_started_at)

        inference_started_at = time.perf_counter()
        with self._inference_lock:
            embeddings = np.asarray(
                backend.infer(mel_segments, self.batch_size),
                dtype=np.float32,
            )
        inference_ms = _duration_ms(time.perf_counter() - inference_started_at)
        self._validate_output(embeddings)

        timestamps = (
            float(offset_seconds)
            + np.arange(len(embeddings), dtype=np.float32) * NMFP_HOP_SECONDS
        ).astype(np.float32)
        metrics = NMFPExtractionMetrics(
            cold_start=cold_start,
            model_load_duration_ms=load_ms,
            preprocessing_duration_ms=preprocess_ms,
            inference_duration_ms=inference_ms,
            total_duration_ms=_duration_ms(time.perf_counter() - total_started_at),
            audio_duration_seconds=float(len(audio) / NMFP_SAMPLE_RATE),
            fingerprint_count=len(embeddings),
        )
        return NMFPExtractionResult(
            embeddings=embeddings,
            timestamps=timestamps,
            model_version=NMFP_MODEL_VERSION,
            preprocessing_version=NMFP_PREPROCESSING_VERSION,
            embedding_dim=NMFP_EMBEDDING_DIM,
            metrics=metrics,
        )

    @staticmethod
    def _validate_versions(model_version: str, preprocessing_version: str) -> None:
        if model_version != NMFP_MODEL_VERSION:
            raise NMFPVersionMismatchError(
                f"Expected model version {NMFP_MODEL_VERSION}, got {model_version or '<empty>'}"
            )
        if preprocessing_version != NMFP_PREPROCESSING_VERSION:
            raise NMFPVersionMismatchError(
                "Expected preprocessing version "
                f"{NMFP_PREPROCESSING_VERSION}, got {preprocessing_version or '<empty>'}"
            )

    @staticmethod
    def _validate_output(embeddings: np.ndarray) -> None:
        if embeddings.ndim != 2 or embeddings.shape[1] != NMFP_EMBEDDING_DIM:
            raise NMFPConfigurationError(
                f"NMFP must emit an (N, {NMFP_EMBEDDING_DIM}) array, got {embeddings.shape}"
            )
        if not np.all(np.isfinite(embeddings)):
            raise NMFPConfigurationError("NMFP emitted non-finite embeddings")


def _read_wav(source: str | Path | BytesIO) -> tuple[np.ndarray, int]:
    try:
        import soundfile as sf
    except ImportError as exc:  # pragma: no cover - production dependency failure
        raise NMFPConfigurationError("soundfile is required for NMFP WAV input") from exc
    audio, sample_rate = sf.read(source, dtype="float32", always_2d=False)
    return np.asarray(audio, dtype=np.float32), int(sample_rate)


def _duration_ms(seconds: float) -> int:
    return max(0, int(round(float(seconds) * 1000.0)))


def _read_repository_commit(repository_path: Path) -> str | None:
    """Read the checkout identity without launching a per-request git process."""

    git_dir = repository_path / ".git"
    if git_dir.is_file():
        value = git_dir.read_text(encoding="utf-8").strip()
        if not value.startswith("gitdir:"):
            return None
        referenced = Path(value.removeprefix("gitdir:").strip())
        git_dir = referenced if referenced.is_absolute() else repository_path / referenced
    head_path = git_dir / "HEAD"
    if not head_path.exists():
        return None
    head = head_path.read_text(encoding="utf-8").strip()
    if not head.startswith("ref:"):
        return head.lower()
    ref_name = head.removeprefix("ref:").strip()
    loose_ref = git_dir / ref_name
    if loose_ref.exists():
        return loose_ref.read_text(encoding="utf-8").strip().lower()
    packed_refs = git_dir / "packed-refs"
    if packed_refs.exists():
        suffix = f" {ref_name}"
        for line in packed_refs.read_text(encoding="utf-8").splitlines():
            if not line.startswith(("#", "^")) and line.endswith(suffix):
                return line.split(" ", 1)[0].lower()
    return None


def model_artifact_identity() -> str:
    """Stable non-secret identity suitable for health checks and logs."""

    value = (
        f"{NMFP_MODEL_VERSION}|{NMFP_PREPROCESSING_VERSION}|"
        f"dim={NMFP_EMBEDDING_DIM}|archive-md5={NMFP_MODEL_ARCHIVE_MD5}"
    )
    return sha256(value.encode("utf-8")).hexdigest()
