import logging
import time
from typing import Protocol

import numpy as np

from sources.audio_source import AudioSource
from pipeline.embedder import Embedder
from sources.audio_chunk import AudioChunk


class _FingerprintWriter(Protocol):
    model_version: str
    preprocessing_version: str

    def store_fingerprints(self, video_id: int, timestamps: np.ndarray) -> list[int]: ...

    def append_vectors(
        self,
        embeddings: np.ndarray,
        ids: list[int],
        creator_id: int | None,
    ) -> None: ...


logger = logging.getLogger("uvicorn.error")


class IngestSession:
    def __init__(
        self,
        source: AudioSource,
        embedder: Embedder,
        fingerprints: _FingerprintWriter,
        poll_interval: float = 0.25,
    ):
        self.source = source
        self.embedder = embedder
        self.fingerprints = fingerprints
        self.poll_interval = poll_interval
        self._running = False

    def run(self) -> None:
        """
        Main ingest loop.
        """
        self._running = True
        self.source.start()

        try:
            while self._running:
                chunk = self.source.next_chunk()

                if chunk is None:
                    if self.source.is_finished:
                        break
                    time.sleep(self.poll_interval)
                    continue

                extract = getattr(self.embedder, "extract", None)
                if callable(extract):
                    extraction = extract(
                        audio_path=chunk.audio_path,
                        offset_seconds=chunk.offset_seconds,
                    )
                    embeddings = extraction.embeddings
                    timestamps = extraction.timestamps
                    metrics = extraction.metrics
                    store_model_version = getattr(self.fingerprints, "model_version", None)
                    store_preprocessing_version = getattr(
                        self.fingerprints, "preprocessing_version", None
                    )
                    if (
                        store_model_version is not None
                        and extraction.model_version != store_model_version
                    ):
                        raise ValueError(
                            "Ingest NMFP model version does not match the vector index"
                        )
                    if (
                        store_preprocessing_version is not None
                        and extraction.preprocessing_version
                        != store_preprocessing_version
                    ):
                        raise ValueError(
                            "Ingest NMFP preprocessing version does not match the vector index"
                        )
                    logger.info(
                        "timing event=nmfp_ingest_extract video_id=%s offset_seconds=%.3f "
                        "audio_seconds=%.3f cold_start=%s model_load_ms=%d preprocessing_ms=%d "
                        "inference_ms=%d total_ms=%d fingerprint_count=%d model_version=%s "
                        "preprocessing_version=%s",
                        self.source.video_id,
                        chunk.offset_seconds,
                        metrics.audio_duration_seconds,
                        metrics.cold_start,
                        metrics.model_load_duration_ms,
                        metrics.preprocessing_duration_ms,
                        metrics.inference_duration_ms,
                        metrics.total_duration_ms,
                        metrics.fingerprint_count,
                        extraction.model_version,
                        extraction.preprocessing_version,
                    )
                else:
                    # Structural compatibility for simple test doubles.
                    embeddings, timestamps = self.embedder.embed(
                        audio_path=chunk.audio_path,
                        offset_seconds=chunk.offset_seconds,
                    )

                if len(timestamps) == 0:
                    raise RuntimeError(
                        "NMFP produced no fingerprints for "
                        f"video_id={self.source.video_id} offset_seconds={chunk.offset_seconds:.3f} "
                        f"duration_seconds={chunk.duration_seconds:.3f}; refusing to advance cursor"
                    )

                ids = self.fingerprints.store_fingerprints(
                    video_id=self.source.video_id,
                    timestamps=timestamps,
                )

                self.fingerprints.append_vectors(
                    embeddings=embeddings,
                    ids=ids,
                    creator_id=self.source.creator_id,
                )

        finally:
            self.source.stop()

    def stop(self) -> None:
        """
        Request a clean stop.
        """
        self._running = False
