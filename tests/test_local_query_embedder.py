from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import time

import numpy as np

from pipeline.nmfp_inference import (
    NMFP_EMBEDDING_DIM,
    NMFP_MODEL_VERSION,
    NMFP_PREPROCESSING_VERSION,
    NMFPExtractionMetrics,
    NMFPExtractionResult,
)
from search.local_query_embedder import LocalQueryEmbedder


class RecordingEmbedder:
    model_version = NMFP_MODEL_VERSION
    preprocessing_version = NMFP_PREPROCESSING_VERSION
    embedding_dim = NMFP_EMBEDDING_DIM
    is_loaded = True

    def __init__(self):
        self._lock = Lock()
        self.active = 0
        self.max_active = 0
        self.order: list[str] = []

    def extract(self, audio_path: str, offset_seconds: float) -> NMFPExtractionResult:
        with self._lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            self.order.append(audio_path)
        try:
            time.sleep(0.04)
            marker = 1.0 if audio_path == "first.wav" else 2.0
            return NMFPExtractionResult(
                embeddings=np.full((1, NMFP_EMBEDDING_DIM), marker, dtype=np.float32),
                timestamps=np.array([offset_seconds], dtype=np.float32),
                model_version=self.model_version,
                preprocessing_version=self.preprocessing_version,
                embedding_dim=self.embedding_dim,
                metrics=NMFPExtractionMetrics(
                    cold_start=False,
                    model_load_duration_ms=0,
                    preprocessing_duration_ms=int(marker),
                    inference_duration_ms=2,
                    total_duration_ms=3,
                    audio_duration_seconds=1.0,
                    fingerprint_count=1,
                ),
            )
        finally:
            with self._lock:
                self.active -= 1


def test_local_query_embedding_is_fifo_and_single_consumer() -> None:
    embedder = RecordingEmbedder()
    query_embedder = LocalQueryEmbedder(embedder)
    try:
        with ThreadPoolExecutor(max_workers=2) as callers:
            first = callers.submit(query_embedder.embed, "first.wav")
            time.sleep(0.01)
            second = callers.submit(query_embedder.embed, "second.wav")
            first.result()
            second.result()

        assert embedder.order == ["first.wav", "second.wav"]
        assert embedder.max_active == 1
    finally:
        query_embedder.close()


def test_local_query_metrics_are_kept_per_calling_thread() -> None:
    query_embedder = LocalQueryEmbedder(RecordingEmbedder())

    def run(path: str):
        query_embedder.embed(path)
        return (
            query_embedder.last_result.metrics.preprocessing_duration_ms,
            query_embedder.last_queue_wait_duration_ms,
        )

    try:
        with ThreadPoolExecutor(max_workers=2) as callers:
            first = callers.submit(run, "first.wav")
            time.sleep(0.01)
            second = callers.submit(run, "second.wav")
            assert first.result()[0] == 1
            second_metrics = second.result()
            assert second_metrics[0] == 2
            assert second_metrics[1] >= 1
    finally:
        query_embedder.close()
