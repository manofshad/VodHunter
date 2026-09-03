from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import logging
from threading import local
import time

import numpy as np

from pipeline.embedder import Embedder


logger = logging.getLogger("uvicorn.error")


def _duration_ms(seconds: float) -> int:
    return max(0, int(round(float(seconds) * 1000.0)))


class LocalQueryEmbedder:
    """FIFO adapter that gives one resident NMFP model a single consumer."""

    def __init__(self, embedder: Embedder):
        self.embedder = embedder
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="nmfp-query")
        self._request_state = local()

    @property
    def model_version(self) -> str:
        return self.embedder.model_version

    @property
    def preprocessing_version(self) -> str:
        return self.embedder.preprocessing_version

    @property
    def embedding_dim(self) -> int:
        return self.embedder.embedding_dim

    @property
    def is_loaded(self) -> bool:
        return self.embedder.is_loaded

    @property
    def last_result(self):
        return getattr(self._request_state, "last_result", None)

    @property
    def last_queue_wait_duration_ms(self) -> int | None:
        return getattr(self._request_state, "last_queue_wait_duration_ms", None)

    def embed(self, wav_path: str) -> tuple[np.ndarray, np.ndarray]:
        queued_at = time.perf_counter()

        def extract():
            queue_wait_ms = _duration_ms(time.perf_counter() - queued_at)
            return self.embedder.extract(audio_path=wav_path, offset_seconds=0.0), queue_wait_ms

        result, queue_wait_ms = self._executor.submit(extract).result()
        self._request_state.last_result = result
        self._request_state.last_queue_wait_duration_ms = queue_wait_ms
        logger.info(
            "timing event=nmfp_local_extract queue_wait_ms=%d model_load_ms=%d "
            "preprocessing_ms=%d inference_ms=%d worker_total_ms=%d fingerprint_count=%d "
            "model_version=%s preprocessing_version=%s",
            queue_wait_ms,
            result.metrics.model_load_duration_ms,
            result.metrics.preprocessing_duration_ms,
            result.metrics.inference_duration_ms,
            result.metrics.total_duration_ms,
            result.metrics.fingerprint_count,
            result.model_version,
            result.preprocessing_version,
        )
        return result.embeddings, result.timestamps

    def close(self, *, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait, cancel_futures=True)
