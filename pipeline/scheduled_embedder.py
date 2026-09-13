"""Priority scheduling for callers sharing one process-resident embedder."""

from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass, field
import itertools
import logging
from queue import PriorityQueue
from threading import Thread
import time
from typing import Any

from pipeline.embedder import Embedder


logger = logging.getLogger("uvicorn.error")

LIVE_PRIORITY = 0
BACKLOG_PRIORITY = 10
_STOP_PRIORITY = 1_000_000


@dataclass(order=True)
class _InferenceRequest:
    priority: int
    sequence: int
    source_name: str = field(compare=False)
    audio_path: str = field(compare=False)
    offset_seconds: float = field(compare=False)
    future: Future[Any] = field(compare=False)
    queued_at: float = field(compare=False)


class ScheduledEmbedder:
    """Run extraction requests from many sessions through one FIFO consumer."""

    def __init__(self, embedder: Embedder):
        self.embedder = embedder
        self._queue: PriorityQueue[_InferenceRequest] = PriorityQueue()
        self._sequence = itertools.count()
        self._closed = False
        self._thread = Thread(
            target=self._consume,
            name="nmfp-ingest",
            daemon=True,
        )
        self._thread.start()

    def load(self) -> int:
        return self.embedder.load()

    def client(self, *, priority: int, source_name: str) -> "ScheduledEmbedderClient":
        if self._closed:
            raise RuntimeError("scheduled embedder is closed")
        return ScheduledEmbedderClient(
            scheduler=self,
            priority=int(priority),
            source_name=str(source_name),
        )

    def extract(
        self,
        *,
        priority: int,
        source_name: str,
        audio_path: str,
        offset_seconds: float,
    ):
        if self._closed:
            raise RuntimeError("scheduled embedder is closed")
        future: Future[Any] = Future()
        queued_at = time.perf_counter()
        self._queue.put(
            _InferenceRequest(
                priority=int(priority),
                sequence=next(self._sequence),
                source_name=source_name,
                audio_path=audio_path,
                offset_seconds=float(offset_seconds),
                future=future,
                queued_at=queued_at,
            )
        )
        return future.result()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        stop_future: Future[Any] = Future()
        self._queue.put(
            _InferenceRequest(
                priority=_STOP_PRIORITY,
                sequence=next(self._sequence),
                source_name="shutdown",
                audio_path="",
                offset_seconds=0.0,
                future=stop_future,
                queued_at=time.perf_counter(),
            )
        )
        self._thread.join()

    def _consume(self) -> None:
        while True:
            request = self._queue.get()
            try:
                if request.priority == _STOP_PRIORITY and self._closed:
                    request.future.set_result(None)
                    return
                started_at = time.perf_counter()
                result = self.embedder.extract(
                    audio_path=request.audio_path,
                    offset_seconds=request.offset_seconds,
                )
                request.future.set_result(result)
                logger.info(
                    "timing event=nmfp_ingest_queue source=%s priority=%d "
                    "queue_wait_ms=%d service_ms=%d queue_depth=%d",
                    request.source_name,
                    request.priority,
                    round((started_at - request.queued_at) * 1000),
                    round((time.perf_counter() - started_at) * 1000),
                    self._queue.qsize(),
                )
            except Exception as exc:
                request.future.set_exception(exc)
            finally:
                self._queue.task_done()


class ScheduledEmbedderClient:
    """Embedder-compatible view with a fixed scheduling priority."""

    def __init__(
        self,
        *,
        scheduler: ScheduledEmbedder,
        priority: int,
        source_name: str,
    ) -> None:
        self.scheduler = scheduler
        self.priority = int(priority)
        self.source_name = source_name

    @property
    def model_version(self) -> str:
        return self.scheduler.embedder.model_version

    @property
    def preprocessing_version(self) -> str:
        return self.scheduler.embedder.preprocessing_version

    @property
    def embedding_dim(self) -> int:
        return self.scheduler.embedder.embedding_dim

    @property
    def is_loaded(self) -> bool:
        return self.scheduler.embedder.is_loaded

    def extract(self, audio_path: str, *, offset_seconds: float = 0.0):
        return self.scheduler.extract(
            priority=self.priority,
            source_name=self.source_name,
            audio_path=audio_path,
            offset_seconds=offset_seconds,
        )

    def embed(self, audio_path: str, offset_seconds: float = 0.0):
        result = self.extract(audio_path, offset_seconds=offset_seconds)
        return result.embeddings, result.timestamps
