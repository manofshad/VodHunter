from __future__ import annotations

import threading
import time
from types import SimpleNamespace

from pipeline.scheduled_embedder import (
    BACKLOG_PRIORITY,
    LIVE_PRIORITY,
    ScheduledEmbedder,
)


class BlockingEmbedder:
    model_version = "model"
    preprocessing_version = "preprocess"
    embedding_dim = 128
    is_loaded = True

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.first_started = threading.Event()
        self.release_first = threading.Event()

    def load(self) -> int:
        return 17

    def extract(self, audio_path: str, *, offset_seconds: float = 0.0):
        self.calls.append(audio_path)
        if audio_path == "backlog-1.wav":
            self.first_started.set()
            assert self.release_first.wait(timeout=2)
        if audio_path == "bad.wav":
            raise RuntimeError("bad inference")
        return SimpleNamespace(
            embeddings=[[audio_path]],
            timestamps=[offset_seconds],
        )


def _wait_for_queue_size(scheduler: ScheduledEmbedder, expected: int) -> None:
    deadline = time.monotonic() + 2
    while scheduler._queue.qsize() < expected and time.monotonic() < deadline:
        time.sleep(0.001)
    assert scheduler._queue.qsize() >= expected


def test_live_request_runs_before_already_queued_backlog_work() -> None:
    embedder = BlockingEmbedder()
    scheduler = ScheduledEmbedder(embedder)  # type: ignore[arg-type]
    backlog = scheduler.client(priority=BACKLOG_PRIORITY, source_name="alice:backlog")
    live = scheduler.client(priority=LIVE_PRIORITY, source_name="bob:live")
    errors: list[Exception] = []

    def extract(client, path: str) -> None:
        try:
            client.extract(path)
        except Exception as exc:  # pragma: no cover - diagnostic only
            errors.append(exc)

    first = threading.Thread(target=extract, args=(backlog, "backlog-1.wav"))
    second = threading.Thread(target=extract, args=(backlog, "backlog-2.wav"))
    urgent = threading.Thread(target=extract, args=(live, "live.wav"))
    first.start()
    assert embedder.first_started.wait(timeout=2)
    second.start()
    _wait_for_queue_size(scheduler, 1)
    urgent.start()
    _wait_for_queue_size(scheduler, 2)
    embedder.release_first.set()

    for thread in (first, second, urgent):
        thread.join(timeout=2)
        assert not thread.is_alive()
    scheduler.close()

    assert errors == []
    assert embedder.calls == ["backlog-1.wav", "live.wav", "backlog-2.wav"]


def test_failed_request_does_not_stop_the_shared_consumer() -> None:
    embedder = BlockingEmbedder()
    scheduler = ScheduledEmbedder(embedder)  # type: ignore[arg-type]
    client = scheduler.client(priority=LIVE_PRIORITY, source_name="alice:live")

    try:
        try:
            client.extract("bad.wav")
        except RuntimeError as exc:
            assert str(exc) == "bad inference"
        else:  # pragma: no cover
            raise AssertionError("expected inference failure")

        result = client.extract("good.wav", offset_seconds=12.5)
        assert result.embeddings == [["good.wav"]]
        assert result.timestamps == [12.5]
    finally:
        scheduler.close()
