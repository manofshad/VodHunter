import time

from sources.audio_source import AudioSource
from pipeline.embedder import Embedder
from storage.vector_store import VectorStore
from sources.audio_chunk import AudioChunk


class IngestSession:
    def __init__(
        self,
        source: AudioSource,
        embedder: Embedder,
        store: VectorStore,
        poll_interval: float = 0.25,
    ):
        self.source = source
        self.embedder = embedder
        self.store = store
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

                embeddings, timestamps = self.embedder.embed(
                    audio_path=chunk.audio_path,
                    offset_seconds=chunk.offset_seconds,
                )

                if len(timestamps) == 0:
                    continue

                ids = self.store.store_fingerprints(
                    video_id=self.source.video_id,
                    timestamps=timestamps,
                )

                self.store.append_vectors(embeddings, ids)

        finally:
            self.source.stop()

    def stop(self) -> None:
        """
        Request a clean stop.
        """
        self._running = False
