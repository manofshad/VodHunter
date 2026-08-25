import logging
import time

from sources.audio_source import AudioSource
from pipeline.embedder import Embedder
from storage.vector_store import VectorStore
from sources.audio_chunk import AudioChunk


logger = logging.getLogger("uvicorn.error")


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

                extract = getattr(self.embedder, "extract", None)
                if callable(extract):
                    extraction = extract(
                        audio_path=chunk.audio_path,
                        offset_seconds=chunk.offset_seconds,
                    )
                    embeddings = extraction.embeddings
                    timestamps = extraction.timestamps
                    metrics = extraction.metrics
                    store_model_version = getattr(self.store, "model_version", None)
                    store_preprocessing_version = getattr(
                        self.store, "preprocessing_version", None
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
                    continue

                ids = self.store.store_fingerprints(
                    video_id=self.source.video_id,
                    timestamps=timestamps,
                )

                self.store.append_vectors(
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
