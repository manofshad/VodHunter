import os
import math
import shutil
import soundfile as sf
from datetime import datetime
from typing import Optional

from pipeline.nmfp_inference import NMFP_HOP_SECONDS, NMFP_SAMPLE_RATE
from sources.audio_source import AudioSource
from sources.audio_chunk import AudioChunk


class VODSource(AudioSource):
    def __init__(
        self,
        audio_path: str,
        creator_name: str,
        video_url: str,
        title: str,
        thumbnail_url: str | None = None,
        chunk_seconds: int = 60,
        temp_dir: str = "temp_vod_chunks",
        store=None,
        streamed_at: datetime | None = None,
    ):
        self.audio_path = audio_path
        self.creator_name = creator_name
        self.video_url = video_url
        self.title = title
        self.thumbnail_url = thumbnail_url
        self.chunk_seconds = chunk_seconds
        self.temp_dir = temp_dir
        self.store = store
        self.streamed_at = streamed_at

        self.video_id: int | None = None
        self._creator_id: int | None = None
        self._chunks = []
        self._index = 0
        self._completed = False


    # --------------------
    # Lifecycle
    # --------------------

    def start(self) -> None:
        """
        Prepare the VOD by splitting it into chunks.
        """
        if self.store is None:
            raise ValueError("VODSource requires a store instance")

        # ---- CREATE CREATOR + VIDEO ROW ----
        creator_id = self.store.create_or_get_creator(self.creator_name, self.creator_name)
        self._creator_id = creator_id
        self.video_id = self.store.create_video(
            creator_id=creator_id,
            url=self.video_url,
            title=self.title,
            thumbnail_url=self.thumbnail_url,
            processed=False,
            status="indexing",
            streamed_at=self.streamed_at,
        )

        os.makedirs(self.temp_dir, exist_ok=True)

        audio, sr = sf.read(self.audio_path)
        if sr != NMFP_SAMPLE_RATE:
            raise ValueError(f"Expected {NMFP_SAMPLE_RATE}Hz WAV, got {sr}")

        samples_per_chunk = self.chunk_seconds * NMFP_SAMPLE_RATE
        overlap_samples = int(round(NMFP_HOP_SECONDS * NMFP_SAMPLE_RATE))
        total_samples = len(audio)
        num_chunks = math.ceil(total_samples / samples_per_chunk)

        self._chunks.clear()
        self._index = 0
        self._completed = False

        for i in range(num_chunks):
            logical_start = i * samples_per_chunk
            start = max(0, logical_start - overlap_samples)
            end = min(logical_start + samples_per_chunk, total_samples)
            chunk_audio = audio[start:end]

            chunk_path = os.path.join(
                self.temp_dir,
                f"vod_chunk_{i:06d}.wav",
            )

            sf.write(chunk_path, chunk_audio, NMFP_SAMPLE_RATE, subtype="PCM_16")

            self._chunks.append(
                AudioChunk(
                    audio_path=chunk_path,
                    offset_seconds=start / float(NMFP_SAMPLE_RATE),
                    duration_seconds=len(chunk_audio) / float(NMFP_SAMPLE_RATE),
                )
            )

    def next_chunk(self) -> Optional[AudioChunk]:
        """
        Return the next chunk, or None if finished.
        """
        if self._index >= len(self._chunks):
            if not self._completed and self.video_id is not None:
                update_video_status = getattr(self.store, "update_video_status", None)
                if callable(update_video_status):
                    update_video_status(self.video_id, "searchable")
                else:
                    self.store.mark_video_processed(self.video_id, processed=True)
                self._completed = True
            return None

        chunk = self._chunks[self._index]
        self._index += 1
        return chunk

    def stop(self) -> None:
        """
        Cleanup temp files.
        """
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    @property
    def creator_id(self) -> int | None:
        return self._creator_id

    @property
    def is_finished(self) -> bool:
        return self._index >= len(self._chunks)
