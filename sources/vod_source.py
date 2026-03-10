import os
import math
import shutil
import soundfile as sf
import numpy as np
from typing import Optional

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
    ):
        self.audio_path = audio_path
        self.creator_name = creator_name
        self.video_url = video_url
        self.title = title
        self.thumbnail_url = thumbnail_url
        self.chunk_seconds = chunk_seconds
        self.temp_dir = temp_dir
        self.store = store

        self.video_id: int | None = None
        self._creator_id: int | None = None
        self._chunks = []
        self._index = 0


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
            processed=True,
        )

        os.makedirs(self.temp_dir, exist_ok=True)

        audio, sr = sf.read(self.audio_path)
        if sr != 16000:
            raise ValueError(f"Expected 16kHz WAV, got {sr}")

        samples_per_chunk = self.chunk_seconds * 16000
        total_samples = len(audio)
        num_chunks = math.ceil(total_samples / samples_per_chunk)

        self._chunks.clear()
        self._index = 0

        for i in range(num_chunks):
            start = i * samples_per_chunk
            end = start + samples_per_chunk
            chunk_audio = audio[start:end]

            if len(chunk_audio) < samples_per_chunk:
                chunk_audio = np.pad(
                    chunk_audio,
                    (0, samples_per_chunk - len(chunk_audio)),
                )

            chunk_path = os.path.join(
                self.temp_dir,
                f"vod_chunk_{i:06d}.wav",
            )

            sf.write(chunk_path, chunk_audio, 16000)

            self._chunks.append(
                AudioChunk(
                    audio_path=chunk_path,
                    offset_seconds=i * self.chunk_seconds,
                    duration_seconds=self.chunk_seconds,
                )
            )

    def next_chunk(self) -> Optional[AudioChunk]:
        """
        Return the next chunk, or None if finished.
        """
        if self._index >= len(self._chunks):
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
