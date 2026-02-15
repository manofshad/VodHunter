# main/audio_source.py
from abc import ABC, abstractmethod
from typing import Optional
from sources.audio_chunk import AudioChunk


class AudioSource(ABC):
    @abstractmethod
    def start(self) -> None:
        """Prepare the source (connect, spawn ffmpeg, open file, etc.)"""
        pass

    @abstractmethod
    def next_chunk(self) -> Optional[AudioChunk]:
        """
        Return the next available AudioChunk.
        Return None if nothing is ready yet.
        """
        pass

    @abstractmethod
    def stop(self) -> None:
        """Clean shutdown and resource cleanup"""
        pass

    @property
    @abstractmethod
    def is_finished(self) -> bool:
        """
        True if the source will never produce more chunks.
        """
        pass
