# main/audio_chunk.py
from dataclasses import dataclass

@dataclass
class AudioChunk:
    audio_path: str          # path to wav file
    offset_seconds: float    # absolute offset in stream
    duration_seconds: float # length of this chunk
