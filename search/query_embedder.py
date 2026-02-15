import numpy as np

from pipeline.embedder import Embedder


class QueryEmbedder:
    def __init__(self, embedder: Embedder):
        self.embedder = embedder

    def embed(self, wav_path: str) -> tuple[np.ndarray, np.ndarray]:
        embeddings, timestamps = self.embedder.embed(
            audio_path=wav_path,
            offset_seconds=0.0,
        )
        return embeddings, timestamps
