import tempfile
import unittest
from pathlib import Path
import sys

import numpy as np

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipeline.embedder import Embedder
from search.modal_embedding_client import ModalEmbeddingError
from search.modal_query_embedder import ModalQueryEmbedder
from search.modal_types import ModalEmbeddingResponse


class StubClient:
    def __init__(self, response=None, error: Exception | None = None):
        self.response = response
        self.error = error
        self.requests = []

    def embed(self, request):
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        return self.response


class StubFallbackEmbedder(Embedder):
    def __init__(self):
        self.calls = []

    def embed(self, audio_path: str, offset_seconds: float = 0.0):
        self.calls.append((audio_path, offset_seconds))
        return np.array([[1.0, 2.0]], dtype=np.float32), np.array([0.0], dtype=np.float32)


class TestModalQueryEmbedder(unittest.TestCase):
    def test_returns_modal_embeddings(self) -> None:
        client = StubClient(
            response=ModalEmbeddingResponse(
                embeddings=[[0.1, 0.2], [0.3, 0.4]],
                timestamps=[0.0, 1.0],
                model_name="ast",
                embedding_dim=2,
                duration_seconds=2.0,
            )
        )
        embedder = ModalQueryEmbedder(client=client, vector_dim=2)

        with tempfile.NamedTemporaryFile(suffix=".wav") as wav_file:
            wav_file.write(b"wav")
            wav_file.flush()
            embeddings, timestamps = embedder.embed(wav_file.name)

        self.assertEqual(len(client.requests), 1)
        self.assertEqual(client.requests[0].filename, Path(wav_file.name).name)
        np.testing.assert_array_equal(embeddings, np.array([[0.1, 0.2], [0.3, 0.4]], dtype=np.float32))
        np.testing.assert_array_equal(timestamps, np.array([0.0, 1.0], dtype=np.float32))

    def test_falls_back_to_local_embedder_on_modal_error(self) -> None:
        fallback = StubFallbackEmbedder()
        client = StubClient(error=ModalEmbeddingError("boom"))
        embedder = ModalQueryEmbedder(
            client=client,
            vector_dim=2,
            fallback_embedder=fallback,
            fallback_to_local=True,
        )

        with tempfile.NamedTemporaryFile(suffix=".wav") as wav_file:
            wav_file.write(b"wav")
            wav_file.flush()
            embeddings, timestamps = embedder.embed(wav_file.name)

        self.assertEqual(fallback.calls, [(wav_file.name, 0.0)])
        np.testing.assert_array_equal(embeddings, np.array([[1.0, 2.0]], dtype=np.float32))
        np.testing.assert_array_equal(timestamps, np.array([0.0], dtype=np.float32))

    def test_raises_runtime_error_when_fallback_disabled(self) -> None:
        client = StubClient(error=ModalEmbeddingError("boom"))
        embedder = ModalQueryEmbedder(
            client=client,
            vector_dim=2,
            fallback_to_local=False,
        )

        with tempfile.NamedTemporaryFile(suffix=".wav") as wav_file:
            wav_file.write(b"wav")
            wav_file.flush()
            with self.assertRaises(RuntimeError):
                embedder.embed(wav_file.name)

    def test_rejects_invalid_embedding_dim(self) -> None:
        client = StubClient(
            response=ModalEmbeddingResponse(
                embeddings=[[0.1, 0.2]],
                timestamps=[0.0],
                model_name="ast",
                embedding_dim=3,
            )
        )
        embedder = ModalQueryEmbedder(client=client, vector_dim=2, fallback_to_local=False)

        with tempfile.NamedTemporaryFile(suffix=".wav") as wav_file:
            wav_file.write(b"wav")
            wav_file.flush()
            with self.assertRaises(RuntimeError):
                embedder.embed(wav_file.name)
