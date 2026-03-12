import pytest
from types import SimpleNamespace
from unittest.mock import patch
from search.modal_embedding_client import ModalEmbeddingClient, ModalEmbeddingError
from search.modal_types import ModalEmbeddingRequest

class StubRemoteFunction:

    def __init__(self, response=None, error: Exception | None=None):
        self.response = response
        self.error = error
        self.calls = []

    def remote(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.response

class TestModalEmbeddingClient:

    def test_looks_up_function_and_returns_response(self) -> None:
        stub_fn = StubRemoteFunction(response={'embeddings': [[0.1, 0.2]], 'timestamps': [0.0], 'model_name': 'ast', 'embedding_dim': 2, 'duration_seconds': 1.0})
        fake_modal = SimpleNamespace(Function=SimpleNamespace(from_name=lambda app_name, function_name: stub_fn))
        with patch('search.modal_embedding_client.modal', fake_modal):
            client = ModalEmbeddingClient('app', 'fn', timeout_seconds=1.0)
            response = client.embed(ModalEmbeddingRequest(wav_bytes=b'wav', request_id='req-1', filename='clip.wav', offset_seconds=0.0, model_version=''))
        assert len(stub_fn.calls) == 1
        assert stub_fn.calls[0]['request_id'] == 'req-1'
        assert response.embedding_dim == 2
        assert response.timestamps == [0.0]

    def test_wraps_remote_errors(self) -> None:
        stub_fn = StubRemoteFunction(error=RuntimeError('remote failed'))
        fake_modal = SimpleNamespace(Function=SimpleNamespace(from_name=lambda app_name, function_name: stub_fn))
        with patch('search.modal_embedding_client.modal', fake_modal):
            client = ModalEmbeddingClient('app', 'fn', timeout_seconds=1.0)
            with pytest.raises(ModalEmbeddingError):
                client.embed(ModalEmbeddingRequest(wav_bytes=b'wav', request_id='req-1', filename='clip.wav', offset_seconds=0.0, model_version=''))
