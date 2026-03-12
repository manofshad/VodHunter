import tempfile
from unittest.mock import patch
import numpy as np
from pipeline.embedder import Embedder

class FakeTensor:

    def to(self, device):
        return self

class FakeFeatureExtractor:

    def __call__(self, batch_audio, sampling_rate, return_tensors):
        return {'input_values': FakeTensor()}

class FakePoolerOutput:

    def __init__(self, values: np.ndarray):
        self.values = values

    def cpu(self):
        return self

    def numpy(self):
        return self.values

class FakeModelOutput:

    def __init__(self, batch_size: int):
        self.pooler_output = FakePoolerOutput(np.ones((batch_size, 3), dtype=np.float32))

class FakeModel:

    def __init__(self):
        self.eval_calls = 0
        self.to_device = None

    def to(self, device):
        self.to_device = device
        return self

    def eval(self):
        self.eval_calls += 1
        return self

    def __call__(self, **inputs):
        batch_size = 1
        return FakeModelOutput(batch_size=batch_size)

class TestEmbedder:

    def test_constructor_does_not_load_model(self) -> None:
        with patch('pipeline.embedder.load_ast_model') as model_loader:
            embedder = Embedder()
        assert embedder.feature_extractor is None
        assert embedder.model is None
        model_loader.assert_not_called()

    def test_embed_loads_model_once_and_reuses_it(self) -> None:
        fake_model = FakeModel()
        audio = np.ones(16000, dtype=np.float32)
        with tempfile.NamedTemporaryFile(suffix='.wav') as wav_file, patch('pipeline.embedder.load_wav_file', return_value=(audio, 16000)), patch('pipeline.embedder.load_ast_model', return_value=(FakeFeatureExtractor(), fake_model)) as model_loader:
            embedder = Embedder()
            first_embeddings, first_timestamps = embedder.embed(wav_file.name)
            second_embeddings, second_timestamps = embedder.embed(wav_file.name)
        assert model_loader.call_count == 1
        assert fake_model.eval_calls == 0
        assert first_embeddings.shape == (1, 3)
        assert second_embeddings.shape == (1, 3)
        np.testing.assert_array_equal(first_timestamps, np.array([0.0], dtype=np.float32))
        np.testing.assert_array_equal(second_timestamps, np.array([0.0], dtype=np.float32))
