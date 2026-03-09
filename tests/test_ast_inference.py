import unittest
from pathlib import Path
import sys

import numpy as np

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipeline.ast_inference import compute_ast_embeddings


class FakeTensor:
    def to(self, device):
        return self


class FakeFeatureExtractor:
    def __call__(self, batch_audio, sampling_rate, return_tensors):
        return {"input_values": FakeTensor()}


class FakePoolerOutput:
    def __init__(self, values: np.ndarray):
        self.values = values

    def cpu(self):
        return self

    def numpy(self):
        return self.values


class FakeModelOutput:
    def __init__(self, batch_size: int):
        self.pooler_output = FakePoolerOutput(
            np.ones((batch_size, 4), dtype=np.float32)
        )


class FakeModel:
    def __call__(self, **inputs):
        return FakeModelOutput(batch_size=2)


class TestAstInference(unittest.TestCase):
    def test_compute_embeddings_splits_audio_into_one_second_chunks(self) -> None:
        audio = np.ones(32000, dtype=np.float32)

        embeddings, timestamps = compute_ast_embeddings(
            audio_data=audio,
            sample_rate=16000,
            feature_extractor=FakeFeatureExtractor(),
            model=FakeModel(),
            device="cpu",
            offset_seconds=1.5,
        )

        self.assertEqual(embeddings.shape, (2, 4))
        np.testing.assert_array_equal(timestamps, np.array([1.5, 2.5], dtype=np.float32))
