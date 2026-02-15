import numpy as np
import soundfile as sf
import torch
from transformers import ASTFeatureExtractor, ASTModel
from typing import Tuple, Optional, List


class Embedder:
    def __init__(self):
        self.device = self._pick_device()

        print("⏳ Loading AST model...")
        self.feature_extractor = ASTFeatureExtractor.from_pretrained(
            "MIT/ast-finetuned-audioset-10-10-0.4593"
        )
        self.model = ASTModel.from_pretrained(
            "MIT/ast-finetuned-audioset-10-10-0.4593"
        ).to(self.device)
        self.model.eval()

    def _pick_device(self) -> torch.device:
        if torch.backends.mps.is_available():
            print("🚀 Using Apple Metal (GPU)")
            return torch.device("mps")
        if torch.cuda.is_available():
            print("🚀 Using CUDA (GPU)")
            return torch.device("cuda")
        print("🐢 Using CPU")
        return torch.device("cpu")

    def embed(
        self,
        audio_path: str,
        offset_seconds: float = 0.0,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Parameters:
            audio_path: path to WAV file (16kHz mono)
            offset_seconds: absolute offset to add to timestamps

        Returns:
            embeddings: (N, D)
            timestamps: (N,)
        """
        audio_data, sr = sf.read(audio_path)

        if sr != 16000:
            raise ValueError(f"Expected 16kHz audio, got {sr}")

        one_second = 16000
        batch_size = 8

        total_samples = len(audio_data)
        num_chunks = int(np.ceil(total_samples / one_second))

        embeddings: List[np.ndarray] = []
        timestamps: List[float] = []

        for i in range(0, num_chunks, batch_size):
            batch_audio = []
            batch_times = []

            for j in range(i, min(i + batch_size, num_chunks)):
                start = j * one_second
                end = start + one_second
                chunk = audio_data[start:end]

                if len(chunk) < one_second:
                    chunk = np.pad(chunk, (0, one_second - len(chunk)))

                batch_audio.append(chunk)
                batch_times.append((start / 16000.0) + offset_seconds)

            inputs = self.feature_extractor(
                batch_audio,
                sampling_rate=16000,
                return_tensors="pt"
            )
            inputs = {k: v.to(self.device) for k, v in inputs.items()}

            with torch.no_grad():
                outputs = self.model(**inputs)

            batch_embs = outputs.pooler_output.cpu().numpy()
            embeddings.append(batch_embs)
            timestamps.extend(batch_times)

        if not embeddings:
            return np.zeros((0,)), np.zeros((0,))

        embeddings_np = np.concatenate(embeddings, axis=0)
        timestamps_np = np.array(timestamps, dtype=np.float32)

        return embeddings_np, timestamps_np
