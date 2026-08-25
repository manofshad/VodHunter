from __future__ import annotations

import json
import shutil
import tempfile
import time
from pathlib import Path

import numpy as np

from ..alignment import temporal_vector_alignment
from ..audio import extract_audio_clip, normalize_audio, probe_duration
from ..models import QueryRecord, SearchResult
from .ast_embedder import ASTEmbedder, DEFAULT_AST_MODEL_NAME
from .base import BenchmarkEngine


class ASTEngine(BenchmarkEngine):
    name = "ast"

    def __init__(self, config):
        super().__init__(config)
        self._embedder: ASTEmbedder | None = None

    def _get_embedder(self) -> ASTEmbedder:
        if self._embedder is None:
            self._embedder = ASTEmbedder(model_name=DEFAULT_AST_MODEL_NAME)
        return self._embedder

    @property
    def index_dir(self) -> Path:
        return self.config.indexes_dir / self.name

    @property
    def embeddings_path(self) -> Path:
        return self.index_dir / "embeddings.npy"

    @property
    def timestamps_path(self) -> Path:
        return self.index_dir / "timestamps.npy"

    def index(self, source_audio: Path, *, force: bool = False) -> dict[str, object]:
        self.index_dir.mkdir(parents=True, exist_ok=True)
        metadata_path = self.index_dir / "metadata.json"
        if force:
            self.embeddings_path.unlink(missing_ok=True)
            self.timestamps_path.unlink(missing_ok=True)
            metadata_path.unlink(missing_ok=True)
        if self.embeddings_path.exists() and self.timestamps_path.exists() and not force:
            if metadata_path.exists():
                return json.loads(metadata_path.read_text(encoding="utf-8"))
        started_at = time.perf_counter()
        chunk_dir = self.index_dir / "chunks"
        if force and chunk_dir.exists():
            shutil.rmtree(chunk_dir)
        chunk_dir.mkdir(parents=True, exist_ok=True)
        duration = probe_duration(source_audio)
        chunk_seconds = self.config.ast_ingest_chunk_seconds
        chunk_paths: list[Path] = []
        start = 0.0
        chunk_index = 0
        while start < duration:
            length = min(chunk_seconds, duration - start)
            chunk_output = chunk_dir / f"chunk_{chunk_index:05d}.npz"
            chunk_paths.append(chunk_output)
            if not chunk_output.exists():
                print(
                    f"[ast] indexing chunk {chunk_index + 1} "
                    f"at {start:.1f}s for {length:.1f}s"
                )
                with tempfile.TemporaryDirectory(prefix="vodhunter-ast-index-") as temp_dir:
                    audio_chunk = Path(temp_dir) / "chunk.wav"
                    extract_audio_clip(
                        source_audio,
                        audio_chunk,
                        start_seconds=start,
                        duration_seconds=length,
                        sample_rate=16000,
                    )
                    chunk_embeddings, chunk_timestamps = self._get_embedder().embed(
                        str(audio_chunk), offset_seconds=start
                    )
                    np.savez(
                        chunk_output,
                        embeddings=chunk_embeddings.astype(np.float32),
                        timestamps=chunk_timestamps.astype(np.float32),
                    )
            else:
                print(f"[ast] reusing completed chunk {chunk_index + 1} at {start:.1f}s")
            start += length
            chunk_index += 1
        embedding_parts: list[np.ndarray] = []
        timestamp_parts: list[np.ndarray] = []
        for chunk_path in chunk_paths:
            with np.load(chunk_path) as chunk:
                embedding_parts.append(chunk["embeddings"])
                timestamp_parts.append(chunk["timestamps"])
        embeddings = np.concatenate(embedding_parts, axis=0)
        timestamps = np.concatenate(timestamp_parts, axis=0)
        np.save(self.embeddings_path, embeddings.astype(np.float32))
        np.save(self.timestamps_path, timestamps.astype(np.float32))
        metadata = self.index_metadata(
            self.index_dir,
            started_at,
            model_name=DEFAULT_AST_MODEL_NAME,
            fingerprint_count=len(timestamps),
            embedding_dimensions=int(embeddings.shape[1]) if embeddings.ndim == 2 else 0,
            hop_seconds=1.0,
            ingest_chunk_seconds=chunk_seconds,
            resumable_chunk_count=len(chunk_paths),
        )
        metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return metadata

    def search(self, query: QueryRecord, query_path: Path) -> SearchResult:
        if not self.embeddings_path.exists():
            raise FileNotFoundError("AST index does not exist; run index first")
        started_at = time.perf_counter()
        with tempfile.TemporaryDirectory(prefix="vodhunter-ast-query-") as temp_dir:
            normalized = Path(temp_dir) / "query.wav"
            normalize_audio(query_path, normalized, sample_rate=16000)
            query_embeddings, query_timestamps = self._get_embedder().embed(str(normalized))
        outcome = temporal_vector_alignment(
            query_embeddings,
            query_timestamps,
            np.load(self.embeddings_path, mmap_mode="r"),
            np.load(self.timestamps_path, mmap_mode="r"),
            top_k=self.config.top_k,
            min_votes=self.config.ast_min_votes,
            min_vote_ratio=self.config.ast_min_vote_ratio,
            offset_bin_seconds=1.0,
        )
        outcome.diagnostics["model_name"] = DEFAULT_AST_MODEL_NAME
        return self.timed_result(query, started_at, outcome)
