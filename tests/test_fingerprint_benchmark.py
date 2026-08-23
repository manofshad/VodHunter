import json
import wave
from pathlib import Path

import numpy as np

from experiments.fingerprint_benchmark.alignment import temporal_vector_alignment
from experiments.fingerprint_benchmark.audio import generate_clean_queries
from experiments.fingerprint_benchmark.config import BenchmarkConfig
from experiments.fingerprint_benchmark.engines.audfprint import MATCH_PATTERN
from experiments.fingerprint_benchmark.engines.ast import ASTEngine
from experiments.fingerprint_benchmark.engines.nmfp import NMFPEngine
from experiments.fingerprint_benchmark.evaluate import summarize
from experiments.fingerprint_benchmark.manifest import load_manifest, write_manifest
from experiments.fingerprint_benchmark.models import QueryRecord, SearchResult


def _write_tone(path: Path, duration: float, sample_rate: int = 16000) -> None:
    samples = np.arange(round(duration * sample_rate))
    audio = (0.2 * np.sin(2 * np.pi * 440 * samples / sample_rate) * 32767).astype("<i2")
    path.parent.mkdir(parents=True, exist_ok=True)
    with wave.open(str(path), "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(sample_rate)
        handle.writeframes(audio.tobytes())


def test_manifest_round_trip(tmp_path: Path) -> None:
    query = tmp_path / "queries" / "q.wav"
    _write_tone(query, 1.0)
    manifest = tmp_path / "manifest.jsonl"
    record = QueryRecord("clean_01", "clean", "queries/q.wav", True, 12.5, duration_seconds=1.0)
    write_manifest(manifest, [record])
    assert load_manifest(manifest) == [record]


def test_temporal_alignment_localizes_consistent_sequence() -> None:
    query_embeddings = np.eye(3, dtype=np.float32)
    database_embeddings = np.eye(3, dtype=np.float32)
    outcome = temporal_vector_alignment(
        query_embeddings,
        np.array([0.0, 1.0, 2.0], dtype=np.float32),
        database_embeddings,
        np.array([10.0, 11.0, 12.0], dtype=np.float32),
        top_k=1,
        min_votes=3,
        min_vote_ratio=1.0,
        offset_bin_seconds=1.0,
    )
    assert outcome.found
    assert outcome.start_seconds == 10.0
    assert outcome.diagnostics["best_votes"] == 3


def test_generate_clean_queries_is_deterministic(tmp_path: Path) -> None:
    config = BenchmarkConfig(
        root=tmp_path,
        clean_clip_count=2,
        clean_clip_seconds=2.0,
        clean_edge_margin_seconds=1.0,
    )
    config.ensure_directories()
    _write_tone(config.source_audio, 12.0)
    first = generate_clean_queries(config)
    second = generate_clean_queries(config)
    assert [record.expected_start_seconds for record in first] == [3.0, 7.0]
    assert first == second
    assert len(load_manifest(config.manifest_path)) == 2


def test_audfprint_match_parser() -> None:
    line = "Matched query.wav 25.0 sec 500 raw hashes as source.wav at 3725.2 s with 18 of 24 common hashes at rank 0"
    match = MATCH_PATTERN.search(line)
    assert match is not None
    assert (float(match.group(1)), int(match.group(2)), int(match.group(3))) == (3725.2, 18, 24)


def test_summary_scores_acceptance_and_timestamp_tolerance() -> None:
    rows = [
        SearchResult("ast", "p1", "clean", True, 10.0, True, 11.0, 0.8, 10, "ok", 1.0),
        SearchResult("ast", "p2", "tiktok", True, 20.0, False, None, 0.1, 12, "rejected", 30.0),
        SearchResult("ast", "n1", "no_match", False, None, False, None, None, 8, "rejected"),
    ]
    result = summarize(rows)["ast"]
    assert result["within_2_rate"] == 0.5
    assert result["within_5_rate"] == 0.5
    assert result["negative_rejection_rate"] == 1.0


def test_ast_index_is_chunked_and_resumable(tmp_path: Path, monkeypatch) -> None:
    config = BenchmarkConfig(root=tmp_path, ast_ingest_chunk_seconds=5.0)
    config.ensure_directories()
    _write_tone(config.source_audio, 12.0)

    class FakeEmbedder:
        def embed(self, audio_path: str, offset_seconds: float = 0.0):
            with wave.open(audio_path, "rb") as handle:
                duration = handle.getnframes() / handle.getframerate()
            count = int(np.ceil(duration))
            timestamps = offset_seconds + np.arange(count, dtype=np.float32)
            embeddings = np.column_stack((timestamps, np.ones(count, dtype=np.float32)))
            return embeddings, timestamps

    engine = ASTEngine(config)
    monkeypatch.setattr(engine, "_get_embedder", lambda: FakeEmbedder())
    metadata = engine.index(config.source_audio)
    assert metadata["resumable_chunk_count"] == 3
    assert len(np.load(engine.timestamps_path)) == 12
    assert len(list((engine.index_dir / "chunks").glob("*.npz"))) == 3


def test_nmfp_index_uses_overlap_for_continuous_timeline(tmp_path: Path, monkeypatch) -> None:
    config = BenchmarkConfig(
        root=tmp_path,
        nmfp_ingest_chunk_seconds=5.0,
        nmfp_hop_seconds=0.5,
    )
    config.ensure_directories()
    _write_tone(config.source_audio, 12.0)
    engine = NMFPEngine(config)

    def fake_extract(input_dir: Path, output_dir: Path) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        for audio_path in input_dir.glob("chunk_*.wav"):
            with wave.open(str(audio_path), "rb") as handle:
                duration = handle.getnframes() / handle.getframerate()
            count = int(np.floor((duration - 1.0) / 0.5)) + 1
            np.save(output_dir / f"{audio_path.stem}.npy", np.ones((count, 4), dtype=np.float32))

    monkeypatch.setattr(engine, "_extract", fake_extract)
    metadata = engine.index(config.source_audio)
    timestamps = np.load(engine.timestamps_path)
    assert metadata["resumable_chunk_count"] == 3
    np.testing.assert_allclose(timestamps, np.arange(0.0, 11.5, 0.5, dtype=np.float32))
