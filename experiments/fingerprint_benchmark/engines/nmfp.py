from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Iterable

import numpy as np

from ..alignment import temporal_vector_alignment
from ..audio import extract_audio_clip, probe_duration, run_command
from ..cut_detection import CutDetectionSettings, CutSearchResult, cut_aware_vector_alignment
from ..external import nmfp_python
from ..models import QueryRecord, SearchResult
from .base import BenchmarkEngine


class NMFPEngine(BenchmarkEngine):
    """Adapter for the pretrained NAFP-derived NMFP-triplet model."""

    name = "nmfp_triplet"

    @property
    def index_dir(self) -> Path:
        return self.config.indexes_dir / self.name

    @property
    def embeddings_path(self) -> Path:
        return self.index_dir / "embeddings.npy"

    @property
    def timestamps_path(self) -> Path:
        return self.index_dir / "timestamps.npy"

    @property
    def query_embeddings_dir(self) -> Path:
        return self.index_dir / "query_embeddings"

    @property
    def repository(self) -> Path:
        return self.config.external_dir / "neural-music-fp"

    def _model_config(self) -> Path:
        candidates = list((self.repository / "pretrained_models").rglob("config.yaml"))
        triplet = [path for path in candidates if "triplet" in str(path).lower()]
        if not triplet:
            raise FileNotFoundError("NMFP-triplet weights are not installed; run setup --engine nmfp_triplet")
        return sorted(triplet)[0]

    def _extract(self, input_dir: Path, output_dir: Path) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        python = nmfp_python(self.config)
        extraction_script = self.repository / "extraction.py"
        if not extraction_script.exists():
            raise FileNotFoundError("NMFP repository is not installed; run setup --engine nmfp_triplet")
        result = run_command(
            [
                str(python),
                str(extraction_script),
                str(input_dir),
                str(self._model_config()),
                str(output_dir),
                "--hop-duration",
                str(self.config.nmfp_hop_seconds),
                "--batch-size",
                "256",
                "--workers",
                "1",
                "--queue",
                "2",
            ],
            cwd=self.repository,
            accepted_return_codes={0, -15, 143},
        )
        if not list(output_dir.rglob("*.npy")):
            detail = (result.stderr or result.stdout).strip()
            raise RuntimeError(f"NMFP extraction produced no embeddings\n{detail}")

    @staticmethod
    def _link_or_copy(source: Path, target: Path) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.unlink(missing_ok=True)
        try:
            os.link(source, target)
        except OSError:
            shutil.copy2(source, target)

    def _make_sentinel(self, source: Path, target: Path) -> None:
        extract_audio_clip(source, target, start_seconds=0.0, duration_seconds=2.0, sample_rate=16000)

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
        chunk_embeddings_dir = self.index_dir / "chunks"
        if force and chunk_embeddings_dir.exists():
            shutil.rmtree(chunk_embeddings_dir)
        chunk_embeddings_dir.mkdir(parents=True, exist_ok=True)
        duration = probe_duration(source_audio)
        chunk_seconds = self.config.nmfp_ingest_chunk_seconds
        step_seconds = chunk_seconds - self.config.nmfp_hop_seconds
        chunk_specs: list[tuple[str, float, float]] = []
        start = 0.0
        chunk_index = 0
        while start + 1.0 <= duration:
            length = min(chunk_seconds, duration - start)
            name = f"chunk_{chunk_index:05d}_{round(start * 1000):012d}"
            chunk_specs.append((name, start, length))
            if start + length >= duration:
                break
            start += step_seconds
            chunk_index += 1
        with tempfile.TemporaryDirectory(prefix="vodhunter-nmfp-index-") as temp:
            temp_root = Path(temp)
            inputs = temp_root / "inputs"
            inputs.mkdir()
            missing = [spec for spec in chunk_specs if not (chunk_embeddings_dir / f"{spec[0]}.npy").exists()]
            for name, chunk_start, length in missing:
                print(f"[nmfp] preparing {name} at {chunk_start:.1f}s for {length:.1f}s")
                extract_audio_clip(
                    source_audio,
                    inputs / f"{name}.wav",
                    start_seconds=chunk_start,
                    duration_seconds=length,
                    sample_rate=8000,
                )
            if missing:
                print(f"[nmfp] extracting fingerprints for {len(missing)} source chunks")
                self._make_sentinel(source_audio, inputs / "__sentinel.wav")
                self._extract(inputs, chunk_embeddings_dir)
            else:
                print(f"[nmfp] reusing all {len(chunk_specs)} completed source chunks")
        embedding_parts: list[np.ndarray] = []
        timestamp_parts: list[np.ndarray] = []
        for name, chunk_start, _ in chunk_specs:
            chunk_path = chunk_embeddings_dir / f"{name}.npy"
            if not chunk_path.exists():
                raise RuntimeError(f"NMFP did not emit {chunk_path.name}")
            part = np.load(chunk_path).astype(np.float32)
            embedding_parts.append(part)
            timestamp_parts.append(
                chunk_start + np.arange(len(part), dtype=np.float32) * self.config.nmfp_hop_seconds
            )
        embeddings = np.concatenate(embedding_parts, axis=0)
        timestamps = np.concatenate(timestamp_parts, axis=0).astype(np.float32)
        np.save(self.embeddings_path, embeddings)
        np.save(self.timestamps_path, timestamps)
        metadata = self.index_metadata(
            self.index_dir,
            started_at,
            model_name="nmfp-triplet",
            model_family="NAFP-derived neural fingerprint",
            fingerprint_count=len(timestamps),
            embedding_dimensions=int(embeddings.shape[1]) if embeddings.ndim == 2 else 0,
            hop_seconds=self.config.nmfp_hop_seconds,
            ingest_chunk_seconds=chunk_seconds,
            ingest_chunk_overlap_seconds=self.config.nmfp_hop_seconds,
            resumable_chunk_count=len(chunk_specs),
        )
        metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return metadata

    def prepare_queries(self, records: Iterable[QueryRecord], manifest_path: Path, *, force: bool = False) -> None:
        selected = [record for record in records if force or not (self.query_embeddings_dir / f"{record.query_id}.npy").exists()]
        if not selected:
            return
        self.query_embeddings_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="vodhunter-nmfp-queries-") as temp:
            temp_root = Path(temp)
            inputs = temp_root / "inputs"
            outputs = temp_root / "outputs"
            inputs.mkdir()
            for record in selected:
                self._link_or_copy(record.resolved_path(manifest_path), inputs / f"{record.query_id}.wav")
            first = selected[0].resolved_path(manifest_path)
            self._make_sentinel(first, inputs / "__sentinel.wav")
            self._extract(inputs, outputs)
            for record in selected:
                extracted = next(iter(outputs.rglob(f"{record.query_id}.npy")), None)
                if extracted is None:
                    raise RuntimeError(f"NMFP did not emit embeddings for {record.query_id}")
                shutil.copy2(extracted, self.query_embeddings_dir / f"{record.query_id}.npy")

    def search(self, query: QueryRecord, query_path: Path) -> SearchResult:
        if not self.embeddings_path.exists():
            raise FileNotFoundError("NMFP index does not exist; run index first")
        query_embeddings_path = self.query_embeddings_dir / f"{query.query_id}.npy"
        if not query_embeddings_path.exists():
            self.prepare_queries([query], self.config.manifest_path)
        started_at = time.perf_counter()
        query_embeddings = np.load(query_embeddings_path).astype(np.float32)
        query_timestamps = np.arange(len(query_embeddings), dtype=np.float32) * self.config.nmfp_hop_seconds
        outcome = temporal_vector_alignment(
            query_embeddings,
            query_timestamps,
            np.load(self.embeddings_path, mmap_mode="r"),
            np.load(self.timestamps_path, mmap_mode="r"),
            top_k=self.config.top_k,
            min_votes=self.config.nmfp_min_votes,
            min_vote_ratio=self.config.nmfp_min_vote_ratio,
            offset_bin_seconds=self.config.nmfp_hop_seconds,
        )
        outcome.diagnostics["model_name"] = "nmfp-triplet"
        return self.timed_result(query, started_at, outcome)

    def search_cuts(
        self,
        query: QueryRecord,
        query_path: Path,
        *,
        settings: CutDetectionSettings,
    ) -> CutSearchResult:
        """Map independently edited portions of one query onto the VOD timeline."""

        if not self.embeddings_path.exists() or not self.timestamps_path.exists():
            raise FileNotFoundError("NMFP index does not exist; run index first")
        query_embeddings_path = self.query_embeddings_dir / f"{query.query_id}.npy"
        if not query_embeddings_path.exists():
            raise FileNotFoundError(
                f"NMFP query embeddings do not exist for {query.query_id}; "
                "prepare the selected manifest before cut-aware search"
            )
        query_embeddings = np.load(query_embeddings_path).astype(np.float32)
        query_timestamps = np.arange(len(query_embeddings), dtype=np.float32) * settings.hop_seconds
        duration = query.duration_seconds or probe_duration(query_path)
        result = cut_aware_vector_alignment(
            query.query_id,
            query_embeddings,
            query_timestamps,
            np.load(self.embeddings_path, mmap_mode="r"),
            np.load(self.timestamps_path, mmap_mode="r"),
            query_duration_seconds=duration,
            settings=settings,
        )
        result.diagnostics["model_name"] = "nmfp-triplet"
        return result
