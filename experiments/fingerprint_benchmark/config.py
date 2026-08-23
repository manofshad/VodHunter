from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parent


@dataclass(frozen=True)
class BenchmarkConfig:
    root: Path = PACKAGE_ROOT
    vod_url: str = "https://www.twitch.tv/videos/2848966623"
    vod_id: str = "2848966623"
    streamer: str = "jasontheween"
    clean_clip_count: int = 10
    clean_clip_seconds: float = 25.0
    clean_edge_margin_seconds: float = 60.0
    top_k: int = 10
    ast_min_votes: int = 3
    ast_min_vote_ratio: float = 0.08
    nmfp_min_votes: int = 5
    nmfp_min_vote_ratio: float = 0.10
    audfprint_min_hashes: int = 5
    nmfp_hop_seconds: float = 0.5
    ast_ingest_chunk_seconds: float = 300.0
    nmfp_ingest_chunk_seconds: float = 600.0

    @property
    def artifacts(self) -> Path:
        return self.root / "artifacts"

    @property
    def source_dir(self) -> Path:
        return self.artifacts / "source"

    @property
    def source_audio(self) -> Path:
        return self.source_dir / f"twitch_{self.vod_id}_16k_mono.wav"

    @property
    def source_metadata(self) -> Path:
        return self.source_dir / "metadata.json"

    @property
    def queries_dir(self) -> Path:
        return self.artifacts / "queries"

    @property
    def indexes_dir(self) -> Path:
        return self.artifacts / "indexes"

    @property
    def results_dir(self) -> Path:
        return self.artifacts / "results"

    @property
    def reports_dir(self) -> Path:
        return self.artifacts / "reports"

    @property
    def external_dir(self) -> Path:
        return self.artifacts / "external"

    @property
    def manifest_path(self) -> Path:
        return self.artifacts / "manifest.jsonl"

    def ensure_directories(self) -> None:
        for path in (
            self.source_dir,
            self.queries_dir / "clean",
            self.queries_dir / "tiktok",
            self.queries_dir / "no_match",
            self.indexes_dir,
            self.results_dir,
            self.reports_dir,
            self.external_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
