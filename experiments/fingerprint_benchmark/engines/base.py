from __future__ import annotations

import time
from abc import ABC, abstractmethod
from pathlib import Path

from ..config import BenchmarkConfig
from ..models import AlignmentOutcome, QueryRecord, SearchResult


class BenchmarkEngine(ABC):
    name: str

    def __init__(self, config: BenchmarkConfig):
        self.config = config

    @abstractmethod
    def index(self, source_audio: Path, *, force: bool = False) -> dict[str, object]:
        raise NotImplementedError

    @abstractmethod
    def search(self, query: QueryRecord, query_path: Path) -> SearchResult:
        raise NotImplementedError

    def timed_result(
        self,
        query: QueryRecord,
        started_at: float,
        outcome: AlignmentOutcome,
    ) -> SearchResult:
        error = None
        candidate = outcome.start_seconds if outcome.found else outcome.raw_start_seconds
        if query.expected_start_seconds is not None and candidate is not None:
            error = abs(float(candidate) - float(query.expected_start_seconds))
        return SearchResult(
            engine=self.name,
            query_id=query.query_id,
            query_kind=query.kind,
            expected_match=query.expected_match,
            expected_start_seconds=query.expected_start_seconds,
            found=outcome.found,
            predicted_start_seconds=outcome.start_seconds,
            confidence=outcome.confidence,
            search_duration_ms=round((time.perf_counter() - started_at) * 1000),
            reason=outcome.reason,
            timestamp_error_seconds=error,
            raw_candidate_start_seconds=outcome.raw_start_seconds,
            raw_candidate_score=outcome.raw_score,
            diagnostics=outcome.diagnostics,
        )

    def index_metadata(self, index_dir: Path, started_at: float, **extra: object) -> dict[str, object]:
        size = sum(path.stat().st_size for path in index_dir.rglob("*") if path.is_file())
        return {
            "engine": self.name,
            "index_duration_seconds": round(time.perf_counter() - started_at, 3),
            "index_size_bytes": size,
            **extra,
        }
