from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal


QueryKind = Literal["clean", "tiktok", "no_match"]


@dataclass(frozen=True)
class QueryRecord:
    query_id: str
    kind: QueryKind
    path: str
    expected_match: bool
    expected_start_seconds: float | None = None
    source_url: str | None = None
    duration_seconds: float | None = None
    notes: str | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "QueryRecord":
        return cls(**value)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def resolved_path(self, manifest_path: Path) -> Path:
        path = Path(self.path)
        return path if path.is_absolute() else manifest_path.parent / path


@dataclass
class SearchResult:
    engine: str
    query_id: str
    query_kind: QueryKind
    expected_match: bool
    expected_start_seconds: float | None
    found: bool
    predicted_start_seconds: float | None
    confidence: float | None
    search_duration_ms: int
    reason: str
    timestamp_error_seconds: float | None = None
    raw_candidate_start_seconds: float | None = None
    raw_candidate_score: float | None = None
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SearchResult":
        return cls(**value)


@dataclass(frozen=True)
class AlignmentOutcome:
    found: bool
    start_seconds: float | None
    confidence: float | None
    reason: str
    raw_start_seconds: float | None
    raw_score: float | None
    diagnostics: dict[str, Any]
