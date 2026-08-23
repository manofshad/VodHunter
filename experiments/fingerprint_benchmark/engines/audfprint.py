from __future__ import annotations

import json
import re
import time
from pathlib import Path

from ..audio import run_command
from ..external import audfprint_python
from ..models import AlignmentOutcome, QueryRecord, SearchResult
from .base import BenchmarkEngine


MATCH_PATTERN = re.compile(
    r"\bat\s+(-?\d+(?:\.\d+)?)\s+s\s+with\s+(\d+)\s+of\s+(\d+)\s+common hashes",
    re.IGNORECASE,
)


class AudfprintEngine(BenchmarkEngine):
    name = "audfprint"

    @property
    def repository(self) -> Path:
        return self.config.external_dir / "audfprint"

    @property
    def index_dir(self) -> Path:
        return self.config.indexes_dir / self.name

    @property
    def database_path(self) -> Path:
        return self.index_dir / "fingerprints.pklz"

    def _script(self) -> Path:
        script = self.repository / "audfprint.py"
        if not script.exists():
            raise FileNotFoundError("audfprint is not installed; run setup --engine audfprint")
        return script

    def index(self, source_audio: Path, *, force: bool = False) -> dict[str, object]:
        self.index_dir.mkdir(parents=True, exist_ok=True)
        metadata_path = self.index_dir / "metadata.json"
        if self.database_path.exists() and not force:
            if metadata_path.exists():
                return json.loads(metadata_path.read_text(encoding="utf-8"))
        started_at = time.perf_counter()
        matplotlib_cache = self.config.artifacts / "cache" / "matplotlib"
        matplotlib_cache.mkdir(parents=True, exist_ok=True)
        run_command(
            [
                str(audfprint_python(self.config)),
                str(self._script()),
                "new",
                "--dbase",
                str(self.database_path),
                "--maxtimebits",
                "20",
                "--ncores",
                "1",
                str(source_audio),
            ],
            cwd=self.repository,
            env={"MPLCONFIGDIR": str(matplotlib_cache)},
        )
        metadata = self.index_metadata(
            self.index_dir,
            started_at,
            model_name="audfprint-landmarks",
            min_aligned_hashes=self.config.audfprint_min_hashes,
        )
        metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return metadata

    def search(self, query: QueryRecord, query_path: Path) -> SearchResult:
        if not self.database_path.exists():
            raise FileNotFoundError("audfprint index does not exist; run index first")
        started_at = time.perf_counter()
        matplotlib_cache = self.config.artifacts / "cache" / "matplotlib"
        matplotlib_cache.mkdir(parents=True, exist_ok=True)
        result = run_command(
            [
                str(audfprint_python(self.config)),
                str(self._script()),
                "match",
                "--dbase",
                str(self.database_path),
                "--maxtimebits",
                "20",
                "--min-count",
                str(self.config.audfprint_min_hashes),
                "--max-matches",
                "5",
                "--ncores",
                "1",
                str(query_path),
            ],
            cwd=self.repository,
            env={"MPLCONFIGDIR": str(matplotlib_cache)},
        )
        matches = [
            (float(match.group(1)), int(match.group(2)), int(match.group(3)))
            for match in MATCH_PATTERN.finditer(result.stdout)
        ]
        if matches:
            offset, aligned, common = matches[0]
            confidence = aligned / max(1, common)
            accepted = aligned >= self.config.audfprint_min_hashes
            outcome = AlignmentOutcome(
                found=accepted,
                start_seconds=offset if accepted else None,
                confidence=confidence,
                reason=(
                    f"Accepted with {aligned} aligned of {common} common hashes"
                    if accepted
                    else f"Rejected with {aligned} aligned of {common} common hashes"
                ),
                raw_start_seconds=offset,
                raw_score=float(aligned),
                diagnostics={
                    "aligned_hashes": aligned,
                    "common_hashes": common,
                    "candidates": [
                        {"start_seconds": item[0], "aligned_hashes": item[1], "common_hashes": item[2]}
                        for item in matches
                    ],
                },
            )
        else:
            outcome = AlignmentOutcome(
                False,
                None,
                None,
                "No audfprint candidate met the landmark threshold",
                None,
                None,
                {"stdout_tail": result.stdout[-1000:]},
            )
        return self.timed_result(query, started_at, outcome)
