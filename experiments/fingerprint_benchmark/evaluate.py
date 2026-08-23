from __future__ import annotations

import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Iterable

from .models import SearchResult


def load_results(path: Path) -> list[SearchResult]:
    if not path.exists():
        raise FileNotFoundError(path)
    results: list[SearchResult] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                results.append(SearchResult.from_dict(json.loads(line)))
    return results


def summarize(results: Iterable[SearchResult]) -> dict[str, dict[str, float | int | None]]:
    by_engine: dict[str, list[SearchResult]] = defaultdict(list)
    for result in results:
        if "error" not in result.diagnostics:
            by_engine[result.engine].append(result)

    summary: dict[str, dict[str, float | int | None]] = {}
    for engine, rows in sorted(by_engine.items()):
        positives = [row for row in rows if row.expected_match]
        negatives = [row for row in rows if not row.expected_match]
        within_2 = sum(
            row.found and row.timestamp_error_seconds is not None and row.timestamp_error_seconds <= 2.0
            for row in positives
        )
        within_5 = sum(
            row.found and row.timestamp_error_seconds is not None and row.timestamp_error_seconds <= 5.0
            for row in positives
        )
        rejected_negatives = sum(not row.found for row in negatives)
        latencies = [row.search_duration_ms for row in rows]
        errors = [row.timestamp_error_seconds for row in positives if row.found and row.timestamp_error_seconds is not None]
        summary[engine] = {
            "queries": len(rows),
            "positive_queries": len(positives),
            "negative_queries": len(negatives),
            "positive_found": sum(row.found for row in positives),
            "within_2_seconds": within_2,
            "within_2_rate": within_2 / len(positives) if positives else None,
            "within_5_seconds": within_5,
            "within_5_rate": within_5 / len(positives) if positives else None,
            "negative_rejections": rejected_negatives,
            "negative_rejection_rate": rejected_negatives / len(negatives) if negatives else None,
            "mean_found_error_seconds": statistics.fmean(errors) if errors else None,
            "median_latency_ms": statistics.median(latencies) if latencies else None,
            "mean_latency_ms": statistics.fmean(latencies) if latencies else None,
        }
    return summary


def write_reports(results: list[SearchResult], reports_dir: Path) -> dict[str, dict[str, float | int | None]]:
    reports_dir.mkdir(parents=True, exist_ok=True)
    summary = summarize(results)
    (reports_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    with (reports_dir / "results.csv").open("w", newline="", encoding="utf-8") as handle:
        fieldnames = [
            "engine",
            "query_id",
            "query_kind",
            "expected_match",
            "expected_start_seconds",
            "found",
            "predicted_start_seconds",
            "timestamp_error_seconds",
            "confidence",
            "raw_candidate_start_seconds",
            "raw_candidate_score",
            "search_duration_ms",
            "reason",
            "diagnostics",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            row = result.to_dict()
            row["diagnostics"] = json.dumps(row["diagnostics"], sort_keys=True)
            writer.writerow(row)

    def rate(value: float | int | None) -> str:
        return "—" if value is None else f"{float(value) * 100:.1f}%"

    lines = [
        "# Fingerprint benchmark report",
        "",
        "| Engine | Positives | Found | ±2s | ±5s | Negatives rejected | Median latency |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for engine, row in summary.items():
        lines.append(
            f"| {engine} | {row['positive_queries']} | {row['positive_found']} | "
            f"{rate(row['within_2_rate'])} | {rate(row['within_5_rate'])} | "
            f"{rate(row['negative_rejection_rate'])} | {row['median_latency_ms']} ms |"
        )
    lines.extend(
        [
            "",
            "A result counts as correct only when the engine accepts it and its timestamp is within the tolerance.",
            "Raw nearest candidates remain available in `results.csv` even when an engine rejects them.",
            "With only three negative clips, rejection rates are diagnostic rather than statistically conclusive.",
            "",
        ]
    )
    (reports_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")
    return summary
