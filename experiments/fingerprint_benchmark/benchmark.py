from __future__ import annotations

import argparse
import json
import traceback
from pathlib import Path
from typing import Iterable

from .audio import generate_clean_queries, import_query, prepare_vod
from .config import BenchmarkConfig
from .engines import ASTEngine, AudfprintEngine, NMFPEngine
from .engines.base import BenchmarkEngine
from .evaluate import load_results, write_reports
from .external import setup_audfprint, setup_nmfp
from .manifest import load_manifest
from .models import QueryRecord, SearchResult
from .preflight import run_preflight


ENGINE_NAMES = ("ast", "nmfp_triplet", "audfprint")


def build_engine(name: str, config: BenchmarkConfig) -> BenchmarkEngine:
    if name == "ast":
        return ASTEngine(config)
    if name == "nmfp_triplet":
        return NMFPEngine(config)
    if name == "audfprint":
        return AudfprintEngine(config)
    raise ValueError(f"Unknown engine: {name}")


def selected_engines(name: str) -> tuple[str, ...]:
    return ENGINE_NAMES if name == "all" else (name,)


def write_results(path: Path, results: Iterable[SearchResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8") as handle:
        for result in results:
            handle.write(json.dumps(result.to_dict(), sort_keys=True) + "\n")
    temp.replace(path)


def run_search(
    config: BenchmarkConfig,
    engine_name: str,
    *,
    kind: str | None,
    continue_on_error: bool,
) -> list[SearchResult]:
    records = load_manifest(config.manifest_path)
    if kind:
        records = [record for record in records if record.kind == kind]
    if not records:
        raise RuntimeError("No benchmark queries matched the selection")
    results: list[SearchResult] = []
    for name in selected_engines(engine_name):
        engine = build_engine(name, config)
        prepare_queries = getattr(engine, "prepare_queries", None)
        if callable(prepare_queries):
            prepare_queries(records, config.manifest_path)
        for record in records:
            try:
                result = engine.search(record, record.resolved_path(config.manifest_path))
            except Exception as exc:
                if not continue_on_error:
                    raise
                result = SearchResult(
                    engine=name,
                    query_id=record.query_id,
                    query_kind=record.kind,
                    expected_match=record.expected_match,
                    expected_start_seconds=record.expected_start_seconds,
                    found=False,
                    predicted_start_seconds=None,
                    confidence=None,
                    search_duration_ms=0,
                    reason=f"Engine error: {exc}",
                    diagnostics={"error": type(exc).__name__, "traceback": traceback.format_exc()},
                )
            print(
                f"{name:14s} {record.query_id:20s} found={str(result.found):5s} "
                f"timestamp={result.predicted_start_seconds} raw={result.raw_candidate_start_seconds} "
                f"error={result.timestamp_error_seconds}"
            )
            results.append(result)
    write_results(config.results_dir / "latest.jsonl", results)
    return results


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Three-engine benchmark for Twitch VOD 2848966623")
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser("prepare-vod", help="Download and normalize the source VOD audio")
    prepare.add_argument("--force", action="store_true")

    clean = subparsers.add_parser("generate-clean", help="Create ten deterministic clean queries")
    clean.add_argument("--force", action="store_true")

    setup = subparsers.add_parser("setup", help="Fetch pinned external engine code and model weights")
    setup.add_argument("--engine", choices=("all", "nmfp_triplet", "audfprint"), default="all")
    setup.add_argument("--skip-nmfp-weights", action="store_true")

    index = subparsers.add_parser("index", help="Index the source VOD")
    index.add_argument("--engine", choices=("all",) + ENGINE_NAMES, default="all")
    index.add_argument("--force", action="store_true")

    search = subparsers.add_parser("search", help="Run manifest queries")
    search.add_argument("--engine", choices=("all",) + ENGINE_NAMES, default="all")
    search.add_argument("--kind", choices=("clean", "tiktok", "no_match"))
    search.add_argument("--continue-on-error", action="store_true")

    evaluate = subparsers.add_parser("evaluate", help="Generate CSV, JSON, and Markdown reports")
    evaluate.add_argument("--results", type=Path)

    add_query = subparsers.add_parser("import-query", help="Import one TikTok or no-match query")
    add_query.add_argument("--source", required=True, help="Local media path or URL")
    add_query.add_argument("--kind", choices=("tiktok", "no_match"), required=True)
    add_query.add_argument("--id", required=True, dest="query_id")
    add_query.add_argument("--expected-start", type=float)
    add_query.add_argument("--notes")

    preflight = subparsers.add_parser("preflight", help="Validate a PC environment without downloading or indexing")
    preflight.add_argument("--engine", choices=("all",) + ENGINE_NAMES, default="all")

    subparsers.add_parser("status", help="Show available source, queries, indexes, and results")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = create_parser().parse_args(argv)
    config = BenchmarkConfig()
    config.ensure_directories()
    if args.command == "prepare-vod":
        print(json.dumps(prepare_vod(config, force=args.force), indent=2, sort_keys=True))
    elif args.command == "generate-clean":
        records = generate_clean_queries(config, force=args.force)
        print(f"Generated or verified {len(records)} clean queries in {config.queries_dir / 'clean'}")
    elif args.command == "setup":
        setup_results = []
        if args.engine in {"all", "audfprint"}:
            setup_results.append(setup_audfprint(config))
        if args.engine in {"all", "nmfp_triplet"}:
            setup_results.append(setup_nmfp(config, download_weights=not args.skip_nmfp_weights))
        print(json.dumps(setup_results, indent=2, sort_keys=True))
    elif args.command == "index":
        if not config.source_audio.exists():
            raise FileNotFoundError("Run prepare-vod before indexing")
        summaries = [
            build_engine(name, config).index(config.source_audio, force=args.force)
            for name in selected_engines(args.engine)
        ]
        print(json.dumps(summaries, indent=2, sort_keys=True))
    elif args.command == "search":
        run_search(config, args.engine, kind=args.kind, continue_on_error=args.continue_on_error)
    elif args.command == "evaluate":
        results_path = args.results or (config.results_dir / "latest.jsonl")
        summary = write_reports(load_results(results_path), config.reports_dir)
        print(json.dumps(summary, indent=2, sort_keys=True))
    elif args.command == "import-query":
        record = import_query(
            config,
            source=args.source,
            kind=args.kind,
            query_id=args.query_id,
            expected_start_seconds=args.expected_start,
            notes=args.notes,
        )
        print(json.dumps(record.to_dict(), indent=2, sort_keys=True))
    elif args.command == "status":
        records = load_manifest(config.manifest_path) if config.manifest_path.exists() else []
        status = {
            "source_ready": config.source_audio.exists(),
            "manifest": str(config.manifest_path),
            "queries": {kind: sum(record.kind == kind for record in records) for kind in ("clean", "tiktok", "no_match")},
            "indexes": {name: (config.indexes_dir / name).exists() for name in ENGINE_NAMES},
            "results_ready": (config.results_dir / "latest.jsonl").exists(),
        }
        print(json.dumps(status, indent=2, sort_keys=True))
    elif args.command == "preflight":
        result = run_preflight(config, args.engine)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["ok"] else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
