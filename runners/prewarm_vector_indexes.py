"""One-shot cache warmup for active creator HNSW indexes."""

from __future__ import annotations

import os
from pathlib import Path
import sys

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")

from storage.database import PostgresDatabase
from storage.vector_cache_repository import PrewarmResult, prewarm_vector_indexes


def report_result(result: PrewarmResult) -> None:
    print(
        f"prewarmed relation={result.relation_name} "
        f"size_bytes={result.size_bytes} blocks_loaded={result.blocks_loaded} "
        f"seconds={result.duration_seconds:.3f}",
        flush=True,
    )


def main() -> int:
    database = PostgresDatabase(os.getenv("DATABASE_URL", "").strip())
    database.ensure_schema_ready()
    results = prewarm_vector_indexes(database, on_result=report_result)
    print(f"prewarm_complete relation_count={len(results)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
