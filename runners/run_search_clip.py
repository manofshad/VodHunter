from dataclasses import asdict
from pathlib import Path
import argparse
import json
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipeline.embedder import Embedder
from backend import config
from search.alignment_service import AlignmentService, AlignmentConfig
from search.query_embedder import QueryEmbedder
from search.query_preprocessor import QueryPreprocessor
from search.search_service import SearchService
from search.vector_matcher import VectorMatcher
from storage.vector_store import VectorStore

TOP_K = 10
MIN_VOTE_COUNT = 3
MIN_VOTE_RATIO = 0.10


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Search a clip against indexed vectors")
    parser.add_argument("--clip", required=True, help="Path to query clip (video or audio)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        config.validate_storage_config()
        store = VectorStore(
            database_url=config.DATABASE_URL,
            vector_dim=config.VECTOR_DIM,
            pgvector_probes=config.PGVECTOR_PROBES,
        )
        embedder = Embedder()

        service = SearchService(
            store=store,
            preprocessor=QueryPreprocessor(temp_dir=config.TEMP_SEARCH_PREPROCESS_DIR),
            query_embedder=QueryEmbedder(embedder=embedder),
            matcher=VectorMatcher(top_k=TOP_K),
            alignment=AlignmentService(
                store=store,
                config=AlignmentConfig(
                    min_vote_count=MIN_VOTE_COUNT,
                    min_vote_ratio=MIN_VOTE_RATIO,
                ),
            ),
        )

        result = service.search_file(args.clip)
        print(json.dumps(asdict(result), indent=2))

        return 0 if result.found else 1
    except Exception as exc:
        print(json.dumps({"found": False, "error": str(exc)}, indent=2))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
