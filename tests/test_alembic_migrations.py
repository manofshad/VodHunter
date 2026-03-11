import unittest
from pathlib import Path
import sys
import importlib.util
from unittest.mock import patch

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from backend.db_url import normalize_database_url, normalize_sqlalchemy_database_url


class FakeContext:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None


class FakeOp:
    def __init__(self):
        self.executed: list[str] = []

    def execute(self, sql: str) -> None:
        self.executed.append(sql)

    def get_context(self):
        return self

    def autocommit_block(self):
        return FakeContext()


class TestAlembicMigrations(unittest.TestCase):
    def _load_module(self, relative_path: str, module_name: str):
        module_path = ROOT_DIR / relative_path
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        if spec is None or spec.loader is None:
            raise AssertionError(f"Could not load module from {module_path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_normalize_database_url_handles_psycopg_scheme(self) -> None:
        self.assertEqual(
            normalize_database_url("postgresql+psycopg://user:pass@db/app"),
            "postgresql://user:pass@db/app",
        )

    def test_normalize_sqlalchemy_database_url_adds_psycopg_scheme(self) -> None:
        self.assertEqual(
            normalize_sqlalchemy_database_url("postgresql://user:pass@db/app"),
            "postgresql+psycopg://user:pass@db/app",
        )

    def test_baseline_revision_uses_raw_sql(self) -> None:
        revision = self._load_module(
            "alembic/versions/20260310_0001_baseline_schema.py",
            "vodhunter_alembic_revision_0001",
        )
        fake_op = FakeOp()

        with patch.object(revision, "op", fake_op):
            revision.upgrade()

        self.assertTrue(any("CREATE EXTENSION IF NOT EXISTS vector" in sql for sql in fake_op.executed))
        self.assertTrue(any("CREATE TABLE IF NOT EXISTS fingerprint_embeddings" in sql for sql in fake_op.executed))
        self.assertTrue(any("CREATE INDEX IF NOT EXISTS idx_fingerprint_embeddings_ivfflat_cos" in sql for sql in fake_op.executed))

    def test_hnsw_revision_uses_concurrent_index_sql(self) -> None:
        revision = self._load_module(
            "alembic/versions/20260310_0002_add_hnsw_index.py",
            "vodhunter_alembic_revision_0002",
        )
        fake_op = FakeOp()

        with patch.object(revision, "op", fake_op):
            revision.upgrade()

        self.assertEqual(len(fake_op.executed), 1)
        self.assertIn("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fingerprint_embeddings_hnsw_cos", fake_op.executed[0])

    def test_drop_ivfflat_revision_uses_concurrent_drop_sql(self) -> None:
        revision = self._load_module(
            "alembic/versions/20260310_0003_drop_ivfflat_index.py",
            "vodhunter_alembic_revision_0003",
        )
        fake_op = FakeOp()

        with patch.object(revision, "op", fake_op):
            revision.upgrade()

        self.assertEqual(fake_op.executed, ["DROP INDEX CONCURRENTLY IF EXISTS idx_fingerprint_embeddings_ivfflat_cos"])


if __name__ == "__main__":
    unittest.main()
