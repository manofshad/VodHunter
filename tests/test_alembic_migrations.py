import importlib.util
from pathlib import Path
from unittest.mock import patch

from backend.db_url import normalize_database_url, normalize_sqlalchemy_database_url


ROOT_DIR = Path(__file__).resolve().parents[1]

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

class TestAlembicMigrations:

    def _load_module(self, relative_path: str, module_name: str):
        module_path = ROOT_DIR / relative_path
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        if spec is None or spec.loader is None:
            raise AssertionError(f'Could not load module from {module_path}')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_normalize_database_url_handles_psycopg_scheme(self) -> None:
        assert normalize_database_url('postgresql+psycopg://user:pass@db/app') == 'postgresql://user:pass@db/app'

    def test_normalize_sqlalchemy_database_url_adds_psycopg_scheme(self) -> None:
        assert normalize_sqlalchemy_database_url('postgresql://user:pass@db/app') == 'postgresql+psycopg://user:pass@db/app'

    def test_baseline_revision_uses_raw_sql(self) -> None:
        revision = self._load_module('alembic/versions/20260310_0001_baseline_schema.py', 'vodhunter_alembic_revision_0001')
        fake_op = FakeOp()
        with patch.object(revision, 'op', fake_op):
            revision.upgrade()
        assert any(('CREATE EXTENSION IF NOT EXISTS vector' in sql for sql in fake_op.executed))
        assert any(('CREATE TABLE IF NOT EXISTS fingerprint_embeddings' in sql for sql in fake_op.executed))
        assert any(('CREATE INDEX IF NOT EXISTS idx_fingerprint_embeddings_ivfflat_cos' in sql for sql in fake_op.executed))

    def test_hnsw_revision_uses_concurrent_index_sql(self) -> None:
        revision = self._load_module('alembic/versions/20260310_0002_add_hnsw_index.py', 'vodhunter_alembic_revision_0002')
        fake_op = FakeOp()
        with patch.object(revision, 'op', fake_op):
            revision.upgrade()
        assert len(fake_op.executed) == 1
        assert 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fingerprint_embeddings_hnsw_cos' in fake_op.executed[0]

    def test_drop_ivfflat_revision_uses_concurrent_drop_sql(self) -> None:
        revision = self._load_module('alembic/versions/20260310_0003_drop_ivfflat_index.py', 'vodhunter_alembic_revision_0003')
        fake_op = FakeOp()
        with patch.object(revision, 'op', fake_op):
            revision.upgrade()
        assert fake_op.executed == ['DROP INDEX CONCURRENTLY IF EXISTS idx_fingerprint_embeddings_ivfflat_cos']

    def test_search_request_revision_creates_logging_table(self) -> None:
        revision = self._load_module('alembic/versions/20260330_0005_add_search_requests.py', 'vodhunter_alembic_revision_0005')
        fake_op = FakeOp()
        with patch.object(revision, 'op', fake_op):
            revision.upgrade()
        assert any(('CREATE TABLE IF NOT EXISTS search_requests' in sql for sql in fake_op.executed))
        assert any(('CREATE INDEX IF NOT EXISTS idx_search_requests_created_at' in sql for sql in fake_op.executed))

    def test_streamed_at_revision_adds_video_column(self) -> None:
        revision = self._load_module('alembic/versions/20260406_0006_add_video_streamed_at.py', 'vodhunter_alembic_revision_0006')
        fake_op = FakeOp()
        with patch.object(revision, 'op', fake_op):
            revision.upgrade()
        assert fake_op.executed == ["ALTER TABLE videos ADD COLUMN IF NOT EXISTS streamed_at TIMESTAMPTZ"]

    def test_search_request_creator_id_revision_updates_table(self) -> None:
        revision = self._load_module('alembic/versions/20260408_0007_add_search_requests_creator_id.py', 'vodhunter_alembic_revision_0007')
        fake_op = FakeOp()
        with patch.object(revision, 'op', fake_op):
            revision.upgrade()
        assert any(('ADD COLUMN IF NOT EXISTS creator_id BIGINT REFERENCES creators(id)' in sql for sql in fake_op.executed))
        assert any(('UPDATE search_requests AS sr' in sql for sql in fake_op.executed))
        assert any(('CREATE INDEX IF NOT EXISTS idx_search_requests_creator_id_created_at' in sql for sql in fake_op.executed))

    def test_search_job_fields_revision_backfills_job_status_from_success(self) -> None:
        revision = self._load_module('alembic/versions/20260415_0008_add_search_job_fields.py', 'vodhunter_alembic_revision_0008')
        fake_op = FakeOp()
        with patch.object(revision, 'op', fake_op):
            revision.upgrade()
        assert any(('ADD COLUMN IF NOT EXISTS job_status TEXT NOT NULL DEFAULT \'completed\'' in sql for sql in fake_op.executed))
        assert any(
            (
                "UPDATE search_requests\n        SET job_status = CASE WHEN success THEN 'completed' ELSE 'failed' END\n        "
                in sql
            )
            for sql in fake_op.executed
        )
        assert not any(("WHERE job_status NOT IN ('queued', 'running', 'completed', 'failed')" in sql for sql in fake_op.executed))
        assert any(('SET finished_at = COALESCE(finished_at, created_at)' in sql for sql in fake_op.executed))
        assert any(('CREATE INDEX IF NOT EXISTS idx_search_requests_source_app_job_status_created_at' in sql for sql in fake_op.executed))
