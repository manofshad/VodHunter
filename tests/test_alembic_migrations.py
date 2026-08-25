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

    def test_video_status_revision_adds_status_column_and_backfills_from_processed(self) -> None:
        revision = self._load_module(
            'alembic/versions/20260419_0008_add_video_status_and_internal_api_fields.py',
            'vodhunter_alembic_revision_video_status',
        )
        fake_op = FakeOp()
        with patch.object(revision, 'op', fake_op):
            revision.upgrade()
        assert any(('ADD COLUMN IF NOT EXISTS status TEXT' in sql for sql in fake_op.executed))
        assert any(
            (
                "SET status = CASE\n            WHEN processed THEN 'searchable'\n            ELSE 'indexing'\n        END" in sql
            )
            for sql in fake_op.executed
        )
        assert any(('ALTER COLUMN status SET NOT NULL' in sql for sql in fake_op.executed))
        assert any(
            (
                "CHECK (status IN ('indexing', 'searchable', 'deleted', 'reindex_requested'))" in sql
            )
            for sql in fake_op.executed
        )

    def test_search_date_range_revision_adds_fields_and_streamed_at_index(self) -> None:
        revision = self._load_module(
            'alembic/versions/20260522_0009_add_search_date_range_fields.py',
            'vodhunter_alembic_revision_search_date_range',
        )
        fake_op = FakeOp()
        with patch.object(revision, 'op', fake_op):
            revision.upgrade()
        assert any(('ADD COLUMN IF NOT EXISTS streamed_from TIMESTAMPTZ' in sql for sql in fake_op.executed))
        assert any(('ADD COLUMN IF NOT EXISTS streamed_to TIMESTAMPTZ' in sql for sql in fake_op.executed))
        assert any(('CREATE INDEX IF NOT EXISTS idx_videos_creator_streamed_at' in sql for sql in fake_op.executed))
        assert any(('WHERE streamed_at IS NOT NULL' in sql for sql in fake_op.executed))

    def test_nmfp_revision_rebuilds_vectors_and_adds_durable_results(self) -> None:
        revision = self._load_module(
            'alembic/versions/20260824_0010_nmfp_production_schema.py',
            'vodhunter_alembic_revision_nmfp_production',
        )
        fake_op = FakeOp()
        with patch.object(revision, 'op', fake_op):
            revision.upgrade()

        combined_sql = '\n'.join(fake_op.executed)
        assert revision.down_revision == '20260522_0009'
        assert 'DELETE FROM fingerprint_embeddings' in combined_sql
        assert 'DELETE FROM fingerprints' in combined_sql
        assert 'DELETE FROM vod_ingest_state' in combined_sql
        assert "SET status = 'reindex_requested'" in combined_sql
        assert 'ALTER COLUMN embedding TYPE vector(128)' in combined_sql
        assert 'ADD COLUMN IF NOT EXISTS model_version TEXT' in combined_sql
        assert 'ADD COLUMN IF NOT EXISTS preprocessing_version TEXT' in combined_sql
        assert 'ALTER COLUMN model_version SET NOT NULL' in combined_sql
        assert 'ALTER COLUMN preprocessing_version SET NOT NULL' in combined_sql
        assert 'DROP COLUMN IF EXISTS model_name' in combined_sql
        assert 'CREATE INDEX IF NOT EXISTS idx_fingerprint_embeddings_hnsw_cos' in combined_sql
        assert 'CREATE TABLE IF NOT EXISTS fingerprint_index_metadata' in combined_sql
        assert 'INSERT INTO fingerprint_index_metadata' in combined_sql
        assert 'nmfp-triplet@15c6f3bcdf6a6da1daddfe47a1ffa5a0d22deadc+zenodo-15719945+ckpt-100' in combined_sql
        assert 'nmfp-8khz-mono-1s-hop0.5-mel-v1' in combined_sql
        assert 'ADD COLUMN IF NOT EXISTS result_payload JSONB' in combined_sql
        assert 'ADD COLUMN IF NOT EXISTS model_startup_duration_ms INTEGER' in combined_sql
        assert 'ADD COLUMN IF NOT EXISTS model_cold_start BOOLEAN' in combined_sql
        assert 'ADD COLUMN IF NOT EXISTS fingerprint_preprocessing_duration_ms INTEGER' in combined_sql
        assert 'ADD COLUMN IF NOT EXISTS fingerprint_inference_duration_ms INTEGER' in combined_sql
        assert 'ADD COLUMN IF NOT EXISTS fingerprint_duration_ms INTEGER' in combined_sql
        assert 'ADD COLUMN IF NOT EXISTS query_fingerprint_count INTEGER' in combined_sql
        assert 'ADD COLUMN IF NOT EXISTS candidate_count INTEGER' in combined_sql
        assert 'ADD COLUMN IF NOT EXISTS segment_count INTEGER' in combined_sql

    def test_nmfp_revision_downgrade_does_not_reinterpret_nmfp_vectors_as_ast(self) -> None:
        revision = self._load_module(
            'alembic/versions/20260824_0010_nmfp_production_schema.py',
            'vodhunter_alembic_revision_nmfp_production_downgrade',
        )
        fake_op = FakeOp()
        with patch.object(revision, 'op', fake_op):
            revision.downgrade()

        combined_sql = '\n'.join(fake_op.executed)
        assert combined_sql.index('DELETE FROM fingerprint_embeddings') < combined_sql.index(
            'ALTER COLUMN embedding TYPE vector(768)'
        )
        assert 'DROP TABLE IF EXISTS fingerprint_index_metadata' in combined_sql
        assert 'DROP COLUMN IF EXISTS result_payload' in combined_sql
