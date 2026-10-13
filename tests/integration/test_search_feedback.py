from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import psycopg
from psycopg import sql
import pytest

from search.access import create_search_token
from search.models import SearchExecutionMetadata, SearchRequestOutcome, SearchResult, SearchSource


pytestmark = pytest.mark.integration


def create_job(store, scope, *, found=True):
    token = create_search_token()
    search_id = scope.remember_search(store.search_jobs.create_public_search_job(
        search_token=token, tiktok_url="https://www.tiktok.com/@feedback/video/123",
        streamer=scope.streamer, creator_id=None,
    ))
    result = SearchResult(
        found=found, timestamp_seconds=100, score=0.92, reason="feedback fixture",
        sources=[SearchSource(video_id=7, title="First VOD"), SearchSource(video_id=8, title="Second VOD")],
    )
    outcome = SearchRequestOutcome(
        result=result, execution_metadata=SearchExecutionMetadata(found_match=found),
        input_type="tiktok_url", download_source="fixture", download_host="www.tiktok.com",
        input_duration_seconds=10.5, total_duration_ms=1234,
    )
    return token, search_id, outcome


def test_rating_retries_replacement_reload_and_full_reporting_details(store, database_scope):
    token, search_id, outcome = create_job(store, database_scope)
    assert store.search_jobs.save_public_search_feedback(token, "up") is None
    store.search_jobs.complete_search_job(search_id, outcome)
    first = store.search_jobs.save_public_search_feedback(token, "up")
    assert first.rating == "up"
    assert store.search_jobs.save_public_search_feedback(token, "up") == first
    changed = store.search_jobs.save_public_search_feedback(token, "down")
    assert changed.rating == "down"
    assert changed.updated_at > first.updated_at
    restored = store.search_jobs.get_public_search_job(token)
    assert restored.feedback == changed
    assert restored.result.to_dict() == outcome.result.to_dict()
    with store.database.connect() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FILTER (WHERE feedback_rating = 'up'), COUNT(*) FILTER (WHERE feedback_rating = 'down') FROM grafana_search_feedback WHERE search_id = %s",
            (search_id,),
        ).fetchone() == (0, 1)
        assert connection.execute(
            "SELECT tiktok_url, matched_timestamp_seconds, result_payload, input_duration_seconds, download_source, download_host, total_duration_ms FROM grafana_search_feedback WHERE search_id = %s",
            (search_id,),
        ).fetchone() == (
            "https://www.tiktok.com/@feedback/video/123", 100, outcome.result.to_dict(), 10.5,
            "fixture", "www.tiktok.com", 1234,
        )
        columns = {row[0] for row in connection.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'grafana_search_feedback'",
        ).fetchall()}
        assert not {"access_token_hash", "search_token", "ip_address"} & columns


@pytest.mark.parametrize("change", ["no_match", "running", "failed", "private", "unsuccessful", "wrong_token", "malformed"])
def test_atomic_rating_eligibility_rejection(store, database_scope, change):
    token, search_id, outcome = create_job(store, database_scope, found=change != "no_match")
    store.search_jobs.complete_search_job(search_id, outcome)
    with store.database.connect() as connection:
        if change in {"running", "failed"}:
            connection.execute("UPDATE search_requests SET job_status = %s WHERE id = %s", (change, search_id))
        elif change == "private":
            connection.execute("UPDATE search_requests SET source_app = 'internal' WHERE id = %s", (search_id,))
        elif change == "unsuccessful":
            connection.execute("UPDATE search_requests SET success = FALSE WHERE id = %s", (search_id,))
    supplied_token = create_search_token() if change == "wrong_token" else "invalid" if change == "malformed" else token
    assert store.search_jobs.save_public_search_feedback(supplied_token, "up") is None
    job = store.search_jobs.get_public_search_job(token)
    if change == "private":
        assert job is None
    else:
        assert job.feedback is None
    with store.database.connect() as connection:
        assert connection.execute("SELECT feedback_rating, feedback_updated_at FROM search_requests WHERE id = %s", (search_id,)).fetchone() == (None, None)


def test_populated_feedback_upgrade_downgrade_preserves_old_view_owner_acl_and_dependents(store, database_scope, monkeypatch):
    token, search_id, outcome = create_job(store, database_scope)
    store.search_jobs.complete_search_job(search_id, outcome)
    path = Path(__file__).resolve().parents[2] / "alembic/versions/20261012_0018_add_search_feedback.py"
    spec = importlib.util.spec_from_file_location("feedback_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    role = sql.Identifier("feedback_reader_" + database_scope.token)
    with psycopg.connect(database_scope.database_url) as connection:
        try:
            monkeypatch.setattr(migration, "op", SimpleNamespace(execute=connection.execute))
            migration.downgrade()
            connection.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(role))
            connection.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(role))
            connection.execute(sql.SQL("GRANT SELECT ON grafana_search_quality TO {}").format(role))
            connection.execute("CREATE VIEW feedback_old_view_dependency AS SELECT search_id, outcome FROM grafana_search_quality")
            old_identity = connection.execute("SELECT oid, relowner, relacl FROM pg_class WHERE oid = 'grafana_search_quality'::regclass").fetchone()
            migration.upgrade()
            assert connection.execute("SELECT oid, relowner, relacl FROM pg_class WHERE oid = 'grafana_search_quality'::regclass").fetchone() == old_identity
            assert connection.execute("SELECT feedback_rating, feedback_updated_at, result_payload FROM search_requests WHERE id = %s", (search_id,)).fetchone() == (None, None, outcome.result.to_dict())
            connection.execute("UPDATE search_requests SET feedback_rating = 'up', feedback_updated_at = NOW() WHERE id = %s", (search_id,))
            connection.execute(sql.SQL("SET LOCAL ROLE {}").format(role))
            assert connection.execute("SELECT feedback_rating, result_payload FROM grafana_search_feedback WHERE search_id = %s", (search_id,)).fetchone() == ("up", outcome.result.to_dict())
            connection.execute("RESET ROLE")
            with pytest.raises(psycopg.errors.CheckViolation):
                with connection.transaction():
                    connection.execute("UPDATE search_requests SET feedback_rating = 'neutral' WHERE id = %s", (search_id,))
            with pytest.raises(psycopg.errors.CheckViolation):
                with connection.transaction():
                    connection.execute("UPDATE search_requests SET feedback_updated_at = NULL WHERE id = %s", (search_id,))
            for assignment in ["feedback_rating = NULL", "success = FALSE", "found_match = FALSE", "job_status = 'failed'", "source_app = 'internal'"]:
                with pytest.raises(psycopg.errors.CheckViolation):
                    with connection.transaction():
                        connection.execute(sql.SQL("UPDATE search_requests SET {} WHERE id = %s").format(sql.SQL(assignment)), (search_id,))
            migration.downgrade()
            assert connection.execute("SELECT oid, relowner, relacl FROM pg_class WHERE oid = 'grafana_search_quality'::regclass").fetchone() == old_identity
            assert connection.execute("SELECT outcome FROM feedback_old_view_dependency WHERE search_id = %s", (search_id,)).fetchone() == ("match",)
            assert connection.execute("SELECT result_payload FROM search_requests WHERE id = %s", (search_id,)).fetchone() == (outcome.result.to_dict(),)
            assert connection.execute("SELECT to_regclass('grafana_search_feedback')").fetchone() == (None,)
            connection.execute(sql.SQL("SET LOCAL ROLE {}").format(role))
            assert connection.execute("SELECT outcome FROM grafana_search_quality WHERE search_id = %s", (search_id,)).fetchone() == ("match",)
        finally:
            connection.rollback()
