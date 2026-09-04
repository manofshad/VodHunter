from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from runners import run_vod_retention
from runners.run_vod_retention import (
    run_forever,
    run_with_retries,
    seconds_until_next_daily_run,
)
from services.vod_retention import VodRetentionResult, purge_expired_vods


UTC = timezone.utc
ROOT_DIR = Path(__file__).resolve().parents[1]


class FakeDatabase:
    def __init__(self, *, now: datetime):
        self.now = now
        self.videos: dict[int, dict[str, object]] = {}
        self.fingerprints: dict[int, dict[str, int]] = {}
        self.embeddings: set[int] = set()
        self.ingest_state_video_ids: set[int] = set()
        self.search_requests: list[dict[str, int | None]] = []
        self.creators: dict[int, str] = {}
        self.index_metadata: dict[str, object] = {}
        self.fail_on: str | None = None
        self.commits = 0
        self.rollbacks = 0
        self.connections: list[FakeConnection] = []

    def connect(self, database_url: str) -> "FakeConnection":
        assert database_url == "postgresql://example/vodhunter"
        connection = FakeConnection(self)
        self.connections.append(connection)
        return connection

    def snapshot(self) -> dict[str, object]:
        return {
            "videos": deepcopy(self.videos),
            "fingerprints": deepcopy(self.fingerprints),
            "embeddings": deepcopy(self.embeddings),
            "ingest_state_video_ids": deepcopy(self.ingest_state_video_ids),
            "search_requests": deepcopy(self.search_requests),
            "creators": deepcopy(self.creators),
            "index_metadata": deepcopy(self.index_metadata),
        }

    def restore(self, snapshot: dict[str, object]) -> None:
        self.videos = snapshot["videos"]  # type: ignore[assignment]
        self.fingerprints = snapshot["fingerprints"]  # type: ignore[assignment]
        self.embeddings = snapshot["embeddings"]  # type: ignore[assignment]
        self.ingest_state_video_ids = snapshot["ingest_state_video_ids"]  # type: ignore[assignment]
        self.search_requests = snapshot["search_requests"]  # type: ignore[assignment]
        self.creators = snapshot["creators"]  # type: ignore[assignment]
        self.index_metadata = snapshot["index_metadata"]  # type: ignore[assignment]


class FakeConnection:
    def __init__(self, database: FakeDatabase):
        self.database = database
        self.snapshot_value: dict[str, object] | None = None
        self.cursor_value = FakeCursor(database)

    def __enter__(self) -> "FakeConnection":
        self.snapshot_value = self.database.snapshot()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is not None:
            assert self.snapshot_value is not None
            self.database.restore(self.snapshot_value)
            self.database.rollbacks += 1
        else:
            self.database.commits += 1
        return False

    def cursor(self) -> "FakeCursor":
        return self.cursor_value


class FakeCursor:
    def __init__(self, database: FakeDatabase):
        self.database = database
        self.executed: list[tuple[str, tuple | None]] = []
        self.rows: list[tuple[int]] = []
        self.rowcount = -1

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, query: str, params=None) -> None:
        normalized_query = " ".join(query.split())
        self.executed.append((query, params))
        self.rowcount = -1

        if normalized_query == "SET TIME ZONE 'UTC'":
            return

        if normalized_query.startswith("SELECT v.id"):
            retention_days = int(params[0])
            cutoff = self.database.now - timedelta(days=retention_days)
            candidate_ids = [
                video_id
                for video_id, video in self.database.videos.items()
                if video["streamed_at"] is not None
                and video["streamed_at"] < cutoff
                and video.get("status") != "indexing"
                and video_id not in self.database.ingest_state_video_ids
            ]
            candidate_ids.sort(
                key=lambda video_id: (
                    self.database.videos[video_id]["streamed_at"],
                    video_id,
                )
            )
            self.rows = [(video_id,) for video_id in candidate_ids]
            return

        if normalized_query.startswith("UPDATE search_requests"):
            self._fail_if_requested("search_requests")
            video_id = int(params[0])
            changed = 0
            for request in self.database.search_requests:
                if request["matched_video_id"] == video_id:
                    request["matched_video_id"] = None
                    changed += 1
            self.rowcount = changed
            return

        if normalized_query.startswith("DELETE FROM fingerprint_embeddings"):
            self._fail_if_requested("fingerprint_embeddings")
            video_id = int(params[0])
            fingerprint_ids = {
                fingerprint_id
                for fingerprint_id, fingerprint in self.database.fingerprints.items()
                if fingerprint["video_id"] == video_id
            }
            before = len(self.database.embeddings)
            self.database.embeddings.difference_update(fingerprint_ids)
            self.rowcount = before - len(self.database.embeddings)
            return

        if normalized_query.startswith("DELETE FROM fingerprints"):
            self._fail_if_requested("fingerprints")
            video_id = int(params[0])
            fingerprint_ids = [
                fingerprint_id
                for fingerprint_id, fingerprint in self.database.fingerprints.items()
                if fingerprint["video_id"] == video_id
            ]
            for fingerprint_id in fingerprint_ids:
                del self.database.fingerprints[fingerprint_id]
            self.rowcount = len(fingerprint_ids)
            return

        if normalized_query.startswith("DELETE FROM vod_ingest_state"):
            self._fail_if_requested("vod_ingest_state")
            video_id = int(params[0])
            if video_id in self.database.ingest_state_video_ids:
                self.database.ingest_state_video_ids.remove(video_id)
                self.rowcount = 1
            else:
                self.rowcount = 0
            return

        if normalized_query.startswith("DELETE FROM videos"):
            self._fail_if_requested("videos")
            video_id = int(params[0])
            if video_id in self.database.videos:
                del self.database.videos[video_id]
                self.rowcount = 1
            else:
                self.rowcount = 0
            return

        raise AssertionError(f"Unexpected SQL: {query}")

    def _fail_if_requested(self, operation: str) -> None:
        if self.database.fail_on == operation:
            raise RuntimeError(f"injected {operation} failure")

    def fetchall(self) -> list[tuple[int]]:
        return list(self.rows)


def _build_database(now: datetime) -> FakeDatabase:
    database = FakeDatabase(now=now)
    database.videos = {
        1: {"streamed_at": now - timedelta(days=31), "status": "searchable"},
        2: {"streamed_at": now - timedelta(days=29), "status": "searchable"},
        3: {"streamed_at": now - timedelta(days=30), "status": "searchable"},
        4: {"streamed_at": None, "status": "searchable"},
        5: {"streamed_at": now - timedelta(days=31), "status": "indexing"},
        6: {"streamed_at": now - timedelta(days=31), "status": "searchable"},
    }
    database.fingerprints = {
        101: {"video_id": 1},
        105: {"video_id": 5},
        106: {"video_id": 6},
    }
    database.embeddings = {101, 105, 106}
    database.ingest_state_video_ids = {6}
    database.search_requests = [
        {"matched_video_id": 1},
        {"matched_video_id": 2},
        {"matched_video_id": None},
    ]
    database.creators = {7: "alice"}
    database.index_metadata = {
        "model_version": "nmfp",
        "preprocessing_version": "v1",
    }
    return database


def test_purges_only_strictly_older_non_active_vods_and_preserves_unrelated_rows() -> None:
    now = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)
    database = _build_database(now)

    result = purge_expired_vods(
        "postgresql://example/vodhunter",
        30,
        connect=database.connect,
    )

    assert result == VodRetentionResult(retention_days=30, deleted_video_ids=(1,))
    assert 1 not in database.videos
    assert 2 in database.videos  # newer
    assert 3 in database.videos  # exact boundary: strict '<'
    assert 4 in database.videos  # NULL dates are never eligible
    assert 5 in database.videos  # active status is protected
    assert 6 in database.videos  # active ingest state is protected
    assert database.fingerprints == {
        105: {"video_id": 5},
        106: {"video_id": 6},
    }
    assert database.embeddings == {105, 106}
    assert database.search_requests == [
        {"matched_video_id": None},
        {"matched_video_id": 2},
        {"matched_video_id": None},
    ]
    assert database.creators == {7: "alice"}
    assert database.index_metadata["model_version"] == "nmfp"
    assert database.commits == 1
    assert database.rollbacks == 0

    executed = database.connections[0].cursor_value.executed
    candidate_query, candidate_params = next(
        (query, params)
        for query, params in executed
        if "SELECT v.id" in query
    )
    assert "NOW()" in candidate_query
    assert "v.streamed_at IS NOT NULL" in candidate_query
    assert "v.streamed_at < NOW()" in candidate_query
    assert "FOR UPDATE OF v SKIP LOCKED" in candidate_query
    assert candidate_params == (30,)


def test_rolls_back_all_changes_when_a_dependency_delete_fails() -> None:
    now = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)
    database = _build_database(now)
    before = database.snapshot()
    database.fail_on = "fingerprints"

    with pytest.raises(RuntimeError, match="injected fingerprints failure"):
        purge_expired_vods(
            "postgresql://example/vodhunter",
            30,
            connect=database.connect,
        )

    assert database.snapshot() == before
    assert database.commits == 0
    assert database.rollbacks == 1


def test_repeated_retention_pass_is_idempotent() -> None:
    now = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)
    database = _build_database(now)

    first = purge_expired_vods("postgresql://example/vodhunter", 30, connect=database.connect)
    second = purge_expired_vods("postgresql://example/vodhunter", 30, connect=database.connect)

    assert first.deleted_video_ids == (1,)
    assert second.deleted_video_ids == ()
    assert database.commits == 2
    assert database.rollbacks == 0


def test_rejects_invalid_retention_days() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        purge_expired_vods("postgresql://example/vodhunter", 0, connect=lambda _: None)


def test_daily_scheduler_waits_until_utc_run_time() -> None:
    before_run = datetime(2026, 9, 3, 2, 59, 0, tzinfo=UTC)
    at_run = datetime(2026, 9, 3, 3, 0, 0, tzinfo=UTC)
    after_run = datetime(2026, 9, 3, 3, 1, 0, tzinfo=UTC)

    assert seconds_until_next_daily_run(before_run) == 60.0
    assert seconds_until_next_daily_run(at_run) == 24 * 60 * 60
    assert seconds_until_next_daily_run(after_run) == (23 * 60 + 59) * 60


def test_daily_scheduler_runs_one_pass_and_can_stop() -> None:
    sleeps: list[float] = []
    passes: list[bool] = []
    stop = {"requested": False}

    def run_pass() -> VodRetentionResult:
        passes.append(True)
        stop["requested"] = True
        return VodRetentionResult(retention_days=30, deleted_video_ids=())

    run_forever(
        run_pass=run_pass,
        sleep=sleeps.append,
        now=lambda: datetime(2026, 9, 3, 2, 0, tzinfo=UTC),
        should_stop=lambda: stop["requested"],
    )

    assert sleeps == [60 * 60]
    assert passes == [True]


def test_daily_scheduler_retries_before_waiting_for_the_next_day() -> None:
    sleeps: list[float] = []
    attempts: list[int] = []
    stop = {"requested": False}

    def run_pass() -> VodRetentionResult:
        attempts.append(len(attempts) + 1)
        if len(attempts) == 1:
            raise RuntimeError("temporary database failure")
        stop["requested"] = True
        return VodRetentionResult(retention_days=30, deleted_video_ids=())

    run_forever(
        run_pass=run_pass,
        sleep=sleeps.append,
        now=lambda: datetime(2026, 9, 3, 2, 0, tzinfo=UTC),
        should_stop=lambda: stop["requested"],
    )

    assert attempts == [1, 2]
    assert sleeps == [60 * 60, 60]


def test_scheduled_pass_retries_with_bounded_exponential_backoff() -> None:
    sleeps: list[float] = []
    attempts: list[int] = []

    def run_pass() -> VodRetentionResult:
        attempts.append(len(attempts) + 1)
        if len(attempts) < 4:
            raise RuntimeError("temporary database failure")
        return VodRetentionResult(retention_days=30, deleted_video_ids=())

    result = run_with_retries(
        run_pass=run_pass,
        sleep=sleeps.append,
        max_attempts=4,
        retry_base_delay_seconds=10,
        retry_max_delay_seconds=15,
    )

    assert result == VodRetentionResult(retention_days=30, deleted_video_ids=())
    assert attempts == [1, 2, 3, 4]
    assert sleeps == [10, 15, 15]


def test_scheduled_pass_returns_after_retry_limit() -> None:
    sleeps: list[float] = []
    attempts: list[bool] = []

    def run_pass() -> VodRetentionResult:
        attempts.append(True)
        raise RuntimeError("database unavailable")

    result = run_with_retries(
        run_pass=run_pass,
        sleep=sleeps.append,
        max_attempts=3,
        retry_base_delay_seconds=10,
        retry_max_delay_seconds=60,
    )

    assert result is None
    assert attempts == [True, True, True]
    assert sleeps == [10, 20]


def test_once_cli_runs_one_pass_and_exits(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[bool] = []
    monkeypatch.setattr(run_vod_retention, "run_once", lambda: calls.append(True))

    assert run_vod_retention.main(["--once"]) == 0
    assert calls == [True]


def test_retention_container_is_independent_of_the_heavy_api_runtime() -> None:
    dockerfile = (ROOT_DIR / "Dockerfile.retention").read_text()
    requirements = (ROOT_DIR / "backend/requirements-retention.txt").read_text()

    assert "FROM python:3.11-slim" in dockerfile
    assert "requirements-retention.txt" in dockerfile
    assert "requirements-api-public.txt" not in dockerfile
    assert "tensorflow" not in requirements
    assert "psycopg[binary]==3.2.1" in requirements


def test_deployment_guide_invokes_the_one_shot_runner() -> None:
    deployment_guide = (ROOT_DIR / "docs/vps-deployment.md").read_text()

    assert (
        "vod-retention python -m runners.run_vod_retention --once"
        in deployment_guide
    )
