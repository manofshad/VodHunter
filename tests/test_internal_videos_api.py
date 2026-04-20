from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from backend import config
from backend.apps.admin import create_admin_app
from backend.apps.public import create_public_app
from storage.vector_store import (
    InvalidVideoStateTransitionError,
    VectorStore,
    VideoNotFoundError,
    VideoOwnerMismatchError,
)


class StubInternalVideoStore:
    def __init__(self):
        self.delete_calls: list[tuple[int, int]] = []
        self.reindex_calls: list[tuple[int, int]] = []
        self.delete_result = "deleted"
        self.reindex_result = "reindex_requested"
        self.delete_error: Exception | None = None
        self.reindex_error: Exception | None = None

    def delete_video_index(self, video_id: int, actor_creator_id: int) -> str:
        self.delete_calls.append((int(video_id), int(actor_creator_id)))
        if self.delete_error is not None:
            raise self.delete_error
        return self.delete_result

    def request_video_reindex(self, video_id: int, actor_creator_id: int) -> str:
        self.reindex_calls.append((int(video_id), int(actor_creator_id)))
        if self.reindex_error is not None:
            raise self.reindex_error
        return self.reindex_result


def _build_client(store: StubInternalVideoStore) -> tuple[Any, TestClient]:
    app = create_admin_app(enable_lifespan=False)
    app.state.store = store
    return app, TestClient(app)


def _build_public_client(store: StubInternalVideoStore) -> tuple[Any, TestClient]:
    app = create_public_app(enable_lifespan=False)
    app.state.store = store
    return app, TestClient(app)


def test_public_app_exposes_internal_video_routes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "INTERNAL_API_KEY", "test-internal-key")
    store = StubInternalVideoStore()
    app, client = _build_public_client(store)

    with client:
        response = client.post(
            "/internal/videos/11/delete-index",
            headers={"X-Internal-Api-Key": "test-internal-key"},
            json={"actor_creator_id": 99},
        )

    assert response.status_code == 200
    assert response.json() == {"video_id": 11, "status": "deleted"}
    assert store.delete_calls == [(11, 99)]


def test_delete_index_returns_deleted_for_searchable_video(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "INTERNAL_API_KEY", "test-internal-key")
    store = StubInternalVideoStore()
    app, client = _build_client(store)

    with client:
        response = client.post(
            "/internal/videos/11/delete-index",
            headers={"X-Internal-Api-Key": "test-internal-key"},
            json={"actor_creator_id": 99},
        )

    assert response.status_code == 200
    assert response.json() == {"video_id": 11, "status": "deleted"}
    assert store.delete_calls == [(11, 99)]
    assert store.reindex_calls == []


def test_request_reindex_returns_reindex_requested_for_deleted_video(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "INTERNAL_API_KEY", "test-internal-key")
    store = StubInternalVideoStore()
    app, client = _build_client(store)

    with client:
        response = client.post(
            "/internal/videos/22/request-reindex",
            headers={"X-Internal-Api-Key": "test-internal-key"},
            json={"actor_creator_id": 44},
        )

    assert response.status_code == 200
    assert response.json() == {"video_id": 22, "status": "reindex_requested"}
    assert store.delete_calls == []
    assert store.reindex_calls == [(22, 44)]


@pytest.mark.parametrize("header_value", [None, "wrong-key"])
def test_internal_videos_rejects_bad_or_missing_api_key(
    monkeypatch: pytest.MonkeyPatch, header_value: str | None
) -> None:
    monkeypatch.setattr(config, "INTERNAL_API_KEY", "test-internal-key")
    store = StubInternalVideoStore()
    app, client = _build_client(store)
    headers = {"X-Internal-Api-Key": header_value} if header_value is not None else {}

    with client:
        response = client.post(
            "/internal/videos/11/delete-index",
            headers=headers,
            json={"actor_creator_id": 99},
        )

    assert response.status_code == 401
    assert response.json() == {
        "detail": {
            "code": "INVALID_INTERNAL_API_KEY",
            "message": "X-Internal-Api-Key is missing or invalid",
        }
    }
    assert store.delete_calls == []
    assert store.reindex_calls == []


def test_internal_videos_rejects_creator_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "INTERNAL_API_KEY", "test-internal-key")
    store = StubInternalVideoStore()
    store.delete_error = VideoOwnerMismatchError()
    app, client = _build_client(store)

    with client:
        response = client.post(
            "/internal/videos/11/delete-index",
            headers={"X-Internal-Api-Key": "test-internal-key"},
            json={"actor_creator_id": 12},
        )

    assert response.status_code == 403
    assert response.json() == {
        "detail": {
            "code": "VIDEO_OWNER_MISMATCH",
            "message": "Video does not belong to actor_creator_id",
        }
    }
    assert store.delete_calls == [(11, 12)]


def test_internal_videos_returns_not_found_for_missing_video(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "INTERNAL_API_KEY", "test-internal-key")
    store = StubInternalVideoStore()
    store.delete_error = VideoNotFoundError()
    app, client = _build_client(store)

    with client:
        response = client.post(
            "/internal/videos/404/delete-index",
            headers={"X-Internal-Api-Key": "test-internal-key"},
            json={"actor_creator_id": 99},
        )

    assert response.status_code == 404
    assert response.json() == {
        "detail": {
            "code": "VIDEO_NOT_FOUND",
            "message": "Video was not found",
        }
    }


@pytest.mark.parametrize(("path", "error"), [
    ("/internal/videos/11/delete-index", InvalidVideoStateTransitionError("indexing")),
    ("/internal/videos/11/delete-index", InvalidVideoStateTransitionError("reindex_requested")),
    ("/internal/videos/11/request-reindex", InvalidVideoStateTransitionError("searchable")),
    ("/internal/videos/11/request-reindex", InvalidVideoStateTransitionError("indexing")),
])
def test_internal_videos_rejects_invalid_state_transitions(
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    error: InvalidVideoStateTransitionError,
) -> None:
    monkeypatch.setattr(config, "INTERNAL_API_KEY", "test-internal-key")
    store = StubInternalVideoStore()
    if path.endswith("delete-index"):
        store.delete_error = error
    else:
        store.reindex_error = error
    app, client = _build_client(store)

    with client:
        response = client.post(
            path,
            headers={"X-Internal-Api-Key": "test-internal-key"},
            json={"actor_creator_id": 99},
        )

    assert response.status_code == 409
    assert response.json() == {
        "detail": {
            "code": "INVALID_VIDEO_STATE_TRANSITION",
            "message": f"Cannot apply requested transition from status '{error.current_status}'",
        }
    }


def test_delete_index_is_idempotent_when_video_already_deleted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "INTERNAL_API_KEY", "test-internal-key")
    store = StubInternalVideoStore()
    app, client = _build_client(store)

    with client:
        response = client.post(
            "/internal/videos/11/delete-index",
            headers={"X-Internal-Api-Key": "test-internal-key"},
            json={"actor_creator_id": 99},
        )

    assert response.status_code == 200
    assert response.json() == {"video_id": 11, "status": "deleted"}
    assert store.delete_calls == [(11, 99)]


def test_request_reindex_is_idempotent_when_already_requested(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "INTERNAL_API_KEY", "test-internal-key")
    store = StubInternalVideoStore()
    app, client = _build_client(store)

    with client:
        response = client.post(
            "/internal/videos/11/request-reindex",
            headers={"X-Internal-Api-Key": "test-internal-key"},
            json={"actor_creator_id": 99},
        )

    assert response.status_code == 200
    assert response.json() == {"video_id": 11, "status": "reindex_requested"}
    assert store.reindex_calls == [(11, 99)]


class FakeCursor:
    def __init__(self, fetchone_results: list[tuple | None] | None = None):
        self.executed: list[tuple[str, tuple | None]] = []
        self._fetchone_results = list(fetchone_results or [])

    def execute(self, query: str, params=None):
        self.executed.append((query, params))

    def fetchone(self):
        if self._fetchone_results:
            return self._fetchone_results.pop(0)
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None


class FakeConnection:
    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None


def test_delete_video_index_purges_related_rows_before_marking_deleted() -> None:
    cursor = FakeCursor(fetchone_results=[(99, "searchable")])
    store = VectorStore.__new__(VectorStore)
    store._connect = lambda: FakeConnection(cursor)

    result = store.delete_video_index(55, actor_creator_id=99)

    assert result == "deleted"
    assert "SELECT creator_id, status" in cursor.executed[0][0]
    assert "FOR UPDATE" in cursor.executed[0][0]
    assert cursor.executed[0][1] == (55,)
    assert "DELETE FROM fingerprint_embeddings" in cursor.executed[1][0]
    assert cursor.executed[1][1] == (55,)
    assert "DELETE FROM fingerprints WHERE video_id = %s" in cursor.executed[2][0]
    assert cursor.executed[2][1] == (55,)
    assert "DELETE FROM vod_ingest_state WHERE video_id = %s" in cursor.executed[3][0]
    assert cursor.executed[3][1] == (55,)
    assert cursor.executed[4] == (
        "UPDATE videos SET status = %s, processed = %s WHERE id = %s",
        ("deleted", True, 55),
    )


def test_delete_video_index_is_idempotent_when_locked_row_is_already_deleted() -> None:
    cursor = FakeCursor(fetchone_results=[(99, "deleted")])
    store = VectorStore.__new__(VectorStore)
    store._connect = lambda: FakeConnection(cursor)

    result = store.delete_video_index(55, actor_creator_id=99)

    assert result == "deleted"
    assert len(cursor.executed) == 1
    assert "FOR UPDATE" in cursor.executed[0][0]


def test_delete_video_index_rejects_owner_mismatch_from_locked_row() -> None:
    cursor = FakeCursor(fetchone_results=[(42, "searchable")])
    store = VectorStore.__new__(VectorStore)
    store._connect = lambda: FakeConnection(cursor)

    with pytest.raises(VideoOwnerMismatchError):
        store.delete_video_index(55, actor_creator_id=99)

    assert len(cursor.executed) == 1
    assert "FOR UPDATE" in cursor.executed[0][0]


def test_request_video_reindex_clears_ingest_state_before_marking_requested() -> None:
    cursor = FakeCursor(fetchone_results=[(99, "deleted")])
    store = VectorStore.__new__(VectorStore)
    store._connect = lambda: FakeConnection(cursor)

    result = store.request_video_reindex(56, actor_creator_id=99)

    assert result == "reindex_requested"
    assert "SELECT creator_id, status" in cursor.executed[0][0]
    assert "FOR UPDATE" in cursor.executed[0][0]
    assert cursor.executed[0][1] == (56,)
    assert cursor.executed[1] == ("DELETE FROM vod_ingest_state WHERE video_id = %s", (56,))
    assert cursor.executed[2] == (
        "UPDATE videos SET status = %s, processed = %s WHERE id = %s",
        ("reindex_requested", True, 56),
    )


def test_request_video_reindex_rejects_invalid_transition_from_locked_row() -> None:
    cursor = FakeCursor(fetchone_results=[(99, "searchable")])
    store = VectorStore.__new__(VectorStore)
    store._connect = lambda: FakeConnection(cursor)

    with pytest.raises(InvalidVideoStateTransitionError) as exc_info:
        store.request_video_reindex(56, actor_creator_id=99)

    assert exc_info.value.current_status == "searchable"
    assert len(cursor.executed) == 1
