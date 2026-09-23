from __future__ import annotations

import pytest

from storage.vector_cache_repository import (
    list_active_vector_indexes,
    prewarm_vector_indexes,
)


class FakeCursor:
    def __init__(self, *, missing_index: bool = False) -> None:
        self.missing_index = missing_index
        self.executed: list[tuple[str, tuple | None]] = []
        self.last_query = ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def execute(self, query: str, params=None) -> None:
        self.last_query = query
        self.executed.append((query, params))

    def fetchall(self):
        if "pg_partition_tree" in self.last_query:
            return [
                (1, "fingerprint_embeddings_creator_1", 101,
                 "fingerprint_embeddings_creator_1_hnsw_cos", 1_000_000),
                (35, "fingerprint_embeddings_creator_35",
                 None if self.missing_index else 135,
                 None if self.missing_index else "fingerprint_embeddings_creator_35_hnsw_cos",
                 None if self.missing_index else 900_000),
            ]
        raise AssertionError(self.last_query)

    def fetchone(self):
        if "pg_try_advisory_xact_lock" in self.last_query:
            return (True,)
        if "index_metadata.indisprimary" in self.last_query:
            return (200, "fingerprints_pkey", 100_000)
        if "pg_prewarm" in self.last_query:
            return (42,)
        raise AssertionError(self.last_query)


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self.cursor_value = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def cursor(self):
        return self.cursor_value


class FakeDatabase:
    def __init__(self, cursor: FakeCursor) -> None:
        self.cursor = cursor

    def connect(self):
        return FakeConnection(self.cursor)


def test_prewarm_discovers_active_indexes_and_fingerprint_primary_key() -> None:
    cursor = FakeCursor()
    reported = []
    results = prewarm_vector_indexes(  # type: ignore[arg-type]
        FakeDatabase(cursor), on_result=reported.append
    )

    assert [result.relation_name for result in results] == [
        "fingerprint_embeddings_creator_1_hnsw_cos",
        "fingerprint_embeddings_creator_35_hnsw_cos",
        "fingerprints_pkey",
    ]
    assert all(result.blocks_loaded == 42 for result in results)
    assert reported == results
    warm_calls = [params for sql, params in cursor.executed if "pg_prewarm" in sql]
    assert warm_calls == [(101,), (135,), (200,)]
    discovery_sql = next(sql for sql, _ in cursor.executed if "pg_partition_tree" in sql)
    assert "index_metadata.indisvalid" in discovery_sql
    assert "operator_class.opcname = 'vector_cosine_ops'" in discovery_sql
    assert "backup" not in discovery_sql


def test_missing_active_index_stops_before_any_prewarm() -> None:
    cursor = FakeCursor(missing_index=True)
    with pytest.raises(RuntimeError, match="fingerprint_embeddings_creator_35"):
        prewarm_vector_indexes(FakeDatabase(cursor))  # type: ignore[arg-type]
    assert not any("pg_prewarm" in sql for sql, _ in cursor.executed)


def test_discovery_returns_missing_index_for_validation() -> None:
    indexes = list_active_vector_indexes(FakeCursor(missing_index=True))
    assert indexes[1].index_oid is None
