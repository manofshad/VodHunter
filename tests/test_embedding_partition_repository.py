from __future__ import annotations

import pytest

from storage.embedding_partition_repository import EmbeddingPartitionRepository


class FakeCursor:
    def __init__(self, *, relkind: str = "p", attached: bool = False) -> None:
        self.relkind = relkind
        self.attached = attached
        self.executed: list[tuple[str, tuple | None]] = []
        self.last_query = ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def execute(self, query: str, params=None) -> None:
        self.last_query = query
        self.executed.append((query, params))

    def fetchone(self):
        if "relation.relkind" in self.last_query:
            return (self.relkind,)
        if "SELECT EXISTS" in self.last_query:
            return (self.attached,)
        return None


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


def test_ensure_creator_partition_creates_list_partition_and_hnsw_index() -> None:
    cursor = FakeCursor(attached=False)
    repository = EmbeddingPartitionRepository(FakeDatabase(cursor))  # type: ignore[arg-type]

    name = repository.ensure_creator_partition(42)

    assert name == "fingerprint_embeddings_creator_42"
    combined_sql = "\n".join(query for query, _ in cursor.executed)
    assert "pg_advisory_xact_lock" in combined_sql
    assert "CREATE TABLE fingerprint_embeddings_creator_42" in combined_sql
    assert "FOR VALUES IN (42)" in combined_sql
    assert "CREATE INDEX IF NOT EXISTS fingerprint_embeddings_creator_42_hnsw_cos" in combined_sql
    assert "USING hnsw (embedding vector_cosine_ops)" in combined_sql


def test_ensure_creator_partition_is_idempotent_when_already_attached() -> None:
    cursor = FakeCursor(attached=True)
    repository = EmbeddingPartitionRepository(FakeDatabase(cursor))  # type: ignore[arg-type]

    repository.ensure_creator_partition(42)

    combined_sql = "\n".join(query for query, _ in cursor.executed)
    assert "CREATE TABLE fingerprint_embeddings_creator_42" not in combined_sql
    assert "CREATE INDEX IF NOT EXISTS fingerprint_embeddings_creator_42_hnsw_cos" in combined_sql


def test_ensure_creator_partition_requires_migrated_parent() -> None:
    repository = EmbeddingPartitionRepository(  # type: ignore[arg-type]
        FakeDatabase(FakeCursor(relkind="r"))
    )

    with pytest.raises(RuntimeError, match="not LIST-partitioned"):
        repository.ensure_creator_partition(42)


@pytest.mark.parametrize("creator_id", [0, -1])
def test_partition_name_rejects_non_positive_creator_id(creator_id: int) -> None:
    with pytest.raises(ValueError, match="positive"):
        EmbeddingPartitionRepository.partition_name(creator_id)
