from __future__ import annotations

import pytest

from storage.database import PostgresDatabase
from storage import repositories as repositories_module


def test_postgres_database_defaults_to_strict_iterative_hnsw_scans() -> None:
    database = PostgresDatabase("postgresql://example/vodhunter")

    assert database.hnsw_ef_search == 100
    assert database.hnsw_iterative_scan == "strict_order"
    assert database.hnsw_max_scan_tuples == 20_000
    assert database.hnsw_scan_mem_multiplier == 1.0


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"hnsw_iterative_scan": "invalid"}, "hnsw_iterative_scan"),
        ({"hnsw_max_scan_tuples": 0}, "hnsw_max_scan_tuples"),
        ({"hnsw_scan_mem_multiplier": 0}, "hnsw_scan_mem_multiplier"),
    ],
)
def test_postgres_database_rejects_invalid_hnsw_settings(
    kwargs: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        PostgresDatabase("postgresql://example/vodhunter", **kwargs)


def test_build_repositories_reads_hnsw_settings_from_environment(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeDatabase:
        def __init__(self, database_url: str, **kwargs: object):
            captured["database_url"] = database_url
            captured.update(kwargs)

        def ensure_schema_ready(self) -> None:
            return None

    monkeypatch.setattr(repositories_module, "PostgresDatabase", FakeDatabase)
    monkeypatch.setenv("HNSW_EF_SEARCH", "160")
    monkeypatch.setenv("HNSW_ITERATIVE_SCAN", "relaxed_order")
    monkeypatch.setenv("HNSW_MAX_SCAN_TUPLES", "32000")
    monkeypatch.setenv("HNSW_SCAN_MEM_MULTIPLIER", "2")

    result = repositories_module.build_repositories("postgresql://example/vodhunter")

    assert result.database.__class__ is FakeDatabase
    assert captured == {
        "database_url": "postgresql://example/vodhunter",
        "hnsw_ef_search": 160,
        "hnsw_iterative_scan": "relaxed_order",
        "hnsw_max_scan_tuples": 32_000,
        "hnsw_scan_mem_multiplier": 2.0,
    }
