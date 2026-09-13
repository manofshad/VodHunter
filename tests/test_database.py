from __future__ import annotations

import pytest

from storage.database import PostgresDatabase


def test_postgres_database_uses_code_owned_iterative_hnsw_defaults() -> None:
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
        ({"hnsw_scan_mem_multiplier": float("nan")}, "hnsw_scan_mem_multiplier"),
    ],
)
def test_postgres_database_rejects_invalid_hnsw_settings(
    kwargs: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        PostgresDatabase("postgresql://example/vodhunter", **kwargs)
