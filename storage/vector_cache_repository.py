"""Discover and warm the HNSW indexes used by active embedding partitions."""

from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Callable

from storage.database import PostgresDatabase


ACTIVE_VECTOR_INDEXES_SQL = """
    SELECT
        partition.relid::oid,
        partition.relid::regclass::text,
        search_index.index_oid,
        search_index.index_name,
        search_index.size_bytes
    FROM pg_partition_tree('fingerprint_embeddings'::regclass) AS partition
    JOIN pg_attribute AS embedding_column
      ON embedding_column.attrelid = partition.relid
     AND embedding_column.attname = 'embedding'
     AND NOT embedding_column.attisdropped
    LEFT JOIN LATERAL (
        SELECT
            index_relation.oid AS index_oid,
            index_relation.oid::regclass::text AS index_name,
            pg_relation_size(index_relation.oid) AS size_bytes
        FROM pg_index AS index_metadata
        JOIN pg_class AS index_relation
          ON index_relation.oid = index_metadata.indexrelid
        JOIN pg_am AS access_method
          ON access_method.oid = index_relation.relam
         AND access_method.amname = 'hnsw'
        JOIN pg_opclass AS operator_class
          ON operator_class.oid = index_metadata.indclass[0]
         AND operator_class.opcname = 'vector_cosine_ops'
         AND operator_class.opcmethod = access_method.oid
        WHERE index_metadata.indrelid = partition.relid
          AND index_metadata.indisvalid
          AND index_metadata.indisready
          AND index_metadata.indislive
          AND index_metadata.indnkeyatts = 1
          AND index_metadata.indkey[0] = embedding_column.attnum
        ORDER BY index_relation.oid
    ) AS search_index ON TRUE
    WHERE partition.isleaf
    ORDER BY partition.relid, search_index.index_oid
"""


@dataclass(frozen=True, slots=True)
class VectorIndex:
    partition_name: str
    index_oid: int | None
    index_name: str | None
    size_bytes: int | None


@dataclass(frozen=True, slots=True)
class PrewarmResult:
    relation_name: str
    size_bytes: int
    blocks_loaded: int
    duration_seconds: float


def list_active_vector_indexes(cursor) -> list[VectorIndex]:
    cursor.execute(ACTIVE_VECTOR_INDEXES_SQL)
    return [
        VectorIndex(
            partition_name=str(row[1]),
            index_oid=int(row[2]) if row[2] is not None else None,
            index_name=str(row[3]) if row[3] is not None else None,
            size_bytes=int(row[4]) if row[4] is not None else None,
        )
        for row in cursor.fetchall()
    ]


def require_active_vector_indexes(indexes: list[VectorIndex]) -> None:
    missing = sorted({index.partition_name for index in indexes if index.index_oid is None})
    if missing:
        raise RuntimeError(
            "Active embedding partitions lack a valid, ready cosine HNSW index: "
            + ", ".join(missing)
        )


def prewarm_vector_indexes(
    database: PostgresDatabase,
    *,
    on_result: Callable[[PrewarmResult], None] | None = None,
) -> list[PrewarmResult]:
    """Warm active partition indexes and the fingerprint lookup index once."""

    results: list[PrewarmResult] = []
    with database.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_try_advisory_xact_lock(hashtextextended(%s, 0))",
                ("vodhunter:vector-index-prewarm",),
            )
            if not cursor.fetchone()[0]:
                raise RuntimeError("Another vector index prewarm is already running")

            indexes = list_active_vector_indexes(cursor)
            require_active_vector_indexes(indexes)
            relations = [
                (index.index_oid, index.index_name, index.size_bytes)
                for index in indexes
            ]
            cursor.execute(
                """
                SELECT index_relation.oid, index_relation.oid::regclass::text,
                       pg_relation_size(index_relation.oid)
                FROM pg_index AS index_metadata
                JOIN pg_class AS index_relation
                  ON index_relation.oid = index_metadata.indexrelid
                WHERE index_metadata.indrelid = 'fingerprints'::regclass
                  AND index_metadata.indisprimary
                  AND index_metadata.indisvalid
                  AND index_metadata.indisready
                  AND index_metadata.indislive
                """
            )
            primary_key = cursor.fetchone()
            if primary_key is None:
                raise RuntimeError("fingerprints has no valid primary-key index")
            relations.append(primary_key)

            for oid, name, size_bytes in relations:
                started_at = time.perf_counter()
                cursor.execute(
                    "SELECT pg_prewarm(%s::oid::regclass, 'buffer')",
                    (int(oid),),
                )
                result = PrewarmResult(
                    relation_name=str(name),
                    size_bytes=int(size_bytes),
                    blocks_loaded=int(cursor.fetchone()[0]),
                    duration_seconds=time.perf_counter() - started_at,
                )
                results.append(result)
                if on_result is not None:
                    on_result(result)
    return results
