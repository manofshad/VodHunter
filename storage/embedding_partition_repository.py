"""Provision creator-scoped partitions for fingerprint embeddings."""

from __future__ import annotations

from storage.database import PostgresDatabase


class EmbeddingPartitionRepository:
    """Create the LIST partition and HNSW index for one approved creator."""

    def __init__(self, database: PostgresDatabase) -> None:
        self.database = database

    @staticmethod
    def partition_name(creator_id: int) -> str:
        normalized = int(creator_id)
        if normalized <= 0:
            raise ValueError("creator_id must be positive")
        return f"fingerprint_embeddings_creator_{normalized}"

    def ensure_creator_partition(self, creator_id: int) -> str:
        normalized = int(creator_id)
        partition_name = self.partition_name(normalized)
        index_name = f"{partition_name}_hnsw_cos"

        with self.database.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"vodhunter:embedding-partition:{normalized}",),
                )
                cur.execute(
                    """
                    SELECT relation.relkind
                    FROM pg_class AS relation
                    JOIN pg_namespace AS namespace
                      ON namespace.oid = relation.relnamespace
                    WHERE namespace.nspname = current_schema()
                      AND relation.relname = 'fingerprint_embeddings'
                    LIMIT 1
                    """
                )
                parent_row = cur.fetchone()
                if parent_row is None or str(parent_row[0]) != "p":
                    raise RuntimeError(
                        "fingerprint_embeddings is not LIST-partitioned; run Alembic migrations"
                    )

                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_inherits AS inheritance
                        JOIN pg_class AS parent
                          ON parent.oid = inheritance.inhparent
                        JOIN pg_class AS child
                          ON child.oid = inheritance.inhrelid
                        JOIN pg_namespace AS namespace
                          ON namespace.oid = parent.relnamespace
                        WHERE namespace.nspname = current_schema()
                          AND parent.relname = 'fingerprint_embeddings'
                          AND child.relname = %s
                    )
                    """,
                    (partition_name,),
                )
                attached_row = cur.fetchone()
                if not attached_row or not bool(attached_row[0]):
                    cur.execute(
                        f"""
                        CREATE TABLE {partition_name}
                        PARTITION OF fingerprint_embeddings
                        FOR VALUES IN ({normalized})
                        """
                    )

                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS {index_name}
                    ON {partition_name}
                    USING hnsw (embedding vector_cosine_ops)
                    """
                )

        return partition_name
