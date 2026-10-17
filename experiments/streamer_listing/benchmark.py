"""Compare the old and current streamer query using temporary PostgreSQL tables."""

from __future__ import annotations

import argparse
from contextlib import nullcontext
import json
import os
from pathlib import Path
import platform
import statistics
import sys
from time import perf_counter
from types import SimpleNamespace

import psycopg
from fastapi import FastAPI
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from backend.routers.search import router
from storage.records import SearchableStreamer
from storage.video_repository import VideoRepository


BASELINE_SQL = """
SELECT c.name, c.profile_image_url
FROM creators c
WHERE c.name IS NOT NULL AND BTRIM(c.name) <> ''
  AND EXISTS (
      SELECT 1
      FROM fingerprint_embeddings fe
      JOIN fingerprints f ON f.id = fe.fingerprint_id
      JOIN videos v ON v.id = f.video_id AND v.creator_id = fe.creator_id
      WHERE fe.creator_id = c.id
        AND v.status IN ('indexing', 'searchable')
  )
ORDER BY LOWER(c.name), c.name
"""
EXPECTED = [
    {"name": "jasontheween", "profile_image_url": None},
    {"name": "stableronaldo", "profile_image_url": None},
]


def seed_catalog(connection, fingerprints_per_video: int) -> None:
    connection.execute("CREATE TEMP TABLE creators(id bigint PRIMARY KEY, name text, profile_image_url text)")
    connection.execute("CREATE TEMP TABLE videos(id bigint PRIMARY KEY, creator_id bigint, status text)")
    connection.execute("CREATE INDEX ON videos(creator_id)")
    connection.execute("CREATE TEMP TABLE fingerprints(id bigint PRIMARY KEY, video_id bigint)")
    connection.execute("CREATE INDEX ON fingerprints(video_id)")
    connection.execute("""
        CREATE TEMP TABLE fingerprint_embeddings(
            creator_id bigint, fingerprint_id bigint,
            PRIMARY KEY(creator_id, fingerprint_id)
        ) PARTITION BY LIST(creator_id)
    """)
    for creator_id in (1, 35):
        connection.execute(f"""
            CREATE TEMP TABLE embeddings_{creator_id}
            PARTITION OF fingerprint_embeddings FOR VALUES IN ({creator_id})
        """)
    connection.execute("""
        INSERT INTO creators VALUES
        (1, 'jasontheween', NULL), (35, 'stableronaldo', NULL),
        (36, 'empty', NULL), (37, '', NULL)
    """)
    connection.execute("""
        INSERT INTO videos
        SELECT i, CASE WHEN i <= 14 THEN 1 ELSE 35 END, 'searchable'
        FROM generate_series(1, 28) i
    """)
    connection.execute("""
        INSERT INTO fingerprints
        SELECT i, 1 + ((i - 1) / %s) FROM generate_series(1, %s) i
    """, (fingerprints_per_video, fingerprints_per_video * 28))
    connection.execute("""
        INSERT INTO fingerprint_embeddings
        SELECT v.creator_id, f.id FROM fingerprints f JOIN videos v ON v.id = f.video_id
    """)
    for table in ("creators", "videos", "fingerprints", "fingerprint_embeddings", "embeddings_1", "embeddings_35"):
        connection.execute(f"ANALYZE {table}")


def summarize(samples: list[float]) -> dict:
    return {"median_ms": statistics.median(samples), "min_ms": min(samples), "max_ms": max(samples), "runs_ms": samples}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fingerprints-per-video", type=int, default=65_000)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.runs < 5 or args.fingerprints_per_video < 1:
        parser.error("use at least five runs and a positive fingerprint count")
    database_url = os.environ["VODHUNTER_TEST_DATABASE_URL"]
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute("SET statement_timeout = '30s'")
        seed_catalog(connection, args.fingerprints_per_video)
        database = SimpleNamespace(connect=lambda: nullcontext(connection))
        current = VideoRepository(database)

        class BaselineRepository:
            def list_searchable_streamers(self):
                return [SearchableStreamer(name=row[0], profile_image_url=row[1])
                        for row in connection.execute(BASELINE_SQL).fetchall()]

        captured = []
        cursor = SimpleNamespace(execute=lambda sql: captured.append(sql), fetchall=lambda: [])
        capture_database = SimpleNamespace(connect=lambda: nullcontext(SimpleNamespace(cursor=lambda: nullcontext(cursor))))
        VideoRepository(capture_database).list_searchable_streamers()
        queries = {"before": BASELINE_SQL, "after": captured[0]}
        plans = {name: connection.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql).fetchone()[0]
                 for name, sql in queries.items()}
        samples = {name: [] for name in queries}
        api_samples = {name: [] for name in queries}
        app = FastAPI()
        app.include_router(router)
        with TestClient(app) as client:
            for _ in range(args.runs):
                for name, repository in (("before", BaselineRepository()), ("after", current)):
                    started = perf_counter()
                    rows = connection.execute(queries[name]).fetchall()
                    samples[name].append((perf_counter() - started) * 1000)
                    assert rows == [("jasontheween", None), ("stableronaldo", None)], rows
                    app.state.videos = repository
                    started = perf_counter()
                    response = client.get("/api/search/streamers")
                    api_samples[name].append((perf_counter() - started) * 1000)
                    assert response.status_code == 200, response.text
                    assert response.json() == EXPECTED, response.text
        result = {
            "postgres": connection.execute("SELECT version()").fetchone()[0],
            "platform": platform.platform(), "logical_cpus": os.cpu_count(),
            "fingerprints": args.fingerprints_per_video * 28, "videos": 28,
            "creators": 4, "returned_streamers": 2, "failed_requests": 0,
            "scope": "Warm local PostgreSQL runs, uncached SQL and FastAPI TestClient requests. Temporary key-only tables preserve partitioning and indexes but omit vector payloads. API timings reuse a connection and exclude DNS, TLS, browser rendering and pgvector registration.",
            "sql": {name: summarize(values) for name, values in samples.items()},
            "api": {name: summarize(values) for name, values in api_samples.items()},
            "plans": plans,
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({key: result[key] for key in ("fingerprints", "failed_requests", "sql", "api")}, indent=2))


if __name__ == "__main__":
    main()
