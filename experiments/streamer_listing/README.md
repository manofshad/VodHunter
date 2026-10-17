# Streamer listing query

The first visit waits for `/api/search/streamers`. Browser caching helps later
visits, but an uncached request still executes the catalog query. The same query
also validates streamer selection when a search starts.

The old `EXISTS` allows PostgreSQL to flatten the embedding, fingerprint, and
video joins. In the local reproduction, the plan scans all 1.82 million
fingerprints and all 1.82 million embeddings, hashes the joins, and aggregates
the result before returning two streamers. Its top node writes 15,008 temporary
blocks. The number of returned streamers does not bound that work.

The replacement starts with each creator's eligible videos. Nested lateral
queries with `LIMIT 1` preserve the early exits. The fingerprint lookup uses the
video index, and the embedding lookup uses the creator partition and primary
key. The measured plan reads two fingerprint entries and two embedding entries.
It still requires an actual embedding on an `indexing` or `searchable` video
owned by the creator.

## Measurements

On October 16, 2026, the public endpoint returned HTTP 200 with two streamers in
3.45 seconds on one request with `Cache-Control: no-cache`. This is one network
observation, not a controlled before/after comparison. Production statistics
reported about 1.83 million embeddings, 28 videos, and four creators. The Grafana
read role cannot access the base tables, so the live execution plan was
unavailable.

The controlled comparison uses PostgreSQL 18.1 on Windows with 1.82 million
fingerprints and embeddings, 28 videos, four creators, and two returned
streamers. Five runs alternate the old and current queries. Every SQL result and
every HTTP response must match the expected two streamers. All requests passed.

| Local measurement | Before median | After median | Before range | After range |
| --- | ---: | ---: | ---: | ---: |
| SQL round trip | 1683.32 ms | 0.85 ms | 1648.00–1799.07 ms | 0.40–0.95 ms |
| FastAPI request | 1682.99 ms | 1.42 ms | 1667.20–1735.96 ms | 1.06–1.60 ms |

The API median drops by 1681.57 ms in this local comparison. The old query's
catalog scans and hash spill dominate the request. Removing those scans explains
the change. The remaining local API time includes SQL round trips, Python
request dispatch, and response serialization.

These are warm-buffer, uncached-query measurements. The temporary tables retain
the relevant keys, partitions, and indexes but omit vector payloads and foreign
keys. API requests reuse a database connection and exclude DNS, TLS, browser
rendering, and pgvector registration. Local Python and PostgreSQL versions also
differ from the pinned deployment versions. The samples alternate sides to
reduce machine-load bias; the host CPU load counter was unavailable. The result
establishes a query improvement in this reproduction, not deployed page-load
latency. No deployment or Tailscale connection was used.

[results.json](results.json) contains the individual timings and plan summaries.

## Reproduce

Install the repository's development requirements and set
`VODHUNTER_TEST_DATABASE_URL` to a scratch PostgreSQL database. The benchmark
creates only connection-local temporary tables and drops them when it exits.
It does not require pgvector because neither query reads the vector payload.

```powershell
python experiments/streamer_listing/benchmark.py --output tmp/streamer-listing-results.json
python -m pytest tests/test_video_repository.py tests/test_search_api_endpoint.py tests/integration/test_streamer_listing.py -q
```

The benchmark saves full `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` plans separately
from the timed SQL and API runs. It exercises the current repository method and
the real FastAPI router. The regression tests cover status eligibility, creator
ownership, missing embeddings, later eligible videos, duplicate prevention,
names, ordering, and avatars.
