# NMFP production operations

This runbook describes a fresh NMFP index build and the local query-inference runtime. It does not authorize or perform an external database creation, application deployment, or production data change.

## Release gates

Before shipping:

1. Confirm that ingest and query workers use the exact same immutable identity:
   - model `nmfp-triplet@15c6f3bcdf6a6da1daddfe47a1ffa5a0d22deadc+zenodo-15719945+ckpt-100`
   - preprocessing `nmfp-8khz-mono-1s-hop0.5-mel-v1`
   - `vector(128)`, 8 kHz mono, 1-second windows, 0.5-second hop
2. Use a newly created database or approve destructive rebuilding of a non-production index. There is no in-place conversion from incompatible legacy fingerprints.
3. Build the API image and verify that its pinned NMFP source, checkpoint, startup preload, and single-consumer query queue work in an approved non-production environment.

The product owner confirmed that the licensing decision for the AGPLv3 upstream code and separately distributed checkpoint is resolved for this migration. Packaging must still preserve applicable third-party notices and terms.

Changing the repository commit, checkpoint, configuration, sample rate, windowing, hop, or embedding dimension creates a new fingerprint identity. Update all pinned identifiers together and rebuild the complete index; never mix identities in one searchable index.

## Environment

Start with `.env.example`. Secret values belong only in the ignored `.env` or the deployment platform's secret store.

The alignment settings are deliberately configurable:

```text
SEARCH_TOP_K=10
NMFP_HOP_SECONDS=0.5
CUT_OFFSET_BIN_SECONDS=0.5
CUT_OFFSET_TOLERANCE_SECONDS=1.0
CUT_MAX_UNMATCHED_GAP_SECONDS=2.0
CUT_MIN_SUPPORT=6
CUT_MIN_SEGMENT_DURATION_SECONDS=4.0
CUT_MIN_DENSITY=0.4
CUT_MERGE_QUERY_GAP_SECONDS=1.0
CUT_MERGE_OFFSET_TOLERANCE_SECONDS=4.0
CUT_MAX_SEGMENTS=12
```

Tune these only with a representative evaluation set. Lowering support or duration thresholds increases the chance that isolated neighbors become false tracks. The aligner cannot guarantee matches for tiny sections, silence, completely overlaid source audio, or strongly transformed audio; it returns those portions in `unmatched_ranges` when evidence is insufficient.

## Fresh schema and index

Database creation is an operator-controlled external action. Once an approved empty target exists and `DATABASE_URL` points to it, inspect the target before migrating:

```bash
psql "$DATABASE_URL" -c 'select current_database(), current_user;'
alembic current
```

Apply the complete migration chain:

```bash
alembic upgrade head
```

The NMFP production revision intentionally performs a full index reset:

- deletes fingerprint embeddings, fingerprint timestamps, and saved VOD ingest cursors;
- changes the vector column to 128 dimensions;
- records non-null model and preprocessing versions;
- creates the cosine HNSW index and singleton fingerprint-index metadata;
- marks retained non-deleted videos `reindex_requested` so they are rebuilt from the start;
- adds durable JSON results and stage metrics to search jobs/logs.

Verify the result before starting a worker:

```bash
psql "$DATABASE_URL" -c 'select model_version, preprocessing_version, embedding_dim from fingerprint_index_metadata where singleton = true;'
psql "$DATABASE_URL" -c 'select count(*) as fingerprints from fingerprints;'
psql "$DATABASE_URL" -c 'select count(*) as saved_cursors from vod_ingest_state;'
psql "$DATABASE_URL" -c 'select status, count(*) from videos group by status order by status;'
```

Immediately after the migration, fingerprint and cursor counts must be zero. The metadata row must match the pinned identity above. Retained active videos must be `reindex_requested`; deleted videos stay deleted.

Application bootstrap calls the schema readiness check and refuses a vector width or metadata mismatch. Treat that refusal as an index-integrity error, not something to bypass.

## First full backfill

Use the guarded flag exactly once for the first backfill after migration:

```bash
python3 -m runners.run_backfill_ingest \
  --streamer STREAMER_LOGIN \
  --days DAYS_TO_INDEX \
  --fresh-nmfp-reindex
```

The guard performs a complete preflight before constructing the NMFP embedder. It accepts new VODs, migration-marked `reindex_requested` VODs, and intentionally deleted VODs. It fails closed if an existing active VOD is still `searchable`, has ambiguous legacy status, or is `indexing`; consequently the guarded run cannot silently skip an old index or resume a saved cursor. A `reindex_requested` row with an unexpected saved cursor is cleared and starts at zero.

Do not use the flag as a general retry switch. If the first run is interrupted, continue normally:

```bash
python3 -m runners.run_backfill_ingest \
  --streamer STREAMER_LOGIN \
  --days DAYS_TO_INDEX
```

Normal mode skips completed `searchable` VODs, resumes a partially indexed NMFP VOD from its saved cursor, and starts remaining `reindex_requested` VODs at zero. Restarting partially indexed work from zero without deleting its already-written fingerprints could duplicate data, which is why the guarded mode refuses `indexing` rows instead of resetting them.

After the initial rebuild, the long-running live/backlog worker is resumable:

```bash
python3 -m runners.run_hybrid_ingest \
  --streamer STREAMER_LOGIN \
  --days DAYS_TO_KEEP_CAUGHT_UP
```

Hybrid backlog selection also clears saved state for `reindex_requested` VODs before starting them. It preserves a cursor only for an `indexing` VOD, which represents interrupted work under the current index identity. Do not run the hybrid worker against a database that has not passed schema readiness.

## Backfill verification

During ingestion, `nmfp_ingest_extract` logs include video and chunk offsets, audio duration, cold-start status, model load, preprocessing, inference and total extraction times, fingerprint count, and both version identifiers.

After a run, verify:

```bash
psql "$DATABASE_URL" -c 'select status, count(*) from videos group by status order by status;'
psql "$DATABASE_URL" -c 'select model_version, preprocessing_version, count(*) from fingerprint_embeddings group by model_version, preprocessing_version;'
psql "$DATABASE_URL" -c 'select min(vector_dims(embedding)), max(vector_dims(embedding)) from fingerprint_embeddings;'
```

Expected searchable rows have only the pinned model/preprocessing pair and dimension 128. Investigate any failed VOD before declaring the backfill complete.

## Search result contract

Simple clients may continue to use `timestamp_seconds` and `video_url_at_timestamp`. Cut-aware clients should iterate `segments` and display `unmatched_ranges`. Tracks are scoped by both `video_id` and offset, so one edited query may map to multiple VODs.

```json
{
  "found": true,
  "timestamp_seconds": 20970,
  "video_url_at_timestamp": "https://www.twitch.tv/videos/123?t=5h49m30s",
  "segments": [
    {
      "query_start": 7.5,
      "query_end": 11.5,
      "video_id": 123,
      "vod_start": 20970.0,
      "vod_end": 20974.0,
      "video_url_at_timestamp": "https://www.twitch.tv/videos/123?t=5h49m30s",
      "score": 0.82
    }
  ],
  "unmatched_ranges": [
    {"query_start": 11.5, "query_end": 30.0}
  ]
}
```

An empty or partial segment list is valid. Never describe unmatched ranges as definitive non-occurrences.

## Latency and observability

The benchmark's roughly 231-240 ms median measured cached alignment with query fingerprints already present. It excluded cold TensorFlow startup and query fingerprint extraction. It is not comparable to upload-to-result latency.

Production emits and persists the following independently:

| Field | Meaning |
|---|---|
| `preprocess_duration_ms` | query audio normalization wall time |
| `embed_duration_ms` | local query fingerprint wall time, including queue wait |
| `model_cold_start` | whether this extraction loaded the NMFP model |
| `model_startup_duration_ms` | model construction/checkpoint load on a cold worker; zero on a warm worker |
| `fingerprint_preprocessing_duration_ms` | NMFP feature preprocessing inside the worker |
| `fingerprint_inference_duration_ms` | neural fingerprint inference inside the worker |
| `fingerprint_duration_ms` | worker extraction total |
| `vector_query_duration_ms` | batched pgvector candidate retrieval |
| `alignment_duration_ms` | multi-video cut-track construction |
| `total_duration_ms` | end-to-end request/job handling measured by the API path |
| `query_fingerprint_count` | number of query fingerprints |
| `candidate_count` | candidate rows retained for alignment |
| `segment_count` | supported segments returned |
| `model_version`, `preprocessing_version` | identity used for this search |
| `result_payload` | durable primary result, segments, and unmatched ranges |

For production baselines, report separate cold and warm distributions. Cold measurement must include `model_cold_start=true`, model startup, feature preprocessing, inference, vector retrieval, alignment, and total time. Warm query extraction must use `model_cold_start=false`; do not omit extraction time just because the model is resident.

## Tests and rollout order

Run focused operational tests and then the complete suites:

```bash
python3 -m pytest tests/test_run_backfill_ingest.py tests/test_run_hybrid_ingest.py
python3 -m pytest
(cd web-public && npm test && npm run build)
(cd web-admin && npm test && npm run build)
```

Recommended rollout order:

1. Build and smoke-test the API with its preloaded pinned model without publishing production traffic.
2. Create or select an explicitly approved empty non-production database.
3. Apply migrations and verify index metadata/cursor counts.
4. Run the guarded first backfill and inspect version/dimension counts.
5. Exercise cold and warm searches, edited multi-VOD queries, isolated-candidate rejection, and unmatched ranges.
6. Load-test vector retrieval and inspect HNSW recall/latency with production-like data.
7. Obtain explicit approval before any production database creation, application deployment, or traffic cutover.

Because incompatible fingerprints are deliberately deleted, rollback of application code does not restore the previous index. Recovery means selecting a compatible database backup or rebuilding the desired fingerprint identity from source VODs.
