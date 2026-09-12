# VodHunter

[vodhunter.com](https://vodhunter.com/) searches short-form audio against Twitch VODs. The production search path uses NMFP neural audio fingerprints and can map an edited query to multiple, possibly non-contiguous VOD ranges.

## Production architecture

The ingest and query paths share one immutable fingerprint identity:

- model: `nmfp-triplet@15c6f3bcdf6a6da1daddfe47a1ffa5a0d22deadc+zenodo-15719945+ckpt-100`
- preprocessing: `nmfp-8khz-mono-1s-hop0.5-mel-v1`
- output width: 128 dimensions
- audio: 8 kHz mono, 1-second windows, 0.5-second fingerprint hop

The service rejects a database, worker response, or local runtime whose dimensions or version identifiers differ. Do not change any part of this identity in isolation; a different checkpoint or preprocessing contract requires a new index and a full reindex.

```mermaid
flowchart LR
    VOD["Twitch VOD"] --> Extract["ffmpeg: 8 kHz mono"]
    Extract --> IngestNMFP["persistent NMFP ingest model"]
    IngestNMFP --> Vectors["Postgres + pgvector(128)"]

    Query["TikTok clip"] --> Normalize["ffmpeg normalization"]
    Normalize --> Queue["single-consumer local NMFP queue"]
    Queue --> LocalNMFP["preloaded backend NMFP model"]
    LocalNMFP --> Candidates["top-k neighbors per fingerprint"]
    Vectors --> Candidates
    Candidates --> Align["video + stable-offset track alignment"]
    Align --> Result["primary timestamp + segments + unmatched ranges"]
```

Ingestion resolves VOD media with `yt-dlp`, extracts overlapping audio chunks, fingerprints them locally, and stores the timestamped vectors with the model and preprocessing versions. The API preloads the same pinned NMFP model during startup. Search normalizes each query and submits only fingerprint extraction to a single-consumer local queue; downloads, FFmpeg normalization, vector retrieval, and alignment remain independently concurrent. The resulting timestamped fingerprints retrieve the top 10 candidates for every query fingerprint and are aligned by both video ID and stable `VOD time - query time` offset.

The public endpoint is asynchronous: `POST /api/search/clip` creates a job and `GET /api/search/clip/{search_id}` returns its state and durable result. A successful result retains the legacy top-level timestamp/URL while adding `segments` and `unmatched_ranges`.

NMFP only reports ranges with enough consistent evidence. Very short sections, fully overlaid audio, silence, heavy transformation, or isolated nearest neighbors can remain unmatched. An unmatched range is an honest lack of support, not proof that the source audio never occurred in a VOD.

## Cut-aware alignment defaults

Alignment thresholds are code-owned defaults in
`search/alignment_service.py`. The current defaults are 10 neighbors per
fingerprint, a 0.5-second fingerprint hop, a 0.5-second offset bin, +/-1
second offset tolerance, a 2-second unsupported-gap limit, six-fingerprint
minimum support, a 4-second minimum segment, 0.4 minimum density, a 1-second
merge gap, a 4-second merge offset tolerance, and at most 12 returned
segments. Treat these as tuned defaults, not guarantees; evaluate code changes
against representative edits before rollout.

## Setup and operations

Copy `.env.example` to an ignored `.env` and fill in deployment values and
secrets locally. NMFP's model, preprocessing, sample-rate, window, hop, and
vector-dimension identity is pinned in code; only the repository and model
configuration paths vary by environment. The production API uses Python 3.11
and installs the TensorFlow/Essentia NMFP runtime from the backend requirements
files. The pinned upstream repository and checkpoint must be present before
startup; the public Docker image bakes them in and verifies their immutable
identities.

For the self-hosted VPS stack, see [VPS deployment](docs/vps-deployment.md). It uses a standalone Coolify PostgreSQL/pgvector resource alongside the public API, polling worker, and public site. Production HTTPS and public routing are supplied by the hosting platform.

The stack also includes a separate daily VOD retention service. Both the
retention setting (`VOD_RETENTION_DAYS`) and the worker's independent
`HYBRID_INGEST_DAYS` setting default to 30 days, keeping ingestion aligned with
the retained search history.

The production schema migration is destructive to incompatible fingerprint data by design. The old production database no longer exists, so rollout assumes a fresh database or a complete rebuild rather than a zero-downtime vector conversion. Apply migrations and run the guarded first backfill as described in [NMFP production operations](docs/nmfp-production-operations.md). That guide also covers resumability, version checks, metrics, and rollback boundaries.

The focused Grafana Cloud search telemetry setup, Alloy configuration, secret
names, dashboard, and smoke-test queries are documented in
[search observability](observability/README.md).

No application deployment or external database creation is performed by repository commands unless an operator explicitly runs the relevant external tooling.

## Latency measurements

The experiment's roughly 231-240 ms median was cached NMFP alignment using already-extracted query fingerprints. It excluded TensorFlow/container startup and query fingerprint extraction, so it is not end-to-end production latency.

Production records audio normalization, cold model startup, fingerprint preprocessing/inference/total extraction, vector retrieval, cut alignment, and total request latency separately. Compare cold requests with cold requests and warm requests with warm requests; do not present the cached experiment number as clip-to-result latency.

## Testing

Run the Python suite with:

```bash
python3 -m pytest -m "not integration"
```

The public frontend tests and production build can be run with:

```bash
(cd web-public && npm ci && npm test && npm run build)
```

Integration tests use a disposable PostgreSQL/pgvector database. Start the local
test database with:

```bash
docker compose -f compose.test.yaml up -d
DATABASE_URL=postgresql://vodhunter:vodhunter@localhost:55432/vodhunter_test alembic upgrade head
VODHUNTER_TEST_DATABASE_URL=postgresql://vodhunter:vodhunter@localhost:55432/vodhunter_test python3 -m pytest -m integration
docker compose -f compose.test.yaml down
```

The production stack is never used by these tests.

## Third-party licensing status

The upstream `neural-music-fp` implementation used by NMFP is identified as AGPLv3. The product owner confirmed that the production licensing decision is resolved for this migration. Preserve the upstream notices and the separately distributed checkpoint terms when packaging the worker.

## License

VodHunter's own source is licensed under the MIT License.
