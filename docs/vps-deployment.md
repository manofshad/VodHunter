# VPS deployment

Production is split across two Coolify resources:

- a standalone PostgreSQL/PGVector database with its own persistent storage,
  health check, backup schedule, and lifecycle;
- the Git-based Docker Compose application in `compose.production.yaml`, which
  runs the public FastAPI API with the pinned local NMFP model, the hybrid Twitch
  polling worker, the VOD retention service, the Grafana Alloy telemetry sidecar,
  and the public React site.

The Coolify-managed Traefik proxy serves HTTPS at `vodhunter.com` and
`www.vodhunter.com`. PostgreSQL is intentionally not part of the application
Compose stack, so routine application deployments do not restart the database
or discard its hot buffer working set.

The worker uses Twitch Helix polling and defaults to a 30-day scan window. The
retention service is configured for the same 30-day window, so the worker can
catch up the full retained history without leaving a one-day boundary gap.

## Standalone database

Create the database before deploying the application. In the same Coolify
project, environment, server, and destination:

1. Create a PostgreSQL resource with the PGVector image and PostgreSQL 16.
2. Use a generated database password and keep the resource private; do not
   publish port 5432.
3. Configure persistent storage at the image's PostgreSQL data path.
4. Configure scheduled off-server backups before the first production cutover.
5. Apply the settings in
   [`deploy/postgresql.production.conf.example`](../deploy/postgresql.production.conf.example).
6. Start the resource and copy its generated Internal URL.

The application must use the database resource's full generated hostname in
`DATABASE_URL`; `db` is only a valid hostname for services in the same Compose
stack. Enable the application's **Connect to Predefined Network** setting so
the application and standalone database can communicate over Coolify's private
network. Do not expose the database publicly.

The `pg_prewarm` extension is installed by the latest Alembic migration. After
the first restore, warm the large HNSW index before opening traffic:

```sql
SELECT pg_prewarm('idx_fingerprint_embeddings_hnsw_cos', 'buffer');
SELECT pg_prewarm('fingerprints_pkey', 'buffer');
```

## First deployment

The production stack is deployed from this repository by self-hosted Coolify.
Create a Git-based Docker Compose application for `compose.production.yaml`,
select the `main` branch, and assign both
`https://vodhunter.com` and `https://www.vodhunter.com` to the `web-public`
service on port 80. Coolify's Traefik proxy terminates HTTPS and routes those
domains to the service. The `web-public` Nginx configuration continues to route
`/api/` requests to the internal `api:8000` service.

Populate the production environment variables in Coolify before the first
deployment. Do not commit the production `.env` file or database credentials to
GitHub. `DATABASE_URL` must be the standalone database's Internal URL. The
application Compose stack owns the `runtime_data` and `alloy_data` volumes;
database storage belongs to the standalone Coolify resource.

The Alloy sidecar additionally requires the five `GRAFANA_CLOUD_*` variables
and `VODHUNTER_ENVIRONMENT` shown in `deploy/.env.example`. Keep the access
token in Coolify's secret store. Alloy has no public route; it scrapes the API
over the private Compose network and tails only the API container's Docker
logs.

The `migrate` service runs `alembic upgrade head` against the standalone
database. The API, worker, and retention service do not start unless that
migration succeeds. The first database is expected to be empty; the NMFP
migration deliberately removes incompatible legacy fingerprints. The retention
service waits for its next scheduled run at 03:00 UTC and does not perform an
ingest backfill.

## Existing-database cutover

Before moving an existing installation, take and verify a custom-format dump
from the currently running Coolify database container. Resolve the active
container and volume from `docker ps` and `docker inspect`; do not start a
stopped `vodhunter-db-1` or assume that the source Compose volume name is the
live volume. Coolify prefixes Compose-managed volume names with the application
resource identifier.

Restore the dump into the standalone database and verify the schema, row counts,
fingerprint-index metadata, HNSW index, and representative searches. Measure
the restore and index-build duration before scheduling the final maintenance
window. During that window, stop API, worker, and retention writers, take a
final dump, restore it into the standalone database, run migrations and
`ANALYZE`, prewarm the HNSW index, update `DATABASE_URL`, and validate health
and searches before reopening traffic.

Keep the original database volume untouched until standalone backups and a
restore have been tested successfully. Never run Docker volume cleanup as part
of this migration.

To run one retention pass manually, use the explicit one-shot mode. This is a
real deletion pass, so inspect the configured database and retention value
before running it:

```bash
docker compose -f compose.production.yaml run --rm --no-deps vod-retention python -m runners.run_vod_retention --once
docker compose -f compose.production.yaml logs --tail=100 vod-retention
```

After verifying the 30-day behavior, edit `.env` to set
`VOD_RETENTION_DAYS=60` and recreate only the retention service:

```bash
docker compose -f compose.production.yaml up -d --force-recreate vod-retention
```

## Checks

```bash
curl --fail https://vodhunter.com/api/health
docker compose -f compose.production.yaml logs --tail=100 api worker
docker compose -f compose.production.yaml logs --tail=100 alloy
```

The API health response should report the pinned 128-dimensional NMFP runtime.
The worker should report `mode=watch` when `jasontheween` is offline and
`mode=live` when a stream is active.

## Updates

Coolify is configured to auto-deploy the `main` branch after changes are
merged. Application deployments should restart only the application resource;
the standalone database should retain its container uptime and cache. Review
the deployment logs, health checks, and database uptime after each deployment.
Manual redeploys remain available from the Coolify dashboard when an operator
needs to replay a deployment.
