# VPS deployment

The production stack is defined in `compose.production.yaml`. It runs:

- PostgreSQL with pgvector and a persistent database volume
- the public FastAPI API with the pinned local NMFP model
- the hybrid Twitch polling worker for the configured streamer
- a daily local VOD retention service
- the public React site
- Caddy for HTTPS at `vodhunter.dev` and `www.vodhunter.dev`

The admin API and Twitch EventSub are not deployed in this first rollout. The
worker uses Helix polling and defaults to a two-day scan window. The retention
service is initially configured for 30 days while the rollout is verified;
raise `VOD_RETENTION_DAYS` to 60 afterward.

## First deployment

Run these commands from the repository root on the VPS:

```bash
cp deploy/.env.example .env
chmod 600 .env
# Edit .env and fill the Twitch credentials and database password.
docker compose -f compose.production.yaml up -d --build
docker compose -f compose.production.yaml ps
docker compose -f compose.production.yaml logs -f worker
```

The `migrate` service runs `alembic upgrade head` after PostgreSQL is healthy.
The API, worker, and retention service do not start unless that migration
succeeds. The first database is expected to be empty; the NMFP migration
deliberately removes incompatible legacy fingerprints. The retention service
waits for its next scheduled run at 03:00 UTC and does not perform an ingest
backfill.

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
curl --fail https://vodhunter.dev/api/health
docker compose -f compose.production.yaml logs --tail=100 api worker
```

The API health response should report the pinned 128-dimensional NMFP runtime.
The worker should report `mode=watch` when `jasontheween` is offline and
`mode=live` when a stream is active.

## Updates

After a change is merged into `main`:

```bash
git pull --ff-only
docker compose -f compose.production.yaml up -d --build
```
