# VPS deployment

The production stack is defined in `compose.production.yaml`. It runs:

- PostgreSQL with pgvector and a persistent database volume
- the public FastAPI API with the pinned local NMFP model
- the hybrid Twitch polling worker for the configured streamer
- the public React site
- Caddy for HTTPS at `vodhunter.dev` and `www.vodhunter.dev`

The admin API and Twitch EventSub are not deployed in this first rollout. The
worker uses Helix polling and defaults to a two-day scan window. The 60-day
retention job is intentionally a later change.

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
The API and worker do not start unless that migration succeeds. The first
database is expected to be empty; the NMFP migration deliberately removes
incompatible legacy fingerprints.

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
