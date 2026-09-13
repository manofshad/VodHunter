# VodHunter observability

VodHunter sends API metrics plus API, ingestion-worker, and retention logs to
Grafana Cloud through the Alloy sidecar. PostgreSQL reporting views provide the
current inventory and durable search-quality data that should not be encoded as
high-cardinality Prometheus labels.

VPS CPU, RAM, disk, network, and per-container resource monitoring are
intentionally outside this application dashboard suite. They should be added as
a separately deployed host-monitoring integration so monitoring survives an
application deployment failure.

## Dashboard suite

The versioned exports in `observability/grafana` are ordered by operational
scope:

| Order | Dashboard | Primary data |
| --- | --- | --- |
| 00 | VodHunter Overview | Prometheus, Loki, PostgreSQL |
| 10 | Search Performance | Prometheus, Loki |
| 10 | Search Quality | PostgreSQL |
| 20 | Streamers & VODs | PostgreSQL |
| 20 | Ingestion Operations | PostgreSQL, Loki |
| 30 | Index & Retention | PostgreSQL, Loki |

All dashboards carry the `vodhunter` tag and expose a dashboard-link dropdown.
Create matching Grafana folders named `00 Overview`, `10 Search`, `20 Content`,
and `30 Operations`, then place each imported dashboard according to its numeric
prefix. Folder placement is Grafana instance state and is not part of a portable
dashboard JSON export.

The dashboards are generated so common datasource definitions and panel
defaults remain consistent. After editing
`observability/grafana/generate_dashboards.py`, regenerate and verify them with:

```sh
python3 observability/grafana/generate_dashboards.py
python3 observability/grafana/generate_dashboards.py --check
```

## Grafana Cloud values

Create or use a Grafana Cloud access policy with `metrics:write` and
`logs:write`. Put the following values in the VPS/Coolify environment, not in
the repository:

```text
GRAFANA_CLOUD_PROMETHEUS_URL
GRAFANA_CLOUD_PROMETHEUS_USER
GRAFANA_CLOUD_LOKI_URL
GRAFANA_CLOUD_LOKI_USER
GRAFANA_CLOUD_API_TOKEN
VODHUNTER_ENVIRONMENT=production
```

The same token can be used for both endpoints when the access policy grants
both scopes. Grafana Cloud provides the endpoint URLs and user IDs in the
Prometheus and Loki connection details.

## PostgreSQL reporting datasource

Alembic revision `20260913_0014` creates these stable reporting views:

- `grafana_streamer_summary`
- `grafana_vod_inventory`
- `grafana_search_quality`
- `grafana_index_partitions`
- `grafana_retention_inventory`

Use a dedicated login that can read only these views. Run equivalent statements
as a database administrator, substituting a generated password and the actual
database name:

```sql
CREATE ROLE vodhunter_grafana LOGIN PASSWORD '<generated-password>';
ALTER ROLE vodhunter_grafana SET default_transaction_read_only = on;
ALTER ROLE vodhunter_grafana SET statement_timeout = '15s';
GRANT CONNECT ON DATABASE vodhunter TO vodhunter_grafana;
GRANT USAGE ON SCHEMA public TO vodhunter_grafana;
GRANT SELECT ON
    grafana_streamer_summary,
    grafana_vod_inventory,
    grafana_search_quality,
    grafana_index_partitions,
    grafana_retention_inventory
TO vodhunter_grafana;
```

Do not grant the dashboard user access to `search_requests` or other application
tables. The reporting views deliberately omit TikTok URLs, clip filenames,
download hosts, and error-message text.

The production database is private. Connect Grafana Cloud with a private
datasource connection rather than publishing PostgreSQL port 5432. Configure a
PostgreSQL datasource named for VodHunter, point it at the production database,
enable TLS as required by the database resource, and use the read-only login.
When importing a dashboard, map `DS_POSTGRES` to this datasource,
`DS_METRICS` to the Grafana Cloud Prometheus datasource, and `DS_LOGS` to Loki.

## VOD status semantics

`videos.status` is the authoritative lifecycle field:

- `searchable`: the VOD completed ingestion;
- `indexing` with a cursor updated in the last ten minutes: in progress;
- `indexing` with an older or missing cursor: stalled;
- `reindex_requested`: queued for a complete rebuild;
- `deleted`: intentionally excluded from search.

Completed VODs display 100 percent even though they have no
`vod_ingest_state` row. Successful finalization deletes the resumable cursor.
The legacy `processed` boolean is intentionally not exposed because statuses
such as `deleted` and `reindex_requested` also map to processed.

Fingerprint counts on the Index & Retention dashboard are PostgreSQL planner
estimates from partition statistics. This avoids an expensive full count of the
embedding index on every dashboard refresh. Run `ANALYZE fingerprint_embeddings`
after unusually large backfills if the estimates need refreshing.

## Deploy

The normal deployment runs `alembic upgrade head`, which installs the reporting
views. To update Alloy after deployment:

```sh
docker compose -f compose.production.yaml config
docker compose -f compose.production.yaml up -d alloy
docker compose -f compose.production.yaml ps alloy
```

Alloy scrapes `api:8000/internal/metrics` on the private Compose network. It
tails the `api`, `worker`, and `vod-retention` containers through the Docker
socket. The API metrics path remains blocked at the public Nginx edge.

The API emits JSON logs. Alloy promotes bounded `level`, `event`, and `outcome`
values to Loki labels and keeps identifiers as structured metadata. The worker
emits bounded key/value operational messages; Alloy labels only action, mode,
and event while keeping streamer, VOD ID, progress, and backlog fields as
structured metadata. Retention logs are handled similarly.

The Docker socket mount is required by `loki.source.docker`; treat the Alloy
container as a host-operations component and keep its changes reviewed.

## Smoke-test queries

PromQL:

```promql
sum by (outcome) (increase(vodhunter_searches_total[$__range]))
histogram_quantile(0.95, sum by (le) (rate(vodhunter_search_duration_seconds_bucket[$__rate_interval])))
histogram_quantile(0.95, sum by (le, stage) (rate(vodhunter_search_stage_duration_seconds_bucket[$__rate_interval])))
```

LogQL:

```logql
{service_name="vodhunter-api", event="search_finished"} | json
{service_name="vodhunter-worker"}
{service_name="vodhunter-worker", action="failed"}
{service_name="vodhunter-retention"}
```

PostgreSQL:

```sql
SELECT * FROM grafana_streamer_summary ORDER BY streamer;
SELECT * FROM grafana_vod_inventory ORDER BY streamed_at DESC LIMIT 20;
SELECT * FROM grafana_index_partitions ORDER BY streamer;
```
