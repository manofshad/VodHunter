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
| 00 | User Experience Overview | Loki, PostgreSQL |
| 10 | Search Performance | Prometheus, Loki |
| 10 | Search Quality | PostgreSQL |
| 12 | Search Reliability | PostgreSQL |
| 20 | Streamers & VODs | PostgreSQL |
| 20 | Ingestion Operations | PostgreSQL, Loki |
| 30 | Index & Retention | PostgreSQL, Loki |
| 40 | Monitoring Health | Prometheus, Loki, PostgreSQL |

All dashboards carry the `vodhunter` tag and expose a dashboard-link dropdown.
Production uses the existing `VodHunter` folder. Preserve stable dashboard UIDs
when updating; numeric prefixes organize the suite. The Browser experience link
opens the registered `vodhunter-public` Frontend Observability application.

The dashboards are generated so common datasource definitions and panel
defaults remain consistent. After editing
`observability/grafana/generate_dashboards.py`, regenerate and verify them with:

```sh
python3 observability/grafana/generate_dashboards.py
python3 observability/grafana/generate_dashboards.py --check
```

## Manage dashboards with gcx

Authenticate with `gcx login vodhunter-production --server
https://vodhunter.grafana.net --oauth`. Credentials belong in the local Keychain,
not this repository. Discover datasource and folder UIDs before export:

```sh
gcx --context vodhunter-production config check
gcx --context vodhunter-production datasources list
gcx --context vodhunter-production resources get folders
```

Bind portable datasource inputs to verified instance UIDs and generate API
resource manifests into a private directory outside the checkout:

```sh
python3 observability/grafana/export_resources.py \
  --metrics-uid METRICS_UID --logs-uid LOGS_UID --postgres-uid POSTGRES_UID \
  --folder-uid FOLDER_UID --namespace STACK_NAMESPACE --output /private/tmp/vodhunter-dashboards
gcx --context vodhunter-production resources validate -p /private/tmp/vodhunter-dashboards
gcx --context vodhunter-production resources push -p /private/tmp/vodhunter-dashboards --dry-run
gcx --context vodhunter-production resources push -p /private/tmp/vodhunter-dashboards
```

Export the original dashboards before replacing them and render snapshots after
upload. On this stack the application datasources are `grafanacloud-prom` and
`grafanacloud-logs`; billing metrics and alert-state history do not contain
VodHunter application telemetry. Always rediscover UIDs before writing to a
different stack. SQL inventory panels represent current state; search cohorts
respect the time picker. Missing latency samples stay unknown.

## Browser experience and rollout

The public frontend uses Grafana Faro for web vitals, error stack locations,
anonymous session lifecycle, and explicit search events. The registered
`vodhunter-public` collector accepts the canonical `https://vodhunter.com`
origin. The collector address is public and carries no administrative token.
Production Compose embeds it at build time; standalone/local builds leave
collection off unless configured. Development mode, Do Not Track, Global
Privacy Control, or `VITE_FARO_URL=disabled` also disable collection.

The telemetry boundary removes query strings, fragments, dynamic page IDs,
arbitrary input, user metadata, console logs, network traces, exception text,
external stack URLs, and web-vital DOM attribution. It retains finite outcomes,
stages, failure categories, random attempt IDs, server search IDs, and timings.
No session replay or tracing instrumentation is installed. Session collection
uses the SDK's normal nonpersistent mode and 100% sampling, subject to browser
blocking and privacy choices; observed sessions are not unique people.

Events include page ready/streamer-list failure, submit attempts, client
validation blocks, job acceptance, submission/poll failures, stage changes,
terminal results committed to the UI, resume/shared-link journeys, visibility
changes while waiting, result-source/segment clicks, history opens, date-filter
usage, and clipboard failures. Result clicks indicate engagement, not Twitch
playback or verified correctness. A hidden view does not establish abandonment.
Accepted jobs that stop polling can still finish on the backend.

Faro requests are bounded and best-effort; search behavior is independent of
collector availability. Set `SOURCE_COMMIT` to the actual deployed SHA for
release comparisons; a build with no SHA is explicitly `unversioned`.

Backend additions expose `vodhunter_http_requests_total`,
`vodhunter_http_request_duration_seconds`, `vodhunter_search_submissions_total`,
and `vodhunter_search_queue_wait_seconds`. HTTP polling counts as requests,
never submissions; routes use templates, not raw job paths. Queue measurement
includes job creation through executor start. Edge Nginx rejections occur
before the API and are not included in API submission counters. Existing
outcome counters initialize to zero, while unsampled latency remains unknown.

Merge and deploy the instrumentation through main/Coolify before expecting new
Faro events or backend metrics. The live SQL-based dashboard improvements do
not require that deployment. Then exercise match, no-match, validation error,
rejected submit, failed status fetch, reload/shared result, and result clicks;
verify sanitized collector payloads and actual Loki fields before building
custom funnel queries. Grafana's native Frontend views are ready to consume
the SDK measurements. All essential journey events use the same attempt ID,
and resumed/shared journeys have separate entry kinds.

Follow-on work: independently deployed VPS/container/database exporters,
external synthetic probes, worker heartbeat and discovery-to-searchable lag,
retention capacity history, exact frontend funnel/cohort queries, and baseline
SLOs/notification rules. API scrape up is not a substitute for a public probe;
quiet logs are not heartbeats. Choose notification destinations and traffic
thresholds before enabling alerts.

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
