# VodHunter search observability

This first slice sends public-search metrics and API logs from the production
Compose stack to Grafana Cloud through the Alloy sidecar.

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

## Deploy

From `/opt/vodhunter` on the VPS:

```sh
docker compose -f compose.production.yaml config
docker compose -f compose.production.yaml up -d alloy
docker compose -f compose.production.yaml ps alloy
```

Alloy scrapes `api:8000/internal/metrics` on the private Compose network and
tails only the `api` container through the Docker socket. The API metrics path
is blocked at the public Nginx edge by `nginx.public.conf.template`.

The API writes one JSON object per log line with the canonical fields
`timestamp`, `level`, `logger`, and `message`. Access logs additionally include
structured HTTP fields, and exception logs keep the exception type, message,
and stack trace in the same JSON record. Alloy promotes the bounded `level`,
`event`, and `outcome` values to Loki labels so Grafana can color and filter
logs without reporting their severity as `unknown`. Search IDs, request paths,
and other high-cardinality fields are attached as structured metadata instead
of labels.

The Docker socket mount is required by `loki.source.docker`; treat the Alloy
container as a host-operations component and keep its image/configuration
changes reviewed.

## Useful queries

PromQL:

```promql
sum by (outcome) (increase(vodhunter_searches_total[$__range]))
histogram_quantile(0.95, sum by (le) (rate(vodhunter_search_duration_seconds_bucket[$__rate_interval])))
histogram_quantile(0.95, sum by (le, stage) (rate(vodhunter_search_stage_duration_seconds_bucket[$__rate_interval])))
```

LogQL:

```logql
{service_name="vodhunter-api", event="search_finished"} | json
{service_name="vodhunter-api", event="search_finished"} | json | search_id = `1842`
{service_name="vodhunter-api", level="error"}
{service_name="vodhunter-api"} | status_code >= 500
```

Search IDs, streamers, result URLs, and timestamps are kept as JSON fields or
structured metadata rather than Loki labels to avoid high-cardinality streams.
