from __future__ import annotations

import ast
import json
import logging
from pathlib import Path
import re

import pytest

from backend.json_logging import JsonFormatter


ROOT = Path(__file__).resolve().parents[1]
DATASOURCE_INPUTS = {
    "prometheus": "DS_METRICS",
    "loki": "DS_LOGS",
    "grafana-postgresql-datasource": "DS_POSTGRES",
}


@pytest.fixture(scope="module")
def dashboards() -> dict[str, dict]:
    return {
        path.name: json.loads(path.read_text())
        for path in (ROOT / "observability/grafana").glob("*.json")
    }


def _objects(value: object):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _objects(child)
    elif isinstance(value, list):
        for child in value:
            yield from _objects(child)


def test_alloy_config_scrapes_api_and_keeps_identifiers_out_of_labels() -> None:
    config = (ROOT / "observability/alloy/config.alloy").read_text()

    assert 'metrics_path    = "/internal/metrics"' in config
    assert 'loki.source.docker "vodhunter_api"' in config
    assert 'source_labels = ["__meta_docker_container_label_com_docker_compose_service"]' in config
    assert 'level   = "normalized_level"' in config
    assert 'legacy_level>TRACE|DEBUG|INFO|WARNING|ERROR|CRITICAL' in config
    assert 'search_id                = ""' in config
    assert 'result_link              = ""' in config
    assert 'GRAFANA_CLOUD_API_TOKEN' in config
    assert "password = \"" not in config


def test_alloy_config_collects_worker_and_retention_logs_without_id_labels() -> None:
    config = (ROOT / "observability/alloy/config.alloy").read_text()

    assert 'loki.source.docker "vodhunter_worker"' in config
    assert 'service_name = "vodhunter-worker"' in config
    assert 'loki.source.docker "vodhunter_retention"' in config
    assert 'service_name = "vodhunter-retention"' in config
    assert 'streamer        = ""' in config
    assert 'vod_id          = ""' in config
    assert 'progress_percent = ""' in config

    worker_labels = config.split('loki.process "vodhunter_worker"', 1)[1].split(
        'discovery.relabel "vodhunter_retention"', 1
    )[0]
    assert 'mode   = ""' in worker_labels
    assert 'streamer = ""' not in worker_labels.split(
        'stage.structured_metadata', 1
    )[0]
    assert 'vod_id = ""' not in worker_labels.split(
        'stage.structured_metadata', 1
    )[0]


def test_production_compose_has_private_alloy_sidecar() -> None:
    compose = (ROOT / "compose.production.yaml").read_text()

    assert "alloy:" in compose
    assert "grafana/alloy:v1.19.0" in compose
    assert "./observability/alloy/config.alloy:/etc/alloy/config.alloy:ro" in compose
    assert "/var/run/docker.sock:/var/run/docker.sock:ro" in compose
    assert '"12345"' in compose


def test_production_compose_leaves_database_lifecycle_outside_the_app_stack() -> None:
    compose = (ROOT / "compose.production.yaml").read_text()

    assert "\n  db:\n" not in compose
    assert "postgres_data" not in compose
    assert "DATABASE_URL" in compose
    assert "db:\n        condition: service_healthy" not in compose


def test_deployment_environment_uses_standalone_database_url() -> None:
    env_example = (ROOT / "deploy/.env.example").read_text()

    assert "POSTGRES_DB" not in env_example
    assert "POSTGRES_USER" not in env_example
    assert "POSTGRES_PASSWORD" not in env_example
    assert "@postgres-<coolify-resource-id>:5432/vodhunter" in env_example


def test_api_image_uses_json_logging_configuration() -> None:
    dockerfile = (ROOT / "Dockerfile.api-public").read_text()
    logging_config = json.loads((ROOT / "backend/logging.json").read_text())

    assert '"--log-config", "/app/backend/logging.json"' in dockerfile
    assert logging_config["formatters"]["json"]["()"] == "backend.json_logging.JsonFormatter"


def test_dashboard_suite_exports_ordered_valid_json(dashboards: dict[str, dict]) -> None:
    expected = {
        "00-vodhunter-overview.json": "vodhunter-overview",
        "10-search-performance.json": "vodhunter-search-performance",
        "10-search-quality.json": "vodhunter-search-quality",
        "12-search-reliability.json": "vodhunter-search-reliability",
        "20-streamers-vods.json": "vodhunter-streamers-vods",
        "20-ingestion-operations.json": "vodhunter-ingestion-operations",
        "30-index-retention.json": "vodhunter-index-retention",
        "40-monitoring-health.json": "vodhunter-monitoring-health",
    }

    assert set(dashboards) == set(expected)
    assert len({dashboard["uid"] for dashboard in dashboards.values()}) == 8

    for filename, uid in expected.items():
        dashboard = dashboards[filename]
        assert dashboard["uid"] == uid
        assert dashboard["title"].split(" - ", 1)[0] == filename.split("-", 1)[0]
        assert "vodhunter" in dashboard["tags"]
        assert dashboard["links"][0]["tags"] == ["vodhunter"]
        assert dashboard["panels"]


def test_dashboard_suite_covers_metrics_logs_and_reporting_views(dashboards: dict[str, dict]) -> None:
    target_text = json.dumps(dashboards)

    assert "vodhunter_search_stage_duration_seconds" in target_text
    assert 'service_name=\\\"vodhunter-worker\\\"' in target_text
    assert 'service_name=\\\"vodhunter-retention\\\"' in target_text
    assert "grafana_streamer_summary" in target_text
    assert "grafana_vod_inventory" in target_text
    assert "grafana_search_quality" in target_text
    assert "grafana_index_partitions" in target_text
    assert "grafana_retention_inventory" in target_text
    assert "node_cpu_seconds_total" not in target_text


def test_dashboard_exports_have_portable_datasources_and_defaults(dashboards: dict[str, dict]) -> None:
    for dashboard in dashboards.values():
        assert "id" not in dashboard
        assert "version" not in dashboard
        assert all(link.get("title") != "Browser experience" for link in dashboard["links"])
        used_inputs = {}
        for value in _objects(dashboard):
            datasource = value.get("datasource")
            if not isinstance(datasource, dict) or datasource.get("type") == "grafana":
                continue
            kind = datasource["type"]
            name = DATASOURCE_INPUTS[kind]
            assert datasource["uid"] == "${" + name + "}"
            used_inputs[name] = kind
        assert {item["name"]: item["pluginId"] for item in dashboard["__inputs"]} == used_inputs
        for variable in dashboard["templating"]["list"]:
            assert not variable["options"]
            if variable.get("includeAll"):
                assert variable["current"] == {"text": "All", "value": "$__all"}
            elif variable["name"] == "retention_days":
                assert variable["type"] == "constant"
                assert variable["query"] == "30"
                assert variable["skipUrlSync"] is True
                assert variable["current"] == {"text": "30", "value": "30"}
                assert "Assumed" in variable["label"]
            else:
                assert "current" not in variable


def test_dashboard_sql_variables_use_sqlstring(dashboards: dict[str, dict]) -> None:
    for dashboard in dashboards.values():
        for value in _objects(dashboard):
            if "rawSql" not in value:
                continue
            sql = value["rawSql"]
            assert not re.search(r"\$(?!__)[a-zA-Z_]\w*", sql)
            assert all(formatter == "sqlstring" for _, formatter in re.findall(r"\$\{(\w+)(?::([^}]+))?\}", sql))


def test_prometheus_queries_use_exported_application_metrics(dashboards: dict[str, dict]) -> None:
    declarations = ast.parse((ROOT / "backend/observability.py").read_text())
    exported = set()
    for node in ast.walk(declarations):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
            continue
        if node.func.id not in {"Counter", "Histogram"}:
            continue
        name = node.args[0].value
        if node.func.id == "Counter":
            exported.add(name.removesuffix("_total") + "_total")
            exported.add(name.removesuffix("_total") + "_created")
        else:
            exported.update(name + suffix for suffix in ("_bucket", "_sum", "_count", "_created"))
    assert exported
    for dashboard in dashboards.values():
        for panel in dashboard["panels"]:
            if panel["datasource"]["type"] != "prometheus":
                continue
            for target in panel["targets"]:
                expression = re.sub(r'"(?:\\.|[^"\\])*"', '""', target["expr"])
                custom_metrics = set(re.findall(r"\bvodhunter_[a-zA-Z0-9_]+\b", expression))
                assert custom_metrics <= exported, custom_metrics - exported
                if panel["type"] in {"stat", "bargauge"}:
                    assert target.get("instant") is True


def test_loki_queries_use_loki_intervals_and_instant_summaries(dashboards: dict[str, dict]) -> None:
    for dashboard in dashboards.values():
        for panel in dashboard["panels"]:
            if panel["datasource"]["type"] != "loki":
                continue
            for target in panel["targets"]:
                assert "$__rate_interval" not in target["expr"]
                assert "vector(0)" not in target["expr"]
                if panel["type"] in {"stat", "bargauge"}:
                    assert target["instant"] is True
                    assert target["queryType"] == "instant"


def test_http_response_and_queue_panels_use_existing_telemetry(dashboards: dict[str, dict]) -> None:
    performance = dashboards["10-search-performance.json"]
    panels = {panel["id"]: panel for panel in performance["panels"]}
    assert 12 not in panels
    for panel_id in (11, 13):
        panel = panels[panel_id]
        assert panel["datasource"]["type"] == "loki"
        query = panel["targets"][0]["expr"]
        assert 'event="http_request"' in query
        assert 'environment="production"' in query
        assert '| json | __error__=""' in query
        assert "sum by (status_code)" in query
        assert "$__auto" in query
    submissions = panels[11]["targets"][0]["expr"]
    assert 'http_method="POST"' in submissions
    assert "/api/search/clip" in submissions
    assert 'url_path=~' in submissions
    assert 'http_method=' not in panels[13]["targets"][0]["expr"]
    errors = panels[10]["targets"][0]["expr"]
    assert '| json | __error__=""' in errors
    assert 'level="error" or status_code>=400' in errors
    assert panels[14]["datasource"]["type"] == "grafana-postgresql-datasource"
    queue_query = panels[14]["targets"][0]["rawSql"]
    assert "percentile_cont(0.95)" in queue_query
    assert "EXTRACT(EPOCH FROM started_at-created_at)" in queue_query
    assert "$__timeGroupAlias(created_at, $__interval)" in queue_query
    assert "grafana_search_quality" in queue_query
    reliability = dashboards["12-search-reliability.json"]
    oldest_pending = next(panel for panel in reliability["panels"] if panel["id"] == 3)
    assert "COALESCE(MAX(EXTRACT(EPOCH FROM NOW()-created_at)), 0)" in oldest_pending["targets"][0]["rawSql"]


def test_ingestion_logs_are_global_and_include_watch_errors(dashboards: dict[str, dict]) -> None:
    ingestion = dashboards["20-ingestion-operations.json"]
    panels = {panel["id"]: panel for panel in ingestion["panels"]}
    for panel in panels.values():
        if panel["datasource"]["type"] != "loki":
            continue
        assert "(all streamers)" in panel["title"]
        assert "Monitoring Health" in panel["description"]
        assert panel["fieldConfig"]["defaults"]["noValue"] == "No matching events"
        for target in panel["targets"]:
            assert "streamer" not in target["expr"]
            assert "regexp" not in target["expr"]
    watch_error = "mode=watch streamer=stableronaldo error=live_check_failed detail=DNS lookup failed"
    failed_chunk = "failed mode=backlog vod=123 reason=download_failed"
    for panel_id in (5, 10):
        query = panels[panel_id]["targets"][0]["expr"]
        assert 'action="failed"' not in query
        line_filter = re.search(r'\|~ "([^"]+)"', query).group(1)
        assert re.search(line_filter, watch_error)
        assert re.search(line_filter, failed_chunk)
        assert not re.search(line_filter, "mode=watch streamer=stableronaldo state=offline")


def test_overview_errors_use_service_specific_evidence(dashboards: dict[str, dict]) -> None:
    overview = dashboards["00-vodhunter-overview.json"]
    errors = next(panel for panel in overview["panels"] if panel["id"] == 9)
    assert errors["type"] == "logs"
    assert errors["options"]["sortOrder"] == "Descending"
    queries = {target["refId"]: target["expr"] for target in errors["targets"]}
    assert set(queries) == {"A", "B", "C"}
    assert all('environment="production"' in query for query in queries.values())

    api = queries["A"]
    assert 'service_name="vodhunter-api"' in api
    assert "|~" not in api
    levels = re.search(r'level=~"([^"]+)"', api).group(1)
    for level, expected in ((logging.INFO, False), (logging.ERROR, True), (logging.CRITICAL, True)):
        record = logging.LogRecord("uvicorn.error", level, __file__, 0, "Application startup complete", (), None)
        payload = json.loads(JsonFormatter().format(record))
        assert payload["logger"] == "uvicorn.error"
        assert bool(re.fullmatch(levels, payload["level"])) is expected

    worker = queries["B"]
    assert 'service_name="vodhunter-worker"' in worker
    assert "level=" not in worker
    line_filter = re.search(r'\|~ "([^"]+)"', worker).group(1)
    assert re.search(line_filter, "mode=watch streamer=stableronaldo error=live_check_failed detail=DNS lookup failed")
    assert re.search(line_filter, "failed mode=backlog vod=123 reason=download_failed")
    assert not re.search(line_filter, "mode=watch streamer=stableronaldo state=offline")

    retention = queries["C"]
    assert 'service_name="vodhunter-retention"' in retention
    assert 'event="vod_retention_failed"' in retention
    assert "level=" not in retention
