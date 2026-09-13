from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


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


def test_dashboard_suite_exports_ordered_valid_json() -> None:
    dashboard_dir = ROOT / "observability/grafana"
    expected = {
        "00-vodhunter-overview.json": "vodhunter-overview",
        "10-search-performance.json": "vodhunter-search-performance",
        "10-search-quality.json": "vodhunter-search-quality",
        "20-streamers-vods.json": "vodhunter-streamers-vods",
        "20-ingestion-operations.json": "vodhunter-ingestion-operations",
        "30-index-retention.json": "vodhunter-index-retention",
    }

    exports = {
        path.name: json.loads(path.read_text())
        for path in dashboard_dir.glob("*.json")
    }
    assert set(exports) == set(expected)

    for filename, uid in expected.items():
        dashboard = exports[filename]
        assert dashboard["uid"] == uid
        assert dashboard["title"].split(" - ", 1)[0] in {"00", "10", "20", "30"}
        assert "vodhunter" in dashboard["tags"]
        assert dashboard["links"][0]["tags"] == ["vodhunter"]
        assert dashboard["panels"]


def test_dashboard_suite_covers_metrics_logs_and_reporting_views() -> None:
    dashboard_dir = ROOT / "observability/grafana"
    dashboards = [json.loads(path.read_text()) for path in dashboard_dir.glob("*.json")]
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


def test_generated_dashboard_exports_are_current() -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "observability/grafana/generate_dashboards.py"),
            "--check",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout
