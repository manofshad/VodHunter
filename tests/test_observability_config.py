from __future__ import annotations

import json
from pathlib import Path


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


def test_dashboard_export_is_valid_json_and_has_metrics_and_logs_panels() -> None:
    dashboard = json.loads(
        (ROOT / "observability/grafana/vodhunter-search-overview.json").read_text()
    )
    panel_types = {panel["type"] for panel in dashboard["panels"]}

    assert dashboard["uid"] == "vodhunter-search-overview"
    assert "timeseries" in panel_types
    assert "logs" in panel_types
    assert any(
        "vodhunter_search_stage_duration_seconds" in target.get("expr", "")
        for panel in dashboard["panels"]
        for target in panel.get("targets", [])
    )
