from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_cobalt_is_private_pinned_and_bounded():
    compose = yaml.safe_load((ROOT / "compose.production.yaml").read_text())
    cobalt = compose["services"]["cobalt"]
    api = compose["services"]["api"]

    assert cobalt["image"].startswith("ghcr.io/imputnet/cobalt:11.7.1@sha256:")
    assert "ports" not in cobalt
    assert "labels" not in cobalt
    assert cobalt["expose"] == ["9000"]
    assert cobalt["environment"]["API_URL"] == "http://cobalt:9000/"
    assert "http://cobalt:9000/" in api["environment"]["COBALT_API_URL"]
    assert cobalt["healthcheck"]["test"]
    assert cobalt["restart"] == "unless-stopped"
    assert cobalt["mem_limit"]
    assert cobalt["cpus"]
