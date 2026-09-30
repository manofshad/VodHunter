"""Bind portable dashboards to verified datasource/folder IDs for gcx."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from generate_dashboards import dashboards


def bind(value, replacements):
    if isinstance(value, str):
        for placeholder, uid in replacements.items():
            value = value.replace(placeholder, uid)
        return value
    if isinstance(value, list):
        return [bind(item, replacements) for item in value]
    if isinstance(value, dict):
        return {key: bind(item, replacements) for key, item in value.items()}
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ("metrics-uid", "logs-uid", "postgres-uid", "folder-uid", "namespace", "output"):
        parser.add_argument(f"--{flag}", required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    replacements = {"${DS_METRICS}": args.metrics_uid, "${DS_LOGS}": args.logs_uid, "${DS_POSTGRES}": args.postgres_uid}
    for filename, portable in dashboards().items():
        spec = bind(portable, replacements)
        uid = spec.pop("uid")
        for key in ("__inputs", "version", "id"):
            spec.pop(key, None)
        resource = {
            "apiVersion": "dashboard.grafana.app/v1beta1", "kind": "Dashboard",
            "metadata": {"name": uid, "namespace": args.namespace,
                         "annotations": {"grafana.app/folder": args.folder_uid}},
            "spec": spec,
        }
        rendered = json.dumps(resource, indent=2) + "\n"
        if "${DS_" in rendered:
            raise ValueError(f"Unbound datasource in {filename}")
        (output / filename).write_text(rendered)
    print(f"Exported {len(dashboards())} dashboards to {output}")


if __name__ == "__main__":
    main()
