from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from .models import QueryRecord


def load_manifest(path: Path) -> list[QueryRecord]:
    if not path.exists():
        return []
    records: list[QueryRecord] = []
    seen: set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            record = QueryRecord.from_dict(json.loads(line))
            if record.query_id in seen:
                raise ValueError(f"Duplicate query_id {record.query_id!r} on line {line_number}")
            if record.expected_match and record.expected_start_seconds is None:
                raise ValueError(f"Positive query {record.query_id!r} has no expected timestamp")
            if not record.resolved_path(path).exists():
                raise FileNotFoundError(f"Query file does not exist: {record.resolved_path(path)}")
            seen.add(record.query_id)
            records.append(record)
    return records


def write_manifest(path: Path, records: Iterable[QueryRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted(records, key=lambda item: item.query_id)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8") as handle:
        for record in ordered:
            handle.write(json.dumps(record.to_dict(), sort_keys=True) + "\n")
    temp_path.replace(path)


def upsert_records(path: Path, new_records: Iterable[QueryRecord]) -> list[QueryRecord]:
    existing = {record.query_id: record for record in load_manifest(path)} if path.exists() else {}
    for record in new_records:
        existing[record.query_id] = record
    records = list(existing.values())
    write_manifest(path, records)
    return sorted(records, key=lambda item: item.query_id)
