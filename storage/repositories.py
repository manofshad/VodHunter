"""Composition root for the application's PostgreSQL repositories."""

from __future__ import annotations

import os
from dataclasses import dataclass

from storage.database import PostgresDatabase
from storage.fingerprint_repository import FingerprintRepository
from storage.ingest_state_repository import IngestStateRepository
from storage.search_job_repository import SearchJobRepository
from storage.video_repository import VideoRepository


@dataclass(frozen=True, slots=True)
class Repositories:
    database: PostgresDatabase
    videos: VideoRepository
    fingerprints: FingerprintRepository
    ingest_states: IngestStateRepository
    search_jobs: SearchJobRepository


def _read_positive_int_env(name: str, default: int) -> int:
    raw_value = os.getenv(name, "").strip()
    if not raw_value:
        return int(default)
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < 1:
        raise ValueError(f"{name} must be >= 1")
    return value


def _read_positive_float_env(name: str, default: float) -> float:
    raw_value = os.getenv(name, "").strip()
    if not raw_value:
        return float(default)
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if value <= 0:
        raise ValueError(f"{name} must be > 0")
    return value


def build_repositories(database_url: str) -> Repositories:
    database = PostgresDatabase(
        database_url,
        hnsw_ef_search=_read_positive_int_env("HNSW_EF_SEARCH", 100),
        hnsw_iterative_scan=os.getenv("HNSW_ITERATIVE_SCAN", "strict_order").strip().lower()
        or "strict_order",
        hnsw_max_scan_tuples=_read_positive_int_env("HNSW_MAX_SCAN_TUPLES", 20_000),
        hnsw_scan_mem_multiplier=_read_positive_float_env("HNSW_SCAN_MEM_MULTIPLIER", 1.0),
    )
    database.ensure_schema_ready()
    videos = VideoRepository(database)
    fingerprints = FingerprintRepository(database)
    ingest_states = IngestStateRepository(database)
    search_jobs = SearchJobRepository(database, videos)
    return Repositories(
        database=database,
        videos=videos,
        fingerprints=fingerprints,
        ingest_states=ingest_states,
        search_jobs=search_jobs,
    )
