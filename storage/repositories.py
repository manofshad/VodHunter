"""Composition root for the application's PostgreSQL repositories."""

from __future__ import annotations

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


def build_repositories(database_url: str) -> Repositories:
    database = PostgresDatabase(database_url)
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
