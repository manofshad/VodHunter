from __future__ import annotations

from dataclasses import dataclass, field
import os
from uuid import uuid4

import pytest

from storage.repositories import Repositories, build_repositories


@pytest.fixture(scope="session")
def database_url() -> str:
    value = os.getenv("VODHUNTER_TEST_DATABASE_URL", "").strip()
    if not value:
        pytest.skip(
            "VODHUNTER_TEST_DATABASE_URL is not set; start the test PostgreSQL/pgvector service "
            "to run integration tests"
        )
    return value


@dataclass
class DatabaseScope:
    database_url: str
    token: str = field(default_factory=lambda: uuid4().hex)
    creator_ids: list[int] = field(default_factory=list)
    video_ids: list[int] = field(default_factory=list)
    search_ids: list[int] = field(default_factory=list)

    @property
    def streamer(self) -> str:
        return f"integration-{self.token}"

    @property
    def creator_url(self) -> str:
        return f"https://www.twitch.tv/{self.streamer}"

    def remember_creator(self, creator_id: int) -> int:
        self.creator_ids.append(int(creator_id))
        return int(creator_id)

    def remember_video(self, video_id: int) -> int:
        self.video_ids.append(int(video_id))
        return int(video_id)

    def remember_search(self, search_id: int) -> int:
        self.search_ids.append(int(search_id))
        return int(search_id)

    def cleanup(self) -> None:
        if not self.creator_ids and not self.video_ids and not self.search_ids:
            return

        import psycopg

        with psycopg.connect(self.database_url) as connection:
            with connection.cursor() as cursor:
                if self.search_ids:
                    cursor.execute(
                        "DELETE FROM search_requests WHERE id = ANY(%s)",
                        (self.search_ids,),
                    )
                if self.video_ids:
                    cursor.execute(
                        "DELETE FROM vod_ingest_state WHERE video_id = ANY(%s)",
                        (self.video_ids,),
                    )
                    cursor.execute(
                        "DELETE FROM fingerprints WHERE video_id = ANY(%s)",
                        (self.video_ids,),
                    )
                    cursor.execute(
                        "DELETE FROM videos WHERE id = ANY(%s)",
                        (self.video_ids,),
                    )
                if self.creator_ids:
                    cursor.execute(
                        "DELETE FROM creators WHERE id = ANY(%s)",
                        (self.creator_ids,),
                    )


@pytest.fixture
def database_scope(database_url: str):
    scope = DatabaseScope(database_url=database_url)
    try:
        yield scope
    finally:
        scope.cleanup()


@pytest.fixture
def store(database_url: str) -> Repositories:
    return build_repositories(database_url)
