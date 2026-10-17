from __future__ import annotations

from contextlib import nullcontext

import psycopg
import pytest

from storage.database import PostgresDatabase
from storage.records import SearchableStreamer
from storage.video_repository import VideoRepository


pytestmark = pytest.mark.integration


@pytest.fixture
def streamer_listing(database_url):
    with psycopg.connect(database_url) as connection:
        connection.execute("SET LOCAL search_path = pg_temp")
        connection.execute(
            """
            CREATE TEMP TABLE creators (
                id bigint PRIMARY KEY,
                name text,
                profile_image_url text
            )
            """
        )
        connection.execute(
            """
            CREATE TEMP TABLE videos (
                id bigint PRIMARY KEY,
                creator_id bigint,
                status text
            )
            """
        )
        connection.execute("CREATE INDEX ON videos (creator_id)")
        connection.execute(
            "CREATE TEMP TABLE fingerprints (id bigint PRIMARY KEY, video_id bigint)"
        )
        connection.execute("CREATE INDEX ON fingerprints (video_id)")
        connection.execute(
            """
            CREATE TEMP TABLE fingerprint_embeddings (
                creator_id bigint,
                fingerprint_id bigint,
                PRIMARY KEY (creator_id, fingerprint_id)
            ) PARTITION BY LIST (creator_id)
            """
        )
        connection.execute(
            """
            CREATE TEMP TABLE fingerprint_embeddings_1
            PARTITION OF fingerprint_embeddings FOR VALUES IN (1)
            """
        )
        connection.execute(
            """
            CREATE TEMP TABLE fingerprint_embeddings_2
            PARTITION OF fingerprint_embeddings FOR VALUES IN (2)
            """
        )
        connection.execute(
            """
            CREATE TEMP TABLE fingerprint_embeddings_default
            PARTITION OF fingerprint_embeddings DEFAULT
            """
        )
        database = PostgresDatabase.__new__(PostgresDatabase)
        database.connect = lambda: nullcontext(connection)
        yield VideoRepository(database), connection


def test_lists_streamers_once_with_original_names_avatars_and_case_insensitive_order(
    streamer_listing,
):
    repository, connection = streamer_listing
    connection.execute(
        """
        INSERT INTO creators VALUES
            (1, 'Zulu', 'https://cdn.example/zulu.png'),
            (2, 'alpha', NULL),
            (3, 'Bravo', ''),
            (4, '', 'https://cdn.example/blank.png'),
            (5, '   ', NULL),
            (6, NULL, NULL)
        """
    )
    connection.execute(
        """
        INSERT INTO videos VALUES
            (10, 1, 'searchable'), (11, 1, 'searchable'),
            (20, 2, 'searchable'), (30, 3, 'searchable'),
            (40, 4, 'searchable'), (50, 5, 'searchable'),
            (60, 6, 'searchable')
        """
    )
    connection.execute(
        """
        INSERT INTO fingerprints VALUES
            (100, 10), (101, 10), (110, 11), (200, 20),
            (300, 30), (400, 40), (500, 50), (600, 60)
        """
    )
    connection.execute(
        """
        INSERT INTO fingerprint_embeddings VALUES
            (1, 100), (1, 101), (1, 110), (2, 200),
            (3, 300), (4, 400), (5, 500), (6, 600)
        """
    )

    assert repository.list_searchable_streamers() == [
        SearchableStreamer(name="alpha", profile_image_url=None),
        SearchableStreamer(name="Bravo", profile_image_url=None),
        SearchableStreamer(name="Zulu", profile_image_url="https://cdn.example/zulu.png"),
    ]


@pytest.mark.parametrize(
    ("status", "included"),
    [
        ("indexing", True),
        ("searchable", True),
        ("unavailable", False),
        ("deleted", False),
        ("reindex_requested", False),
    ],
)
def test_only_lists_vods_in_search_eligible_states(streamer_listing, status, included):
    repository, connection = streamer_listing
    connection.execute(
        "INSERT INTO creators VALUES (1, 'Candidate', NULL), (2, 'Control', NULL)"
    )
    connection.execute(
        "INSERT INTO videos VALUES (10, 1, %s), (20, 2, 'searchable')", (status,)
    )
    connection.execute("INSERT INTO fingerprints VALUES (100, 10), (200, 20)")
    connection.execute("INSERT INTO fingerprint_embeddings VALUES (1, 100), (2, 200)")

    expected = [SearchableStreamer(name="Control", profile_image_url=None)]
    if included:
        expected.insert(0, SearchableStreamer(name="Candidate", profile_image_url=None))
    assert repository.list_searchable_streamers() == expected


def test_requires_an_embedding_owned_by_the_videos_creator(streamer_listing):
    repository, connection = streamer_listing
    connection.execute(
        """
        INSERT INTO creators VALUES
            (1, 'Eligible', NULL),
            (2, 'Fingerprint only', NULL),
            (3, 'Wrong owner', NULL),
            (4, 'Embedding owner', NULL)
        """
    )
    connection.execute(
        """
        INSERT INTO videos VALUES
            (10, 1, 'searchable'), (20, 2, 'searchable'), (30, 3, 'searchable')
        """
    )
    connection.execute(
        "INSERT INTO fingerprints VALUES (100, 10), (200, 20), (300, 30)"
    )
    connection.execute("INSERT INTO fingerprint_embeddings VALUES (1, 100), (4, 300)")

    assert repository.list_searchable_streamers() == [
        SearchableStreamer(name="Eligible", profile_image_url=None),
    ]


def test_checks_later_vods_after_vods_without_embeddings(streamer_listing):
    repository, connection = streamer_listing
    connection.execute("INSERT INTO creators VALUES (1, 'Later VOD', NULL)")
    connection.execute(
        """
        INSERT INTO videos VALUES
            (10, 1, 'searchable'), (20, 1, 'indexing'), (30, 1, 'searchable')
        """
    )
    connection.execute("INSERT INTO fingerprints VALUES (200, 20), (300, 30)")
    connection.execute("INSERT INTO fingerprint_embeddings VALUES (1, 300)")

    assert repository.list_searchable_streamers() == [
        SearchableStreamer(name="Later VOD", profile_image_url=None),
    ]
