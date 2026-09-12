from datetime import datetime, timezone

from storage.database import PostgresDatabase
from storage.ingest_state_repository import IngestStateRepository
from storage.records import SearchableStreamer, VideoRecord, VideoStatus, VodIngestStateRecord
from storage.video_repository import VideoRepository

class FakeCursor:

    def __init__(self):
        self.executed: list[tuple[str, tuple | None]] = []
        self._fetchall_results: list[list[tuple]] = []
        self._fetchone_results: list[tuple | None] = []

    def execute(self, query: str, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        if self._fetchall_results:
            return list(self._fetchall_results.pop(0))
        return []

    def fetchone(self):
        if self._fetchone_results:
            return self._fetchone_results.pop(0)
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

class FakeConnection:

    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

def build_database(cursor: FakeCursor) -> PostgresDatabase:
    database = PostgresDatabase.__new__(PostgresDatabase)
    database.connect = lambda: FakeConnection(cursor)
    return database


def build_video_repository(cursor: FakeCursor) -> VideoRepository:
    return VideoRepository(build_database(cursor))


def build_ingest_state_repository(cursor: FakeCursor) -> IngestStateRepository:
    return IngestStateRepository(build_database(cursor))


class TestVideoRepository:

    def test_get_video_by_url_returns_named_record(self) -> None:
        streamed_at = datetime(2026, 9, 9, 12, 0, tzinfo=timezone.utc)
        cursor = FakeCursor()
        cursor._fetchone_results = [
            (17, 3, 'https://www.twitch.tv/videos/17', 'A VOD', 'https://cdn/thumb.jpg', 'searchable', True, streamed_at)
        ]
        store = build_video_repository(cursor)

        record = store.get_video_by_url('https://www.twitch.tv/videos/17')

        assert record == VideoRecord(
            id=17,
            creator_id=3,
            url='https://www.twitch.tv/videos/17',
            title='A VOD',
            thumbnail_url='https://cdn/thumb.jpg',
            status=VideoStatus.SEARCHABLE,
            processed=True,
            streamed_at=streamed_at,
        )
        assert 'status, processed, streamed_at' in cursor.executed[0][0]

    def test_get_video_with_creator_returns_enriched_named_record(self) -> None:
        cursor = FakeCursor()
        cursor._fetchone_results = [
            (
                17,
                3,
                'https://www.twitch.tv/videos/17',
                'A VOD',
                'https://cdn/thumb.jpg',
                'searchable',
                True,
                None,
                'xqc',
                'https://cdn/xqc.png',
            )
        ]
        store = build_video_repository(cursor)

        record = store.get_video_with_creator(17)

        assert record is not None
        assert record.id == 17
        assert record.creator_name == 'xqc'
        assert record.creator_profile_image_url == 'https://cdn/xqc.png'
        assert record.status == VideoStatus.SEARCHABLE

    def test_get_vod_ingest_state_returns_named_record(self) -> None:
        updated_at = datetime(2026, 9, 9, 12, 0, tzinfo=timezone.utc)
        cursor = FakeCursor()
        cursor._fetchone_results = [('vod-17', 17, 'xqc', 120, 180, updated_at)]
        store = build_ingest_state_repository(cursor)

        record = store.get('vod-17')

        assert record == VodIngestStateRecord(
            vod_platform_id='vod-17',
            video_id=17,
            streamer='xqc',
            last_ingested_seconds=120,
            last_seen_duration_seconds=180,
            updated_at=updated_at,
        )

    def test_get_creator_id_by_name_normalizes_input(self) -> None:
        cursor = FakeCursor()
        cursor._fetchone_results = [(7,)]
        store = build_video_repository(cursor)
        creator_id = store.get_creator_id_by_name(' XqC ')
        assert creator_id == 7
        assert 'WHERE LOWER(name) = %s' in cursor.executed[0][0]
        assert cursor.executed[0][1] == ('xqc',)

    def test_get_creator_id_by_name_returns_none_when_missing(self) -> None:
        cursor = FakeCursor()
        store = build_video_repository(cursor)
        creator_id = store.get_creator_id_by_name('missing')
        assert creator_id is None

    def test_list_searchable_streamers_returns_names(self) -> None:
        cursor = FakeCursor()
        cursor._fetchall_results = [[('Jason', 'https://cdn/jason.png'), ('ronaldo', None)]]
        store = build_video_repository(cursor)
        names = store.list_searchable_streamers()
        assert names == [
            SearchableStreamer(name='Jason', profile_image_url='https://cdn/jason.png'),
            SearchableStreamer(name='ronaldo', profile_image_url=None),
        ]
        assert 'SELECT c.name, c.profile_image_url' in cursor.executed[0][0]
        assert 'EXISTS' in cursor.executed[0][0]
        assert 'WHERE fe.creator_id = c.id' in cursor.executed[0][0]
        assert 'JOIN fingerprints' not in cursor.executed[0][0]

    def test_create_or_get_creator_upserts_profile_image_url(self) -> None:
        cursor = FakeCursor()
        cursor._fetchone_results = [(7,)]
        store = build_video_repository(cursor)
        creator_id = store.create_or_get_creator('xqc', 'https://twitch.tv/xqc', profile_image_url='https://cdn/xqc.png')
        assert creator_id == 7
        assert 'profile_image_url' in cursor.executed[0][0]
        assert cursor.executed[0][1] == ('xqc', 'https://twitch.tv/xqc', 'https://cdn/xqc.png')

    def test_update_video_metadata_updates_thumbnail_and_processed(self) -> None:
        cursor = FakeCursor()
        store = build_video_repository(cursor)
        store.update_video_metadata(55, title='Updated title', thumbnail_url='https://cdn/thumb.jpg', processed=False)
        query, params = cursor.executed[0]
        assert 'title = %s' in query
        assert 'thumbnail_url = %s' in query
        assert 'processed = %s' in query
        assert 'status = %s' in query
        assert params == ['Updated title', 'https://cdn/thumb.jpg', False, 'indexing', 55]

    def test_update_video_status_keeps_deleted_vods_processed(self) -> None:
        cursor = FakeCursor()
        repository = build_video_repository(cursor)

        repository.update_video_status(55, VideoStatus.DELETED)

        assert cursor.executed == [
            (
                "UPDATE videos SET status = %s, processed = %s WHERE id = %s",
                ("deleted", True, 55),
            )
        ]

    def test_update_video_status_keeps_reindex_requested_vods_processed(self) -> None:
        cursor = FakeCursor()
        repository = build_video_repository(cursor)

        repository.update_video_status(56, VideoStatus.REINDEX_REQUESTED)

        assert cursor.executed == [
            (
                "UPDATE videos SET status = %s, processed = %s WHERE id = %s",
                ("reindex_requested", True, 56),
            )
        ]
