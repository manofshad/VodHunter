from storage.vector_store import VectorStore

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

class TestVectorStoreStreamerScope:

    def test_get_creator_id_by_name_normalizes_input(self) -> None:
        cursor = FakeCursor()
        cursor._fetchone_results = [(7,)]
        store = VectorStore.__new__(VectorStore)
        store._connect = lambda: FakeConnection(cursor)
        creator_id = store.get_creator_id_by_name(' XqC ')
        assert creator_id == 7
        assert 'WHERE LOWER(name) = %s' in cursor.executed[0][0]
        assert cursor.executed[0][1] == ('xqc',)

    def test_get_creator_id_by_name_returns_none_when_missing(self) -> None:
        cursor = FakeCursor()
        store = VectorStore.__new__(VectorStore)
        store._connect = lambda: FakeConnection(cursor)
        creator_id = store.get_creator_id_by_name('missing')
        assert creator_id is None

    def test_list_searchable_streamers_returns_names(self) -> None:
        cursor = FakeCursor()
        cursor._fetchall_results = [[('Jason', 'https://cdn/jason.png'), ('ronaldo', None)]]
        store = VectorStore.__new__(VectorStore)
        store._connect = lambda: FakeConnection(cursor)
        names = store.list_searchable_streamers()
        assert names == [
            {'name': 'Jason', 'profile_image_url': 'https://cdn/jason.png'},
            {'name': 'ronaldo', 'profile_image_url': None},
        ]
        assert 'SELECT c.name, c.profile_image_url' in cursor.executed[0][0]
        assert 'GROUP BY c.name, c.profile_image_url' in cursor.executed[0][0]

    def test_create_or_get_creator_upserts_profile_image_url(self) -> None:
        cursor = FakeCursor()
        cursor._fetchone_results = [(7,)]
        store = VectorStore.__new__(VectorStore)
        store._connect = lambda: FakeConnection(cursor)
        creator_id = store.create_or_get_creator('xqc', 'https://twitch.tv/xqc', profile_image_url='https://cdn/xqc.png')
        assert creator_id == 7
        assert 'profile_image_url' in cursor.executed[0][0]
        assert cursor.executed[0][1] == ('xqc', 'https://twitch.tv/xqc', 'https://cdn/xqc.png')

    def test_update_video_metadata_updates_thumbnail_and_processed(self) -> None:
        cursor = FakeCursor()
        store = VectorStore.__new__(VectorStore)
        store._connect = lambda: FakeConnection(cursor)
        store.update_video_metadata(55, title='Updated title', thumbnail_url='https://cdn/thumb.jpg', processed=False)
        query, params = cursor.executed[0]
        assert 'title = %s' in query
        assert 'thumbnail_url = %s' in query
        assert 'processed = %s' in query
        assert 'status = %s' in query
        assert params == ['Updated title', 'https://cdn/thumb.jpg', False, 'indexing', 55]
