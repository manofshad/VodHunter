import os
import tempfile
from sources.historical_archive_vod_source import HistoricalArchiveVODSource

class FakeStore:

    def __init__(self):
        self._creator_id = 0
        self._video_id = 0
        self.creators: dict[str, tuple[int, str, str]] = {}
        self.videos_by_url: dict[str, tuple[int, int, str, str, str | None, bool]] = {}
        self.vod_state: dict[str, dict] = {}

    def create_or_get_creator(self, name: str, url: str) -> int:
        existing = self.creators.get(url)
        if existing is not None:
            return existing[0]
        self._creator_id += 1
        self.creators[url] = (self._creator_id, name, url)
        return self._creator_id

    def get_video_by_url(self, url: str):
        return self.videos_by_url.get(url)

    def create_video(self, creator_id: int, url: str, title: str, processed: bool, thumbnail_url: str | None=None) -> int:
        self._video_id += 1
        row = (self._video_id, int(creator_id), url, title, thumbnail_url, bool(processed))
        self.videos_by_url[url] = row
        return self._video_id

    def update_video_metadata(self, video_id: int, *, title: str | None=None, thumbnail_url: str | None=None, processed: bool | None=None) -> None:
        for url, row in list(self.videos_by_url.items()):
            if int(row[0]) != int(video_id):
                continue
            self.videos_by_url[url] = (row[0], row[1], row[2], title if title is not None else row[3], thumbnail_url if thumbnail_url is not None else row[4], bool(processed) if processed is not None else row[5])
            return

    def mark_video_processed(self, video_id: int, processed: bool=True) -> None:
        for url, row in list(self.videos_by_url.items()):
            if int(row[0]) == int(video_id):
                self.videos_by_url[url] = (row[0], row[1], row[2], row[3], row[4], bool(processed))
                return

    def get_vod_ingest_state(self, vod_platform_id: str):
        return self.vod_state.get(vod_platform_id)

    def upsert_vod_ingest_state(self, vod_platform_id: str, video_id: int, streamer: str, last_ingested_seconds: int, last_seen_duration_seconds: int) -> None:
        self.vod_state[vod_platform_id] = {'vod_platform_id': vod_platform_id, 'video_id': int(video_id), 'streamer': streamer, 'last_ingested_seconds': int(last_ingested_seconds), 'last_seen_duration_seconds': int(last_seen_duration_seconds), 'updated_at': 'now'}

    def delete_vod_ingest_state(self, vod_platform_id: str) -> None:
        self.vod_state.pop(vod_platform_id, None)

class TestHistoricalArchiveVODSource:

    def _make_vod(self) -> dict[str, object]:
        return {'id': 'vod-1', 'url': 'https://www.twitch.tv/videos/vod-1', 'title': 'Historical stream', 'thumbnail_url': 'https://static-cdn.jtvnw.net/thumb.jpg', 'duration_seconds': 180}

    def test_resumes_from_saved_cursor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = FakeStore()
            creator_id = store.create_or_get_creator('alice', 'https://twitch.tv/alice')
            video_id = store.create_video(creator_id=creator_id, url='https://www.twitch.tv/videos/vod-1', title='Old', thumbnail_url=None, processed=False)
            store.upsert_vod_ingest_state('vod-1', video_id, 'alice', 120, 180)
            source = HistoricalArchiveVODSource(streamer='alice', vod_metadata=self._make_vod(), store=store, chunk_seconds=60, temp_dir=f'{tmp}/chunks')

            def fake_extract_chunk(start_seconds: int, duration_seconds: int) -> str:
                out = os.path.join(source.temp_dir, f'chunk_{start_seconds}_{duration_seconds}.wav')
                os.makedirs(source.temp_dir, exist_ok=True)
                with open(out, 'wb') as handle:
                    handle.write(b'fake')
                return out
            source._extract_chunk = fake_extract_chunk
            source.start()
            assert source.ingest_cursor_seconds == 120
            chunk = source.next_chunk()
            assert chunk is not None
            assert chunk is not None
            assert chunk.offset_seconds == 120.0
            assert store.vod_state['vod-1']['last_ingested_seconds'] == 120
            source.next_chunk()
            assert source.ingest_cursor_seconds == 180
            assert source.is_finished

    def test_finalize_marks_video_processed_and_clears_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = FakeStore()
            source = HistoricalArchiveVODSource(streamer='alice', vod_metadata=self._make_vod(), store=store, chunk_seconds=180, temp_dir=f'{tmp}/chunks')

            def fake_extract_chunk(start_seconds: int, duration_seconds: int) -> str:
                out = os.path.join(source.temp_dir, f'chunk_{start_seconds}_{duration_seconds}.wav')
                os.makedirs(source.temp_dir, exist_ok=True)
                with open(out, 'wb') as handle:
                    handle.write(b'fake')
                return out
            source._extract_chunk = fake_extract_chunk
            source.start()
            assert 'vod-1' in store.vod_state
            source.next_chunk()
            source.next_chunk()
            row = store.get_video_by_url('https://www.twitch.tv/videos/vod-1')
            assert row is not None
            assert row is not None
            assert row[5]
            assert 'vod-1' not in store.vod_state
            assert source.is_finished
