import os
import tempfile
from sources.live_archive_vod_source import LiveArchiveVODSource

class FakeMonitor:

    def __init__(self, live_sequence: list[bool], vod_duration_seconds: int=240):
        self.live_sequence = list(live_sequence)
        self.vod_duration_seconds = vod_duration_seconds

    def is_live(self, streamer: str) -> bool:
        if len(self.live_sequence) > 1:
            return self.live_sequence.pop(0)
        return self.live_sequence[0]

    def get_user_id(self, streamer: str) -> str:
        return 'user-1'

    def get_latest_archive_vod(self, user_id: str):
        return {'id': 'vod-1', 'url': 'https://www.twitch.tv/videos/vod-1', 'title': 'Live stream', 'thumbnail_url': 'https://static-cdn.jtvnw.net/cf_vods/thumb-320x180.jpg', 'duration_seconds': self.vod_duration_seconds}

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

    def mark_video_processed(self, video_id: int, processed: bool=True) -> None:
        for url, row in list(self.videos_by_url.items()):
            if int(row[0]) == int(video_id):
                self.videos_by_url[url] = (row[0], row[1], row[2], row[3], row[4], bool(processed))
                return

    def update_video_metadata(self, video_id: int, *, title: str | None=None, thumbnail_url: str | None=None, processed: bool | None=None) -> None:
        for url, row in list(self.videos_by_url.items()):
            if int(row[0]) != int(video_id):
                continue
            self.videos_by_url[url] = (row[0], row[1], row[2], title if title is not None else row[3], thumbnail_url if thumbnail_url is not None else row[4], bool(processed) if processed is not None else row[5])
            return

    def get_vod_ingest_state(self, vod_platform_id: str):
        return self.vod_state.get(vod_platform_id)

    def upsert_vod_ingest_state(self, vod_platform_id: str, video_id: int, streamer: str, last_ingested_seconds: int, last_seen_duration_seconds: int) -> None:
        self.vod_state[vod_platform_id] = {'vod_platform_id': vod_platform_id, 'video_id': int(video_id), 'streamer': streamer, 'last_ingested_seconds': int(last_ingested_seconds), 'last_seen_duration_seconds': int(last_seen_duration_seconds), 'updated_at': 'now'}

    def delete_vod_ingest_state(self, vod_platform_id: str) -> None:
        self.vod_state.pop(vod_platform_id, None)

class TestLiveArchiveVODSource:

    def _make_source(self, tmp: str, live_sequence: list[bool]) -> LiveArchiveVODSource:
        store = FakeStore()
        monitor = FakeMonitor(live_sequence=live_sequence)
        source = LiveArchiveVODSource(streamer='alice', store=store, twitch_monitor=monitor, chunk_seconds=60, lag_seconds=120, poll_seconds=0.0, finalize_checks=2, temp_dir=f'{tmp}/chunks')

        def fake_extract_chunk(start_seconds: int, duration_seconds: int) -> str:
            out = os.path.join(source.temp_dir, f'chunk_{start_seconds}_{duration_seconds}.wav')
            os.makedirs(source.temp_dir, exist_ok=True)
            with open(out, 'wb') as f:
                f.write(b'fake')
            return out
        source._extract_chunk = fake_extract_chunk
        return source

    def test_cursor_advances_on_next_poll(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = self._make_source(tmp, live_sequence=[True, True, True])
            source.start()
            chunk1 = source.next_chunk()
            assert chunk1 is not None
            assert source.ingest_cursor_seconds == 0
            chunk2 = source.next_chunk()
            assert chunk2 is not None
            assert source.ingest_cursor_seconds == 60

    def test_finalize_marks_video_processed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = self._make_source(tmp, live_sequence=[True, True, False, False, False, False])
            source.start()
            for _ in range(20):
                source.next_chunk()
                if source.is_finished:
                    break
            assert source.is_finished
            assert source.video_id is not None
            assert source.video_id is not None
            row = source.store.get_video_by_url('https://www.twitch.tv/videos/vod-1')
            assert row is not None
            assert row is not None
            assert row[4] == 'https://static-cdn.jtvnw.net/cf_vods/thumb-320x180.jpg'
            assert row[5]

    def test_existing_video_metadata_is_refreshed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = self._make_source(tmp, live_sequence=[True])
            existing_creator_id = source.store.create_or_get_creator('alice', 'https://twitch.tv/alice')
            source.store.create_video(creator_id=existing_creator_id, url='https://www.twitch.tv/videos/vod-1', title='Old title', thumbnail_url=None, processed=True)
            source.start()
            row = source.store.get_video_by_url('https://www.twitch.tv/videos/vod-1')
            assert row is not None
            assert row is not None
            assert row[3] == 'Live stream'
            assert row[4] == 'https://static-cdn.jtvnw.net/cf_vods/thumb-320x180.jpg'
            assert not row[5]
