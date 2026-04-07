import os
import tempfile
from sources.live_archive_vod_source import LiveArchiveVODSource

class FakeMonitor:

    def __init__(
        self,
        live_sequence: list[bool],
        vod_duration_seconds: int = 240,
        vod_sequence: list[dict[str, object]] | None = None,
    ):
        self.live_sequence = list(live_sequence)
        self.vod_duration_seconds = vod_duration_seconds
        self.vod_sequence = list(vod_sequence or [])
        self.user_profile_sequence: list[dict[str, object]] = [
            {
                'id': 'user-1',
                'login': 'alice',
                'display_name': 'alice',
                'profile_image_url': 'https://static-cdn.jtvnw.net/jtv_user_pictures/alice.png',
            }
        ]

    def is_live(self, streamer: str) -> bool:
        if len(self.live_sequence) > 1:
            return self.live_sequence.pop(0)
        return self.live_sequence[0]

    def get_user_id(self, streamer: str) -> str:
        return 'user-1'

    def get_user_profile(self, streamer: str, force_refresh: bool = False):
        if self.user_profile_sequence:
            if force_refresh and len(self.user_profile_sequence) > 1:
                self.user_profile_sequence.pop(0)
                return dict(self.user_profile_sequence[0])
            return dict(self.user_profile_sequence[0])
        return {'id': 'user-1', 'login': streamer, 'display_name': streamer, 'profile_image_url': None}

    def get_latest_archive_vod(self, user_id: str):
        if self.vod_sequence:
            if len(self.vod_sequence) > 1:
                return dict(self.vod_sequence.pop(0))
            return dict(self.vod_sequence[0])
        return {'id': 'vod-1', 'url': 'https://www.twitch.tv/videos/vod-1', 'title': 'Live stream', 'thumbnail_url': 'https://static-cdn.jtvnw.net/cf_vods/thumb-320x180.jpg', 'duration_seconds': self.vod_duration_seconds, 'created_at': '2026-04-06T00:00:00Z'}

class FakeStore:

    def __init__(self):
        self._creator_id = 0
        self._video_id = 0
        self.creators: dict[str, tuple[int, str, str, str | None]] = {}
        self.videos_by_url: dict[str, tuple[int, int, str, str, str | None, bool]] = {}
        self.vod_state: dict[str, dict] = {}
        self.metadata_updates: list[dict[str, object]] = []
        self.creator_metadata_updates: list[dict[str, object]] = []

    def create_or_get_creator(self, name: str, url: str, profile_image_url: str | None = None) -> int:
        existing = self.creators.get(url)
        if existing is not None:
            self.creators[url] = (existing[0], name, url, profile_image_url if profile_image_url is not None else existing[3])
            return existing[0]
        self._creator_id += 1
        self.creators[url] = (self._creator_id, name, url, profile_image_url)
        return self._creator_id

    def update_creator_metadata(self, creator_id: int, *, name: str | None = None, profile_image_url: str | None = None) -> None:
        self.creator_metadata_updates.append(
            {
                'creator_id': int(creator_id),
                'name': name,
                'profile_image_url': profile_image_url,
            }
        )
        for url, row in list(self.creators.items()):
            if int(row[0]) != int(creator_id):
                continue
            self.creators[url] = (
                row[0],
                name if name is not None else row[1],
                row[2],
                profile_image_url if profile_image_url is not None else row[3],
            )
            return

    def get_video_by_url(self, url: str):
        return self.videos_by_url.get(url)

    def create_video(self, creator_id: int, url: str, title: str, processed: bool, thumbnail_url: str | None=None, streamed_at=None) -> int:
        self._video_id += 1
        row = (self._video_id, int(creator_id), url, title, thumbnail_url, bool(processed), streamed_at)
        self.videos_by_url[url] = row
        return self._video_id

    def mark_video_processed(self, video_id: int, processed: bool=True) -> None:
        for url, row in list(self.videos_by_url.items()):
            if int(row[0]) == int(video_id):
                self.videos_by_url[url] = (row[0], row[1], row[2], row[3], row[4], bool(processed), row[6])
                return

    def update_video_metadata(self, video_id: int, *, title: str | None=None, thumbnail_url: str | None=None, processed: bool | None=None, streamed_at=None) -> None:
        self.metadata_updates.append(
            {
                'video_id': int(video_id),
                'title': title,
                'thumbnail_url': thumbnail_url,
                'processed': processed,
            }
        )
        for url, row in list(self.videos_by_url.items()):
            if int(row[0]) != int(video_id):
                continue
            self.videos_by_url[url] = (row[0], row[1], row[2], title if title is not None else row[3], thumbnail_url if thumbnail_url is not None else row[4], bool(processed) if processed is not None else row[5], row[6])
            return

    def get_vod_ingest_state(self, vod_platform_id: str):
        return self.vod_state.get(vod_platform_id)

    def upsert_vod_ingest_state(self, vod_platform_id: str, video_id: int, streamer: str, last_ingested_seconds: int, last_seen_duration_seconds: int) -> None:
        self.vod_state[vod_platform_id] = {'vod_platform_id': vod_platform_id, 'video_id': int(video_id), 'streamer': streamer, 'last_ingested_seconds': int(last_ingested_seconds), 'last_seen_duration_seconds': int(last_seen_duration_seconds), 'updated_at': 'now'}

    def delete_vod_ingest_state(self, vod_platform_id: str) -> None:
        self.vod_state.pop(vod_platform_id, None)

class TestLiveArchiveVODSource:

    def _make_source(
        self,
        tmp: str,
        live_sequence: list[bool],
        *,
        vod_sequence: list[dict[str, object]] | None = None,
    ) -> LiveArchiveVODSource:
        store = FakeStore()
        monitor = FakeMonitor(live_sequence=live_sequence, vod_sequence=vod_sequence)
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
            assert source.store.creators['https://twitch.tv/alice'][3] == 'https://static-cdn.jtvnw.net/jtv_user_pictures/alice.png'
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
            assert source.store.metadata_updates == [
                {
                    'video_id': source.video_id,
                    'title': None,
                    'thumbnail_url': None,
                    'processed': False,
                },
                {
                    'video_id': source.video_id,
                    'title': 'Live stream',
                    'thumbnail_url': 'https://static-cdn.jtvnw.net/cf_vods/thumb-320x180.jpg',
                    'processed': None,
                },
            ]

    def test_refresh_updates_creator_profile_image_when_it_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = self._make_source(
                tmp,
                live_sequence=[True, True],
                vod_sequence=[
                    {
                        'id': 'vod-1',
                        'url': 'https://www.twitch.tv/videos/vod-1',
                        'title': 'Live stream',
                        'thumbnail_url': 'https://static-cdn.jtvnw.net/cf_vods/thumb-320x180.jpg',
                        'duration_seconds': 240,
                    },
                    {
                        'id': 'vod-1',
                        'url': 'https://www.twitch.tv/videos/vod-1',
                        'title': 'Live stream',
                        'thumbnail_url': 'https://static-cdn.jtvnw.net/cf_vods/thumb-320x180.jpg',
                        'duration_seconds': 300,
                    },
                ],
            )
            source.twitch_monitor.user_profile_sequence = [
                {
                    'id': 'user-1',
                    'login': 'alice',
                    'display_name': 'alice',
                    'profile_image_url': 'https://static-cdn.jtvnw.net/jtv_user_pictures/alice-old.png',
                },
                {
                    'id': 'user-1',
                    'login': 'alice',
                    'display_name': 'alice',
                    'profile_image_url': 'https://static-cdn.jtvnw.net/jtv_user_pictures/alice-new.png',
                },
            ]
            source.start()
            source.next_chunk()
            assert source.store.creators['https://twitch.tv/alice'][3] == 'https://static-cdn.jtvnw.net/jtv_user_pictures/alice-new.png'
            assert source.store.creator_metadata_updates == [
                {
                    'creator_id': 1,
                    'name': None,
                    'profile_image_url': 'https://static-cdn.jtvnw.net/jtv_user_pictures/alice-new.png',
                }
            ]

    def test_refresh_updates_thumbnail_when_vod_metadata_fills_in_later(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = self._make_source(
                tmp,
                live_sequence=[True, True],
                vod_sequence=[
                    {
                        'id': 'vod-1',
                        'url': 'https://www.twitch.tv/videos/vod-1',
                        'title': 'Live stream',
                        'thumbnail_url': None,
                        'duration_seconds': 240,
                    },
                    {
                        'id': 'vod-1',
                        'url': 'https://www.twitch.tv/videos/vod-1',
                        'title': 'Live stream',
                        'thumbnail_url': 'https://static-cdn.jtvnw.net/cf_vods/thumb-320x180.jpg',
                        'duration_seconds': 300,
                    },
                ],
            )
            source.start()
            row = source.store.get_video_by_url('https://www.twitch.tv/videos/vod-1')
            assert row is not None
            assert row[4] is None

            source.next_chunk()

            row = source.store.get_video_by_url('https://www.twitch.tv/videos/vod-1')
            assert row is not None
            assert row[4] == 'https://static-cdn.jtvnw.net/cf_vods/thumb-320x180.jpg'
            assert source.store.metadata_updates == [
                {
                    'video_id': source.video_id,
                    'title': 'Live stream',
                    'thumbnail_url': 'https://static-cdn.jtvnw.net/cf_vods/thumb-320x180.jpg',
                    'processed': None,
                },
            ]

    def test_refresh_skips_redundant_metadata_writes_when_values_are_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = self._make_source(
                tmp,
                live_sequence=[True, True, True],
                vod_sequence=[
                    {
                        'id': 'vod-1',
                        'url': 'https://www.twitch.tv/videos/vod-1',
                        'title': 'Live stream',
                        'thumbnail_url': 'https://static-cdn.jtvnw.net/cf_vods/thumb-320x180.jpg',
                        'duration_seconds': 240,
                    },
                    {
                        'id': 'vod-1',
                        'url': 'https://www.twitch.tv/videos/vod-1',
                        'title': 'Live stream',
                        'thumbnail_url': 'https://static-cdn.jtvnw.net/cf_vods/thumb-320x180.jpg',
                        'duration_seconds': 300,
                    },
                    {
                        'id': 'vod-1',
                        'url': 'https://www.twitch.tv/videos/vod-1',
                        'title': 'Live stream',
                        'thumbnail_url': 'https://static-cdn.jtvnw.net/cf_vods/thumb-320x180.jpg',
                        'duration_seconds': 360,
                    },
                ],
            )
            source.start()
            source.next_chunk()
            source.next_chunk()

            assert source.store.metadata_updates == []

    def test_refresh_updates_title_once_when_same_vod_title_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = self._make_source(
                tmp,
                live_sequence=[True, True, True],
                vod_sequence=[
                    {
                        'id': 'vod-1',
                        'url': 'https://www.twitch.tv/videos/vod-1',
                        'title': 'Original title',
                        'thumbnail_url': 'https://static-cdn.jtvnw.net/cf_vods/thumb-320x180.jpg',
                        'duration_seconds': 240,
                    },
                    {
                        'id': 'vod-1',
                        'url': 'https://www.twitch.tv/videos/vod-1',
                        'title': 'Updated live title',
                        'thumbnail_url': 'https://static-cdn.jtvnw.net/cf_vods/thumb-320x180.jpg',
                        'duration_seconds': 300,
                    },
                    {
                        'id': 'vod-1',
                        'url': 'https://www.twitch.tv/videos/vod-1',
                        'title': 'Updated live title',
                        'thumbnail_url': 'https://static-cdn.jtvnw.net/cf_vods/thumb-320x180.jpg',
                        'duration_seconds': 360,
                    },
                ],
            )
            source.start()
            source.next_chunk()
            source.next_chunk()

            row = source.store.get_video_by_url('https://www.twitch.tv/videos/vod-1')
            assert row is not None
            assert row[3] == 'Updated live title'
            assert source.store.metadata_updates == [
                {
                    'video_id': source.video_id,
                    'title': 'Updated live title',
                    'thumbnail_url': None,
                    'processed': None,
                },
            ]
