import os
import subprocess
import tempfile
from unittest.mock import patch

from sources.historical_archive_vod_source import (
    HistoricalArchiveVODSource,
    parse_hls_media_playlist,
    select_hls_chunk,
)

class FakeStore:

    def __init__(self):
        self._creator_id = 0
        self._video_id = 0
        self.creators: dict[str, tuple[int, str, str, str | None]] = {}
        self.videos_by_url: dict[str, tuple[int, int, str, str, str | None, bool]] = {}
        self.vod_state: dict[str, dict] = {}
        self.video_status_by_id: dict[int, str | None] = {}

    def create_or_get_creator(self, name: str, url: str, profile_image_url: str | None = None) -> int:
        existing = self.creators.get(url)
        if existing is not None:
            self.creators[url] = (existing[0], name, url, profile_image_url if profile_image_url is not None else existing[3])
            return existing[0]
        self._creator_id += 1
        self.creators[url] = (self._creator_id, name, url, profile_image_url)
        return self._creator_id

    def get_video_by_url(self, url: str):
        return self.videos_by_url.get(url)

    def create_video(self, creator_id: int, url: str, title: str, processed: bool, thumbnail_url: str | None=None, streamed_at=None, status: str | None = None) -> int:
        self._video_id += 1
        resolved_status = status if status is not None else ('searchable' if bool(processed) else 'indexing')
        row = (self._video_id, int(creator_id), url, title, thumbnail_url, bool(processed), streamed_at)
        self.videos_by_url[url] = row
        self.video_status_by_id[self._video_id] = resolved_status
        return self._video_id

    def update_video_metadata(self, video_id: int, *, title: str | None=None, thumbnail_url: str | None=None, processed: bool | None=None, streamed_at=None, status: str | None = None) -> None:
        for url, row in list(self.videos_by_url.items()):
            if int(row[0]) != int(video_id):
                continue
            self.videos_by_url[url] = (row[0], row[1], row[2], title if title is not None else row[3], thumbnail_url if thumbnail_url is not None else row[4], bool(processed) if processed is not None else row[5], streamed_at if streamed_at is not None else row[6])
            if status is not None:
                self.video_status_by_id[int(video_id)] = status
            return

    def mark_video_processed(self, video_id: int, processed: bool=True) -> None:
        for url, row in list(self.videos_by_url.items()):
            if int(row[0]) == int(video_id):
                self.videos_by_url[url] = (row[0], row[1], row[2], row[3], row[4], bool(processed), row[6])
                return

    def get_video_status(self, video_id: int):
        return self.video_status_by_id.get(int(video_id))

    def update_video_status(self, video_id: int, status: str) -> None:
        self.video_status_by_id[int(video_id)] = status
        processed = status != 'indexing'
        for url, row in list(self.videos_by_url.items()):
            if int(row[0]) == int(video_id):
                self.videos_by_url[url] = (row[0], row[1], row[2], row[3], row[4], processed, row[6])
                return

    def get_vod_ingest_state(self, vod_platform_id: str):
        return self.vod_state.get(vod_platform_id)

    def upsert_vod_ingest_state(self, vod_platform_id: str, video_id: int, streamer: str, last_ingested_seconds: int, last_seen_duration_seconds: int) -> None:
        self.vod_state[vod_platform_id] = {'vod_platform_id': vod_platform_id, 'video_id': int(video_id), 'streamer': streamer, 'last_ingested_seconds': int(last_ingested_seconds), 'last_seen_duration_seconds': int(last_seen_duration_seconds), 'updated_at': 'now'}

    def delete_vod_ingest_state(self, vod_platform_id: str) -> None:
        self.vod_state.pop(vod_platform_id, None)

class TestHistoricalArchiveVODSource:

    def test_selects_only_overlapping_hls_segments_with_local_seek(self) -> None:
        playlist = parse_hls_media_playlist(
            """#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:10
#EXT-X-MEDIA-SEQUENCE:50
#EXTINF:10.0,
segment-0.ts
#EXTINF:10.0,
segment-1.ts
#EXTINF:10.0,
segment-2.ts
#EXTINF:10.0,
segment-3.ts
#EXT-X-ENDLIST
""",
            "https://cdn.example.test/path/index.m3u8",
        )

        selection = select_hls_chunk(playlist, start_seconds=15.0, duration_seconds=12.0)

        assert selection.local_seek_seconds == 5.0
        assert selection.first_segment_start_seconds == 10.0
        assert "#EXT-X-MEDIA-SEQUENCE:51" in selection.manifest
        assert "https://cdn.example.test/path/segment-0.ts" not in selection.manifest
        assert "https://cdn.example.test/path/segment-1.ts" in selection.manifest
        assert "https://cdn.example.test/path/segment-2.ts" in selection.manifest
        assert "https://cdn.example.test/path/segment-3.ts" not in selection.manifest

    def test_extracts_from_local_hls_subset_and_validates_duration(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = HistoricalArchiveVODSource(
                streamer='alice',
                vod_metadata=self._make_vod(),
                store=FakeStore(),
                chunk_seconds=60,
                temp_dir=f'{tmp}/chunks',
            )
            os.makedirs(source.temp_dir, exist_ok=True)
            playlist = parse_hls_media_playlist(
                """#EXTM3U
#EXT-X-TARGETDURATION:10
#EXTINF:10.0,
segment-0.ts
#EXTINF:10.0,
segment-1.ts
#EXTINF:10.0,
segment-2.ts
#EXT-X-ENDLIST
""",
                "https://cdn.example.test/index.m3u8",
            )
            ffmpeg_calls: list[list[str]] = []
            ffmpeg_options: list[dict[str, object]] = []
            subset_manifests: list[str] = []

            def fake_run(command, **kwargs):
                if command[0] == 'ffmpeg':
                    ffmpeg_calls.append(list(command))
                    ffmpeg_options.append(dict(kwargs))
                    subset_path = command[command.index('-i') + 1]
                    with open(subset_path, encoding='utf-8') as handle:
                        subset_manifests.append(handle.read())
                    output_path = command[-1]
                    with open(output_path, 'wb') as handle:
                        handle.write(b'R' * 1_000)
                    return subprocess.CompletedProcess(command, 0, '', '')
                if command[0] == 'ffprobe':
                    return subprocess.CompletedProcess(command, 0, '12.000\n', '')
                raise AssertionError(command)

            with patch.object(source, '_load_media_playlist', return_value=playlist), patch(
                'sources.historical_archive_vod_source.subprocess.run', side_effect=fake_run
            ):
                output_path = source._extract_chunk(15.0, 12.0)

            assert os.path.exists(output_path)
            assert len(ffmpeg_calls) == 1
            command = ffmpeg_calls[0]
            assert command.index('-i') < command.index('-ss')
            assert command[command.index('-ss') + 1] == '5.000'
            assert ffmpeg_options[0]['timeout'] == 180.0
            assert 'segment-0.ts' not in subset_manifests[0]
            assert 'segment-1.ts' in subset_manifests[0]
            assert 'segment-2.ts' in subset_manifests[0]
            assert not os.path.exists(f'{output_path}.m3u8')

    def _make_vod(self) -> dict[str, object]:
        return {'id': 'vod-1', 'url': 'https://www.twitch.tv/videos/vod-1', 'title': 'Historical stream', 'thumbnail_url': 'https://static-cdn.jtvnw.net/thumb.jpg', 'duration_seconds': 180}

    def test_resumes_from_saved_cursor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = FakeStore()
            creator_id = store.create_or_get_creator('alice', 'https://twitch.tv/alice')
            video_id = store.create_video(creator_id=creator_id, url='https://www.twitch.tv/videos/vod-1', title='Old', thumbnail_url=None, processed=False)
            store.upsert_vod_ingest_state('vod-1', video_id, 'alice', 120, 180)
            source = HistoricalArchiveVODSource(streamer='alice', vod_metadata=self._make_vod(), store=store, creator_metadata={'profile_image_url': 'https://cdn/alice.png'}, chunk_seconds=60, temp_dir=f'{tmp}/chunks')

            extraction_calls: list[tuple[float, float]] = []

            def fake_extract_chunk(start_seconds: float, duration_seconds: float) -> str:
                extraction_calls.append((start_seconds, duration_seconds))
                out = os.path.join(source.temp_dir, f'chunk_{start_seconds}_{duration_seconds}.wav')
                os.makedirs(source.temp_dir, exist_ok=True)
                with open(out, 'wb') as handle:
                    handle.write(b'fake')
                return out
            source._extract_chunk = fake_extract_chunk
            source.start()
            assert store.creators['https://twitch.tv/alice'][3] == 'https://cdn/alice.png'
            assert source.ingest_cursor_seconds == 120
            assert store.get_video_status(video_id) == 'indexing'
            chunk = source.next_chunk()
            assert chunk is not None
            assert chunk is not None
            assert chunk.offset_seconds == 119.5
            assert chunk.duration_seconds == 60.5
            assert extraction_calls == [(119.5, 60.5)]
            assert store.vod_state['vod-1']['last_ingested_seconds'] == 120
            source.next_chunk()
            assert source.ingest_cursor_seconds == 180
            assert source.is_finished

    def test_finalize_marks_video_processed_and_clears_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = FakeStore()
            source = HistoricalArchiveVODSource(streamer='alice', vod_metadata=self._make_vod(), store=store, creator_metadata={'profile_image_url': 'https://cdn/alice.png'}, chunk_seconds=180, temp_dir=f'{tmp}/chunks')

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
            assert store.get_video_status(source.video_id) == 'searchable'
            assert 'vod-1' not in store.vod_state
            assert source.is_finished

    def test_reindex_requested_vod_clears_stale_cursor_and_restarts_from_zero(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = FakeStore()
            creator_id = store.create_or_get_creator('alice', 'https://twitch.tv/alice')
            video_id = store.create_video(
                creator_id=creator_id,
                url='https://www.twitch.tv/videos/vod-1',
                title='Old',
                thumbnail_url=None,
                processed=True,
                status='reindex_requested',
            )
            store.upsert_vod_ingest_state('vod-1', video_id, 'alice', 120, 180)
            source = HistoricalArchiveVODSource(
                streamer='alice',
                vod_metadata=self._make_vod(),
                store=store,
                creator_metadata={'profile_image_url': 'https://cdn/alice.png'},
                chunk_seconds=60,
                temp_dir=f'{tmp}/chunks',
            )

            def fake_extract_chunk(start_seconds: int, duration_seconds: int) -> str:
                out = os.path.join(source.temp_dir, f'chunk_{start_seconds}_{duration_seconds}.wav')
                os.makedirs(source.temp_dir, exist_ok=True)
                with open(out, 'wb') as handle:
                    handle.write(b'fake')
                return out

            source._extract_chunk = fake_extract_chunk
            source.start()

            assert source.ingest_cursor_seconds == 0
            assert store.get_video_status(video_id) == 'indexing'
            assert store.vod_state['vod-1']['last_ingested_seconds'] == 0

    def test_stop_does_not_commit_pending_progress(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = FakeStore()
            source = HistoricalArchiveVODSource(
                streamer='alice',
                vod_metadata=self._make_vod(),
                store=store,
                creator_metadata={'profile_image_url': 'https://cdn/alice.png'},
                chunk_seconds=60,
                temp_dir=f'{tmp}/chunks',
            )

            def fake_extract_chunk(start_seconds: int, duration_seconds: int) -> str:
                out = os.path.join(source.temp_dir, f'chunk_{start_seconds}_{duration_seconds}.wav')
                os.makedirs(source.temp_dir, exist_ok=True)
                with open(out, 'wb') as handle:
                    handle.write(b'fake')
                return out

            source._extract_chunk = fake_extract_chunk
            source.start()
            chunk = source.next_chunk()

            assert chunk is not None
            assert store.vod_state['vod-1']['last_ingested_seconds'] == 0

            source.stop()

            assert store.vod_state['vod-1']['last_ingested_seconds'] == 0
