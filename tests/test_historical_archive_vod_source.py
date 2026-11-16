import io
import os
import subprocess
import tempfile
from dataclasses import replace
from datetime import datetime, timezone
from unittest.mock import Mock, patch
import numpy as np
import pytest

from pipeline.ingest_session import IngestSession
from runners.run_hybrid_ingest import _build_backlog
from services.twitch_monitor import TwitchMonitor
from sources.historical_archive_vod_source import (
    HistoricalArchiveVODSource,
    parse_hls_media_playlist,
    select_hls_chunk,
)
from storage.records import VodIngestStateRecord, VideoRecord, VideoStatus

class FakeStore:

    def __init__(self):
        self._creator_id = 0
        self._video_id = 0
        self.creators: dict[str, tuple[int, str, str, str | None]] = {}
        self.videos_by_url: dict[str, VideoRecord] = {}
        self.vod_state: dict[str, VodIngestStateRecord] = {}
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
        resolved_status = VideoStatus(status) if status is not None else (
            VideoStatus.SEARCHABLE if bool(processed) else VideoStatus.INDEXING
        )
        row = VideoRecord(
            id=self._video_id,
            creator_id=int(creator_id),
            url=url,
            title=title,
            thumbnail_url=thumbnail_url,
            status=resolved_status,
            processed=resolved_status != VideoStatus.INDEXING,
            streamed_at=streamed_at,
        )
        self.videos_by_url[url] = row
        self.video_status_by_id[self._video_id] = resolved_status.value
        return self._video_id

    def update_video_metadata(self, video_id: int, *, title: str | None=None, thumbnail_url: str | None=None, processed: bool | None=None, streamed_at=None, status: str | None = None) -> None:
        for url, row in list(self.videos_by_url.items()):
            if row.id != int(video_id):
                continue
            if status is not None:
                resolved_status = VideoStatus(status)
                resolved_processed = resolved_status != VideoStatus.INDEXING
            else:
                resolved_status = row.status
                resolved_processed = bool(processed) if processed is not None else row.processed
            self.videos_by_url[url] = replace(
                row,
                title=title if title is not None else row.title,
                thumbnail_url=thumbnail_url if thumbnail_url is not None else row.thumbnail_url,
                processed=resolved_processed,
                status=resolved_status,
                streamed_at=streamed_at if streamed_at is not None else row.streamed_at,
            )
            if status is not None:
                self.video_status_by_id[int(video_id)] = resolved_status.value
            return

    def mark_video_processed(self, video_id: int, processed: bool=True) -> None:
        for url, row in list(self.videos_by_url.items()):
            if row.id == int(video_id):
                self.videos_by_url[url] = replace(
                    row,
                    processed=bool(processed),
                    status=VideoStatus.SEARCHABLE if processed else VideoStatus.INDEXING,
                )
                self.video_status_by_id[int(video_id)] = "searchable" if processed else "indexing"
                return

    def get_video_status(self, video_id: int):
        value = self.video_status_by_id.get(int(video_id))
        return VideoStatus(value) if value is not None else None

    def update_video_status(self, video_id: int, status: str) -> None:
        self.video_status_by_id[int(video_id)] = status
        processed = status != 'indexing'
        for url, row in list(self.videos_by_url.items()):
            if row.id == int(video_id):
                self.videos_by_url[url] = replace(
                    row,
                    processed=processed,
                    status=VideoStatus(status),
                )
                return

    def get_vod_ingest_state(self, vod_platform_id: str):
        return self.vod_state.get(vod_platform_id)

    def upsert_vod_ingest_state(self, vod_platform_id: str, video_id: int, streamer: str, last_ingested_seconds: int, last_seen_duration_seconds: int) -> None:
        self.vod_state[vod_platform_id] = VodIngestStateRecord(
            vod_platform_id=vod_platform_id,
            video_id=int(video_id),
            streamer=streamer,
            last_ingested_seconds=int(last_ingested_seconds),
            last_seen_duration_seconds=int(last_seen_duration_seconds),
            updated_at=datetime.now(timezone.utc),
        )

    def delete_vod_ingest_state(self, vod_platform_id: str) -> None:
        self.vod_state.pop(vod_platform_id, None)


class FakeIngestStates:
    def __init__(self, store: FakeStore):
        self.store = store

    def get(self, vod_platform_id: str):
        return self.store.get_vod_ingest_state(vod_platform_id)

    def upsert(self, **kwargs) -> None:
        self.store.upsert_vod_ingest_state(**kwargs)

    def delete(self, vod_platform_id: str) -> None:
        self.store.delete_vod_ingest_state(vod_platform_id)

    def list_pending(self, streamer: str):
        return [state for state in self.store.vod_state.values() if state.streamer == streamer]


@pytest.fixture
def archive(tmp_path):
    store = FakeStore()
    vod = {'id': 'vod-1', 'url': 'https://www.twitch.tv/videos/vod-1', 'duration_seconds': 10}
    monitor = Mock(spec=TwitchMonitor)
    monitor.get_user_id.return_value = 'user-1'
    monitor.get_archive_vod_by_id.return_value = vod
    monitor.get_live_streamers.return_value = set()
    events = []
    source = HistoricalArchiveVODSource(
        streamer='alice', vod_metadata=vod, videos=store,
        ingest_states=FakeIngestStates(store), twitch_monitor=monitor,
        temp_dir=str(tmp_path / 'chunks'), progress_callback=events.append,
    )

    def extract(start_seconds, duration_seconds):
        path = os.path.join(source.temp_dir, f'{start_seconds}.wav')
        with open(path, 'wb') as handle:
            handle.write(b'fake')
        return path

    source._extract_chunk = Mock(side_effect=extract)
    source._load_media_playlist = Mock(return_value=parse_hls_media_playlist(
        '#EXTM3U\n#EXTINF:10,\nsegment.ts\n#EXT-X-ENDLIST\n',
        'https://cdn.example.test/index.m3u8',
    ))
    return source, store, monitor, events

class TestHistoricalArchiveVODSource:

    def _make_monitor(self):
        monitor = Mock(spec=TwitchMonitor)
        monitor.get_user_id.return_value = 'user-1'
        monitor.get_archive_vod_by_id.return_value = self._make_vod()
        monitor.get_live_streamers.return_value = set()
        return monitor

    def _make_playlist(self, duration=180, *, endlist=True, media_sequence=0):
        return parse_hls_media_playlist(
            f'#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:{media_sequence}\n#EXTINF:{duration},\nsegment.ts\n'
            + ('#EXT-X-ENDLIST\n' if endlist else ''),
            'https://cdn.example.test/index.m3u8',
        )

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
            store = FakeStore()
            source = HistoricalArchiveVODSource(
                streamer='alice',
                vod_metadata=self._make_vod(),
                videos=store,
                ingest_states=FakeIngestStates(store),
                twitch_monitor=self._make_monitor(),
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

    def test_unavailable_source_refuses_start_before_mutating_storage_or_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = FakeStore()
            creator_id = store.create_or_get_creator('alice', 'https://twitch.tv/alice')
            video_id = store.create_video(
                creator_id=creator_id, url='https://www.twitch.tv/videos/vod-1',
                title='Preserved title', processed=True, status='unavailable',
            )
            store.upsert_vod_ingest_state('vod-1', video_id, 'alice', 120, 180)
            expected_video = store.get_video_by_url('https://www.twitch.tv/videos/vod-1')
            expected_state = store.vod_state['vod-1']
            expected_creators = dict(store.creators)
            source = HistoricalArchiveVODSource(
                streamer='alice', vod_metadata=self._make_vod(), videos=store,
                ingest_states=FakeIngestStates(store),
                twitch_monitor=self._make_monitor(),
                creator_metadata={'profile_image_url': 'https://cdn/new-profile.png'},
                temp_dir=f'{tmp}/chunks',
            )

            with pytest.raises(RuntimeError, match='VOD source is unavailable: vod-1'):
                source.start()

            assert store.get_video_by_url('https://www.twitch.tv/videos/vod-1') == expected_video
            assert store.vod_state['vod-1'] == expected_state
            assert store.creators == expected_creators
            assert not os.path.exists(source.temp_dir)
            assert source.video_id is None

    def test_resumes_from_saved_cursor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = FakeStore()
            creator_id = store.create_or_get_creator('alice', 'https://twitch.tv/alice')
            video_id = store.create_video(creator_id=creator_id, url='https://www.twitch.tv/videos/vod-1', title='Old', thumbnail_url=None, processed=False)
            store.upsert_vod_ingest_state('vod-1', video_id, 'alice', 120, 180)
            source = HistoricalArchiveVODSource(streamer='alice', vod_metadata=self._make_vod(), videos=store, ingest_states=FakeIngestStates(store), twitch_monitor=self._make_monitor(), creator_metadata={'profile_image_url': 'https://cdn/alice.png'}, chunk_seconds=60, temp_dir=f'{tmp}/chunks')

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
            assert store.vod_state['vod-1'].last_ingested_seconds == 120
            with patch.object(source, '_load_media_playlist', return_value=self._make_playlist()):
                source.next_chunk()
            assert source.ingest_cursor_seconds == 180
            assert source.is_finished

    def test_discovery_duration_does_not_complete_a_growing_archive(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = FakeStore()
            vod = {**self._make_vod(), 'duration_seconds': 10}
            events: list[dict[str, object]] = []
            monitor = Mock(spec=TwitchMonitor)
            monitor.get_archive_vod_by_id.return_value = vod
            monitor.get_live_streamers.return_value = {'alice'}
            monitor.get_latest_archive_vod.return_value = vod
            source = HistoricalArchiveVODSource(
                streamer='alice',
                vod_metadata=vod,
                videos=store,
                ingest_states=FakeIngestStates(store),
                twitch_monitor=monitor,
                creator_metadata={'id': 'user-1'},
                chunk_seconds=60,
                temp_dir=f'{tmp}/chunks',
                progress_callback=events.append,
            )
            playlist = parse_hls_media_playlist(
                """#EXTM3U
#EXT-X-TARGETDURATION:10
#EXTINF:10.0,
segment-0.ts
""",
                'https://cdn.example.test/index.m3u8',
            )

            def fake_extract_chunk(start_seconds: float, duration_seconds: float) -> str:
                out = os.path.join(source.temp_dir, 'chunk.wav')
                with open(out, 'wb') as handle:
                    handle.write(b'fake')
                return out

            with patch.object(source, '_extract_chunk', side_effect=fake_extract_chunk), patch.object(
                source, '_load_media_playlist', return_value=playlist
            ):
                source.start()
                chunk = source.next_chunk()
                assert chunk is not None
                assert chunk.offset_seconds == 0
                assert chunk.duration_seconds == 10
                assert store.vod_state['vod-1'].last_ingested_seconds == 0

                assert source.next_chunk() is None

            row = store.get_video_by_url(str(vod['url']))
            assert row is not None
            assert row.status.value == 'indexing'
            assert store.vod_state['vod-1'].last_ingested_seconds == 10
            assert not source.is_finished
            assert all(event['event'] != 'vod_complete' for event in events)

    def test_finalize_marks_video_processed_and_clears_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = FakeStore()
            source = HistoricalArchiveVODSource(streamer='alice', vod_metadata=self._make_vod(), videos=store, ingest_states=FakeIngestStates(store), twitch_monitor=self._make_monitor(), creator_metadata={'profile_image_url': 'https://cdn/alice.png'}, chunk_seconds=180, temp_dir=f'{tmp}/chunks')

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
            with patch.object(source, '_load_media_playlist', return_value=self._make_playlist()):
                source.next_chunk()
            row = store.get_video_by_url('https://www.twitch.tv/videos/vod-1')
            assert row is not None
            assert row is not None
            assert row.processed
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
                videos=store,
                ingest_states=FakeIngestStates(store),
                twitch_monitor=self._make_monitor(),
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
            assert store.vod_state['vod-1'].last_ingested_seconds == 0

    def test_stop_does_not_commit_pending_progress(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = FakeStore()
            source = HistoricalArchiveVODSource(
                streamer='alice',
                vod_metadata=self._make_vod(),
                videos=store,
                ingest_states=FakeIngestStates(store),
                twitch_monitor=self._make_monitor(),
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
            assert store.vod_state['vod-1'].last_ingested_seconds == 0

            source.stop()

            assert store.vod_state['vod-1'].last_ingested_seconds == 0

    def test_session_indexes_a_growing_tail_and_finishes_only_after_coverage(self, archive) -> None:
        source, store, monitor, events = archive
        source.completion_poll_seconds = 0
        initial = dict(source.vod_metadata)
        grown = {**initial, 'duration_seconds': 180}
        monitor.get_archive_vod_by_id.side_effect = [initial, grown, grown]
        monitor.get_live_streamers.side_effect = [{'alice'}, set(), set()]
        monitor.get_latest_archive_vod.return_value = initial
        source._load_media_playlist.side_effect = [
            self._make_playlist(10, endlist=False),
            self._make_playlist(180),
            self._make_playlist(180),
        ]
        written_offsets = []

        class Embedder:
            def embed(self, audio_path, offset_seconds=0):
                return np.ones((1, 2)), np.array([offset_seconds])

        class Fingerprints:
            def store_fingerprints(self, video_id, timestamps):
                written_offsets.extend(timestamps.tolist())
                return [len(written_offsets)]

            def append_vectors(self, embeddings, ids, creator_id):
                pass

        def waiting_for_tail(_seconds):
            assert store.get_video_status(source.video_id) == 'indexing'
            assert store.vod_state['vod-1'].last_ingested_seconds == 10
            monitor.list_archive_vods_since.return_value = [initial]
            backlog = _build_backlog(
                twitch_monitor=monitor, videos=store, ingest_states=source.ingest_states,
                streamer='alice', user_id='user-1', days=1,
                skipped_vods_logged=set(), out=lambda _line: None,
            )
            assert len(backlog) == 1
            assert backlog[0].existing_state.last_ingested_seconds == 10

        with patch('pipeline.ingest_session.time.sleep', side_effect=waiting_for_tail) as sleep:
            IngestSession(source, Embedder(), Fingerprints(), poll_interval=0).run()

        assert sleep.call_count == 1
        assert written_offsets == [0, 9.5, 69.5, 129.5]
        assert store.get_video_status(source.video_id) == 'searchable'
        assert 'vod-1' not in store.vod_state
        assert [event['event'] for event in events].count('vod_complete') == 1
        assert events[-1]['duration_seconds'] == 180

    @pytest.mark.parametrize('endlist,live,latest', [
        (False, False, None), (True, True, 'vod-1'), (True, True, None),
    ])
    def test_incomplete_evidence_yields_and_paces_checks(self, archive, endlist, live, latest) -> None:
        source, store, monitor, events = archive
        monitor.get_live_streamers.return_value = {'alice'} if live else set()
        monitor.get_latest_archive_vod.return_value = {'id': latest} if latest else None
        source._load_media_playlist.return_value = self._make_playlist(10, endlist=endlist)
        with patch('sources.historical_archive_vod_source.time.monotonic', return_value=100) as now, patch(
            'sources.historical_archive_vod_source.time.sleep', side_effect=AssertionError('source slept')
        ):
            source.start()
            assert source.next_chunk() is not None
            for _ in range(100):
                assert source.next_chunk() is None
            assert monitor.get_archive_vod_by_id.call_count == 1
            assert source._load_media_playlist.call_count == 1
            now.return_value = 115
            assert source.next_chunk() is None
            assert monitor.get_archive_vod_by_id.call_count == 2

        assert not source.is_finished
        assert store.get_video_status(source.video_id) == 'indexing'
        assert store.vod_state['vod-1'].last_ingested_seconds == 10
        assert all(event['event'] != 'vod_complete' for event in events)

    def test_old_closed_archive_completes_while_another_archive_is_live(self, archive) -> None:
        source, store, monitor, events = archive
        monitor.get_live_streamers.return_value = {'alice'}
        monitor.get_archive_vod_by_id.return_value = {
            **source.vod_metadata, 'created_at': '2026-11-14T01:00:00Z',
        }
        monitor.get_latest_archive_vod.return_value = {
            'id': 'new-vod', 'created_at': '2026-11-15T01:00:00Z',
        }
        source.start()
        source.next_chunk()
        assert source.next_chunk() is None
        assert source.is_finished
        assert store.get_video_status(source.video_id) == 'searchable'
        assert 'vod-1' not in store.vod_state
        monitor.get_archive_vod_by_id.assert_called_with('vod-1', expected_user_id='user-1')
        monitor.get_user_id.assert_called_once_with('alice')
        monitor.is_live.assert_not_called()

    @pytest.mark.parametrize('target_created,latest_created', [
        ('2026-11-15T01:00:00Z', '2026-11-14T01:00:00Z'),
        ('2026-11-15T01:00:00Z', '2026-11-15T01:00:00Z'),
        ('2026-11-15T01:00:00Z', None),
        ('2026-11-15T01:00:00Z', 'invalid'),
        ('2026-11-15T01:00:00Z', '2026-11-16T01:00:00'),
        (None, '2026-11-16T01:00:00Z'),
        ('invalid', '2026-11-16T01:00:00Z'),
    ])
    def test_live_archive_listing_must_prove_the_target_is_older(self, archive, target_created, latest_created) -> None:
        source, store, monitor, events = archive
        monitor.get_live_streamers.return_value = {'alice'}
        monitor.get_archive_vod_by_id.return_value = {
            **source.vod_metadata, 'created_at': target_created,
        }
        monitor.get_latest_archive_vod.return_value = {
            'id': 'different-vod', 'created_at': latest_created,
        }
        source.start()
        source.next_chunk()
        assert source.next_chunk() is None
        assert store.get_video_status(source.video_id) == 'indexing'
        assert store.vod_state['vod-1'].last_ingested_seconds == 10
        assert not source.is_finished
        assert all(event['event'] != 'vod_complete' for event in events)

    def test_closed_playlist_can_extend_stale_metadata_and_floors_fractional_tail(self, archive) -> None:
        source, store, monitor, events = archive
        source.completion_poll_seconds = 0
        source._load_media_playlist.return_value = self._make_playlist(30.9)
        source.start()
        first = source.next_chunk()
        tail = source.next_chunk()
        assert first.duration_seconds == 10
        assert tail.offset_seconds == 9.5
        assert tail.duration_seconds == 20.5
        assert store.vod_state['vod-1'].last_ingested_seconds == 10
        assert store.vod_state['vod-1'].last_seen_duration_seconds == 30
        assert source.next_chunk() is None
        assert store.get_video_status(source.video_id) == 'searchable'
        assert events[-1]['duration_seconds'] == 30

    @pytest.mark.parametrize('cursor,observed', [(30, 60), (60, 30)])
    def test_resume_never_rewinds_known_cursor_or_extent(self, archive, cursor, observed) -> None:
        source, store, monitor, events = archive
        creator_id = store.create_or_get_creator('alice', 'https://twitch.tv/alice')
        video_id = store.create_video(creator_id, source.current_vod_url, 'Old', False)
        store.upsert_vod_ingest_state('vod-1', video_id, 'alice', cursor, observed)
        source.start()
        assert source.ingest_cursor_seconds == cursor
        assert store.vod_state['vod-1'].last_seen_duration_seconds == 60
        if cursor == 30:
            chunk = source.next_chunk()
            assert chunk.offset_seconds == 29.5
        assert source.next_chunk() is None
        assert store.vod_state['vod-1'].last_ingested_seconds == 60
        assert store.vod_state['vod-1'].last_seen_duration_seconds == 60
        assert store.get_video_status(video_id) == 'indexing'
        assert not source.is_finished

    @pytest.mark.parametrize('failure', ['lookup', 'manifest', 'live', 'missing', 'persist'])
    def test_completion_failure_retains_committed_progress(self, archive, failure) -> None:
        source, store, monitor, events = archive
        source.start()
        source.next_chunk()
        if failure == 'lookup':
            monitor.get_archive_vod_by_id.side_effect = RuntimeError('lookup failed')
        elif failure == 'manifest':
            source._load_media_playlist.side_effect = RuntimeError('manifest failed')
        elif failure == 'live':
            monitor.get_live_streamers.side_effect = RuntimeError('live lookup failed')
        elif failure == 'missing':
            monitor.get_archive_vod_by_id.return_value = None
        else:
            source.ingest_states.upsert = Mock(side_effect=RuntimeError('save failed'))
        with pytest.raises(RuntimeError):
            source.next_chunk()
        assert store.get_video_status(source.video_id) == 'indexing'
        assert store.vod_state['vod-1'].last_ingested_seconds == (0 if failure == 'persist' else 10)
        assert source.ingest_cursor_seconds == (0 if failure == 'persist' else 10)
        assert not source.is_finished
        assert all(event['event'] != 'vod_complete' for event in events)

    def test_failed_tail_extraction_retains_preceding_chunk(self, archive) -> None:
        source, store, monitor, events = archive
        source.start()
        source.next_chunk()
        monitor.get_archive_vod_by_id.return_value = {**source.vod_metadata, 'duration_seconds': 60}
        source._load_media_playlist.return_value = self._make_playlist(60)
        source._extract_chunk.side_effect = RuntimeError('extraction failed')
        with pytest.raises(RuntimeError, match='extraction failed'):
            source.next_chunk()
        assert store.vod_state['vod-1'].last_ingested_seconds == 10
        assert store.vod_state['vod-1'].last_seen_duration_seconds == 60
        assert store.get_video_status(source.video_id) == 'indexing'

    @pytest.mark.parametrize('observed_duration', [10, 20])
    def test_rolling_playlist_cannot_supply_a_growing_tail(self, archive, observed_duration) -> None:
        source, store, monitor, events = archive
        source.completion_poll_seconds = 0
        del source._load_media_playlist
        monitor.get_archive_vod_by_id.return_value = {
            **source.vod_metadata, 'duration_seconds': observed_duration,
        }
        rolling = b'#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:100\n#EXTINF:10,\n100.ts\n#EXTINF:10,\n101.ts\n#EXT-X-ENDLIST\n'
        full = b'#EXTM3U\n#EXTINF:10,\n0.ts\n#EXTINF:10,\n1.ts\n#EXT-X-ENDLIST\n'
        source.start()
        source.next_chunk()
        with patch.object(source, '_resolve_media_url', return_value='https://cdn.example.test/index.m3u8'), patch(
            'sources.historical_archive_vod_source.urllib.request.urlopen',
            side_effect=[io.BytesIO(rolling), io.BytesIO(full), io.BytesIO(full)],
        ):
            with pytest.raises(RuntimeError, match='origin'):
                source.next_chunk()
            assert source._extract_chunk.call_count == 1
            assert store.get_video_status(source.video_id) == 'indexing'
            assert store.vod_state['vod-1'].last_ingested_seconds == 10
            assert not source.is_finished
            assert source._media_playlist is None

            tail = source.next_chunk()
            assert tail.offset_seconds == 9.5
            assert tail.duration_seconds == 10.5
            assert source.next_chunk() is None

        assert store.get_video_status(source.video_id) == 'searchable'
        assert 'vod-1' not in store.vod_state
        assert source.is_finished

    def test_initial_rolling_playlist_is_rejected_before_audio_extraction(self, archive) -> None:
        source, store, monitor, events = archive
        del source._extract_chunk
        del source._load_media_playlist
        rolling = b'#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:100\n#EXTINF:10,\n100.ts\n#EXT-X-ENDLIST\n'
        source.start()
        with patch.object(source, '_resolve_media_url', return_value='https://cdn.example.test/index.m3u8'), patch(
            'sources.historical_archive_vod_source.urllib.request.urlopen',
            side_effect=lambda *_args, **_kwargs: io.BytesIO(rolling),
        ), patch(
            'sources.historical_archive_vod_source.subprocess.run',
            side_effect=AssertionError('rolling playlist reached ffmpeg'),
        ) as ffmpeg:
            with pytest.raises(RuntimeError, match='origin'):
                source.next_chunk()
        ffmpeg.assert_not_called()
        assert source._media_playlist is None
        assert store.vod_state['vod-1'].last_ingested_seconds == 0
        assert store.get_video_status(source.video_id) == 'indexing'

    def test_subset_endlist_does_not_change_original_playlist_finality(self) -> None:
        playlist = self._make_playlist(10, endlist=False)
        selection = select_hls_chunk(playlist, 0, 10)
        assert '#EXT-X-ENDLIST' in selection.manifest
        assert not playlist.endlist
        assert playlist.duration_seconds == 10

    def test_completion_refresh_reloads_and_revalidates_the_original_manifest(self, archive) -> None:
        source, store, monitor, events = archive
        del source._load_media_playlist
        source._media_playlist = self._make_playlist(10)
        response = Mock()
        response.read.return_value = b'#EXTM3U\n#EXTINF:30,\ntail.ts\n'
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        with patch.object(source, '_resolve_media_url', return_value='https://cdn.example.test/index.m3u8') as resolve, patch(
            'sources.historical_archive_vod_source.urllib.request.urlopen', return_value=response,
        ) as urlopen:
            playlist = source._load_media_playlist(force_refresh=True)
        assert playlist.duration_seconds == 30
        assert not playlist.endlist
        resolve.assert_called_once_with(force_refresh=True)
        request = urlopen.call_args.args[0]
        assert request.full_url == 'https://cdn.example.test/index.m3u8'
        assert request.get_header('Cache-control') == 'no-cache'
        assert request.get_header('Pragma') == 'no-cache'

    @pytest.mark.parametrize('status', ['deleted', 'unavailable', 'reindex_requested'])
    def test_terminal_status_cannot_be_finalized(self, archive, status) -> None:
        source, store, monitor, events = archive
        source.start()
        source.next_chunk()
        store.update_video_status(source.video_id, status)
        with pytest.raises(RuntimeError, match='outside indexing status'):
            source.next_chunk()
        assert store.get_video_status(source.video_id) == status
        assert 'vod-1' in store.vod_state
        assert not source.is_finished
        assert all(event['event'] != 'vod_complete' for event in events)

    def test_searchable_video_is_not_reopened(self, archive) -> None:
        source, store, monitor, events = archive
        creator_id = store.create_or_get_creator('alice', 'https://twitch.tv/alice')
        video_id = store.create_video(creator_id, source.current_vod_url, 'Done', True)
        source.start()
        assert source.next_chunk() is None
        assert source.is_finished
        assert store.get_video_status(video_id) == 'searchable'
        assert 'vod-1' not in store.vod_state
        assert not os.path.exists(source.temp_dir)
        assert events == []
