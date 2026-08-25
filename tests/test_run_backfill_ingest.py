import pytest

from runners.run_backfill_ingest import (
    FreshNMFPReindexPreconditionError,
    main,
    run_backfill_ingest,
)


class FakeMonitor:

    def __init__(self, vods):
        self.vods = list(vods)

    def get_user_id(self, streamer: str) -> str:
        return 'user-1'

    def get_user_profile(self, streamer: str) -> dict[str, str]:
        return {
            'id': 'user-1',
            'login': streamer,
            'display_name': streamer,
            'profile_image_url': 'https://cdn/profile.png',
        }

    def list_archive_vods_since(self, user_id: str, created_after):
        return list(self.vods)

class FakeStore:

    def __init__(self):
        self.videos_by_url: dict[str, tuple[int, int, str, str, str | None, bool]] = {}
        self.vod_state: dict[str, dict] = {}
        self.video_status_by_id: dict[int, str | None] = {}
        self.deleted_vod_state_ids: list[str] = []

    def get_video_by_url(self, url: str):
        return self.videos_by_url.get(url)

    def get_video_status(self, video_id: int):
        return self.video_status_by_id.get(int(video_id))

    def get_vod_ingest_state(self, vod_platform_id: str):
        return self.vod_state.get(vod_platform_id)

    def delete_vod_ingest_state(self, vod_platform_id: str) -> None:
        self.deleted_vod_state_ids.append(vod_platform_id)
        self.vod_state.pop(vod_platform_id, None)

class FakeSource:

    def __init__(self, streamer, vod_metadata, store, chunk_seconds, temp_dir, progress_callback=None, creator_metadata=None):
        self.streamer = streamer
        self.vod_metadata = vod_metadata
        self.store = store
        self.chunk_seconds = chunk_seconds
        self.temp_dir = temp_dir
        self.progress_callback = progress_callback
        self.creator_metadata = creator_metadata

class FakeSession:

    def __init__(self, source, embedder, store, poll_interval):
        self.source = source

    def run(self) -> None:
        if self.source.vod_metadata.get('should_fail'):
            raise RuntimeError('boom')

class TestRunBackfillIngest:

    def _build_state(self, store: FakeStore):
        return {'store': store}

    def test_skips_processed_resumes_partial_and_continues_on_failure(self) -> None:
        store = FakeStore()
        store.videos_by_url['https://www.twitch.tv/videos/processed'] = (1, 1, 'https://www.twitch.tv/videos/processed', 'Processed', None, True, None)
        store.vod_state['resume'] = {'vod_platform_id': 'resume', 'video_id': 2, 'streamer': 'alice', 'last_ingested_seconds': 60, 'last_seen_duration_seconds': 120, 'updated_at': 'now'}
        monitor = FakeMonitor([{'id': 'resume', 'url': 'https://www.twitch.tv/videos/resume'}, {'id': 'processed', 'url': 'https://www.twitch.tv/videos/processed'}, {'id': 'fail', 'url': 'https://www.twitch.tv/videos/fail', 'should_fail': True}])
        logs: list[str] = []
        seen_vods: list[str] = []
        seen_creator_metadata: list[dict[str, str] | None] = []

        def source_factory(**kwargs):
            seen_vods.append(kwargs['vod_metadata']['id'])
            seen_creator_metadata.append(kwargs.get('creator_metadata'))
            return FakeSource(**kwargs)
        result = run_backfill_ingest('Alice', 7, monitor=monitor, build_store=lambda: self._build_state(store), build_ingest=lambda: {'embedder': object()}, source_factory=source_factory, session_factory=FakeSession, out=logs.append)
        assert seen_vods == ['resume', 'fail']
        assert seen_creator_metadata == [
            {
                'id': 'user-1',
                'login': 'alice',
                'display_name': 'alice',
                'profile_image_url': 'https://cdn/profile.png',
            },
            {
                'id': 'user-1',
                'login': 'alice',
                'display_name': 'alice',
                'profile_image_url': 'https://cdn/profile.png',
            },
        ]
        assert result.ingested == 1
        assert result.resumed == 1
        assert result.skipped == 1
        assert result.failed == 1
        assert any((line.startswith('resume vod=resume') for line in logs))
        assert any((line.startswith('starting vod 1/3 vod=resume') for line in logs))
        assert any((line.startswith('skip processed vod=processed') for line in logs))
        assert any((line.startswith('failed vod=fail') for line in logs))

    def test_skips_deleted_and_restarts_reindex_requested_vod(self) -> None:
        store = FakeStore()
        store.videos_by_url['https://www.twitch.tv/videos/deleted'] = (
            1,
            1,
            'https://www.twitch.tv/videos/deleted',
            'Deleted',
            None,
            True,
            None,
        )
        store.video_status_by_id[1] = 'deleted'
        store.videos_by_url['https://www.twitch.tv/videos/reindex'] = (
            2,
            1,
            'https://www.twitch.tv/videos/reindex',
            'Needs reindex',
            None,
            True,
            None,
        )
        store.video_status_by_id[2] = 'reindex_requested'
        store.vod_state['reindex'] = {
            'vod_platform_id': 'reindex',
            'video_id': 2,
            'streamer': 'alice',
            'last_ingested_seconds': 90,
            'last_seen_duration_seconds': 180,
            'updated_at': 'now',
        }
        monitor = FakeMonitor(
            [
                {'id': 'deleted', 'url': 'https://www.twitch.tv/videos/deleted'},
                {'id': 'reindex', 'url': 'https://www.twitch.tv/videos/reindex'},
            ]
        )
        logs: list[str] = []
        seen_vods: list[str] = []

        def source_factory(**kwargs):
            seen_vods.append(kwargs['vod_metadata']['id'])
            return FakeSource(**kwargs)

        result = run_backfill_ingest(
            'alice',
            7,
            monitor=monitor,
            build_store=lambda: self._build_state(store),
            build_ingest=lambda: {'embedder': object()},
            source_factory=source_factory,
            session_factory=FakeSession,
            out=logs.append,
        )

        assert seen_vods == ['reindex']
        assert result.ingested == 1
        assert result.resumed == 0
        assert result.skipped == 1
        assert store.deleted_vod_state_ids == ['reindex']
        assert not any(line.startswith('resume vod=reindex') for line in logs)
        assert any(line.startswith('skip deleted vod=deleted') for line in logs)
        assert any(line.startswith('starting vod 2/2 vod=reindex') and 'cursor=0' in line for line in logs)

    def test_resumes_indexing_vod_when_state_exists(self) -> None:
        store = FakeStore()
        store.videos_by_url['https://www.twitch.tv/videos/resume-indexing'] = (
            3,
            1,
            'https://www.twitch.tv/videos/resume-indexing',
            'Resume indexing',
            None,
            False,
            None,
        )
        store.video_status_by_id[3] = 'indexing'
        store.vod_state['resume-indexing'] = {
            'vod_platform_id': 'resume-indexing',
            'video_id': 3,
            'streamer': 'alice',
            'last_ingested_seconds': 30,
            'last_seen_duration_seconds': 180,
            'updated_at': 'now',
        }
        monitor = FakeMonitor(
            [
                {'id': 'resume-indexing', 'url': 'https://www.twitch.tv/videos/resume-indexing'},
            ]
        )
        logs: list[str] = []
        seen_vods: list[str] = []

        def source_factory(**kwargs):
            seen_vods.append(kwargs['vod_metadata']['id'])
            return FakeSource(**kwargs)

        result = run_backfill_ingest(
            'alice',
            7,
            monitor=monitor,
            build_store=lambda: self._build_state(store),
            build_ingest=lambda: {'embedder': object()},
            source_factory=source_factory,
            session_factory=FakeSession,
            out=logs.append,
        )

        assert seen_vods == ['resume-indexing']
        assert result.ingested == 1
        assert result.resumed == 1
        assert not any(line.startswith('skip indexing vod=resume-indexing') for line in logs)
        assert any(line.startswith('resume vod=resume-indexing cursor=30') for line in logs)

    def test_fresh_nmfp_reindex_rejects_searchable_or_resumable_rows_before_model_build(self) -> None:
        store = FakeStore()
        store.videos_by_url['https://www.twitch.tv/videos/searchable'] = (
            4,
            1,
            'https://www.twitch.tv/videos/searchable',
            'Legacy searchable',
            None,
            True,
            None,
        )
        store.video_status_by_id[4] = 'searchable'
        store.videos_by_url['https://www.twitch.tv/videos/indexing'] = (
            5,
            1,
            'https://www.twitch.tv/videos/indexing',
            'Ambiguous partial index',
            None,
            False,
            None,
        )
        store.video_status_by_id[5] = 'indexing'
        store.vod_state['indexing'] = {
            'vod_platform_id': 'indexing',
            'video_id': 5,
            'streamer': 'alice',
            'last_ingested_seconds': 60,
            'last_seen_duration_seconds': 180,
            'updated_at': 'now',
        }
        monitor = FakeMonitor(
            [
                {'id': 'searchable', 'url': 'https://www.twitch.tv/videos/searchable'},
                {'id': 'indexing', 'url': 'https://www.twitch.tv/videos/indexing'},
            ]
        )
        build_ingest_calls: list[bool] = []

        with pytest.raises(FreshNMFPReindexPreconditionError) as raised:
            run_backfill_ingest(
                'alice',
                7,
                fresh_nmfp_reindex=True,
                monitor=monitor,
                build_store=lambda: self._build_state(store),
                build_ingest=lambda: build_ingest_calls.append(True) or {'embedder': object()},
                source_factory=FakeSource,
                session_factory=FakeSession,
            )

        message = str(raised.value)
        assert 'Apply the NMFP schema migration first' in message
        assert 'vod=searchable status=searchable has_saved_cursor=false' in message
        assert 'vod=indexing status=indexing has_saved_cursor=true' in message
        assert build_ingest_calls == []

    def test_fresh_nmfp_reindex_accepts_migration_marked_rows_and_never_resumes_cursor(self) -> None:
        store = FakeStore()
        store.videos_by_url['https://www.twitch.tv/videos/reindex'] = (
            6,
            1,
            'https://www.twitch.tv/videos/reindex',
            'Prepared reindex',
            None,
            True,
            None,
        )
        store.video_status_by_id[6] = 'reindex_requested'
        store.vod_state['reindex'] = {
            'vod_platform_id': 'reindex',
            'video_id': 6,
            'streamer': 'alice',
            'last_ingested_seconds': 90,
            'last_seen_duration_seconds': 180,
            'updated_at': 'now',
        }
        monitor = FakeMonitor(
            [{'id': 'reindex', 'url': 'https://www.twitch.tv/videos/reindex'}]
        )
        logs: list[str] = []

        result = run_backfill_ingest(
            'alice',
            7,
            fresh_nmfp_reindex=True,
            monitor=monitor,
            build_store=lambda: self._build_state(store),
            build_ingest=lambda: {'embedder': object()},
            source_factory=FakeSource,
            session_factory=FakeSession,
            out=logs.append,
        )

        assert result.ingested == 1
        assert result.resumed == 0
        assert store.deleted_vod_state_ids == ['reindex']
        assert logs[0] == (
            'fresh_nmfp_reindex preflight=passed '
            'streamer=alice vod_count=1 resume_allowed=false'
        )
        assert not any(line.startswith('resume vod=reindex') for line in logs)
        assert any(line.startswith('starting vod 1/1 vod=reindex') and 'cursor=0' in line for line in logs)

    def test_fresh_nmfp_reindex_accepts_a_vod_missing_from_a_fresh_database(self) -> None:
        store = FakeStore()
        monitor = FakeMonitor(
            [{'id': 'new-vod', 'url': 'https://www.twitch.tv/videos/new-vod'}]
        )
        logs: list[str] = []

        result = run_backfill_ingest(
            'alice',
            7,
            fresh_nmfp_reindex=True,
            monitor=monitor,
            build_store=lambda: self._build_state(store),
            build_ingest=lambda: {'embedder': object()},
            source_factory=FakeSource,
            session_factory=FakeSession,
            out=logs.append,
        )

        assert result.ingested == 1
        assert result.resumed == 0
        assert store.deleted_vod_state_ids == []
        assert any(line.startswith('starting vod 1/1 vod=new-vod') and 'cursor=0' in line for line in logs)

    def test_main_returns_non_zero_on_failure(self) -> None:
        import runners.run_backfill_ingest as module
        original = module.run_backfill_ingest
        try:
            seen: list[tuple[str, int, bool]] = []
            module.run_backfill_ingest = lambda streamer, days, fresh_nmfp_reindex=False: (
                seen.append((streamer, days, fresh_nmfp_reindex))
                or type('R', (), {'failed': 1})()
            )
            assert main(['--streamer', 'alice', '--days', '3']) == 1
            assert main(['--streamer', 'alice', '--days', '3', '--fresh-nmfp-reindex']) == 1
            assert seen == [('alice', 3, False), ('alice', 3, True)]
        finally:
            module.run_backfill_ingest = original
