from runners.run_backfill_ingest import main, run_backfill_ingest

class FakeMonitor:

    def __init__(self, vods):
        self.vods = list(vods)

    def get_user_id(self, streamer: str) -> str:
        return 'user-1'

    def list_archive_vods_since(self, user_id: str, created_after):
        return list(self.vods)

class FakeStore:

    def __init__(self):
        self.videos_by_url: dict[str, tuple[int, int, str, str, str | None, bool]] = {}
        self.vod_state: dict[str, dict] = {}

    def get_video_by_url(self, url: str):
        return self.videos_by_url.get(url)

    def get_vod_ingest_state(self, vod_platform_id: str):
        return self.vod_state.get(vod_platform_id)

class FakeSource:

    def __init__(self, streamer, vod_metadata, store, chunk_seconds, temp_dir, progress_callback=None):
        self.streamer = streamer
        self.vod_metadata = vod_metadata
        self.store = store
        self.chunk_seconds = chunk_seconds
        self.temp_dir = temp_dir
        self.progress_callback = progress_callback

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
        store.videos_by_url['https://www.twitch.tv/videos/processed'] = (1, 1, 'https://www.twitch.tv/videos/processed', 'Processed', None, True)
        store.vod_state['resume'] = {'vod_platform_id': 'resume', 'video_id': 2, 'streamer': 'alice', 'last_ingested_seconds': 60, 'last_seen_duration_seconds': 120, 'updated_at': 'now'}
        monitor = FakeMonitor([{'id': 'resume', 'url': 'https://www.twitch.tv/videos/resume'}, {'id': 'processed', 'url': 'https://www.twitch.tv/videos/processed'}, {'id': 'fail', 'url': 'https://www.twitch.tv/videos/fail', 'should_fail': True}])
        logs: list[str] = []
        seen_vods: list[str] = []

        def source_factory(**kwargs):
            seen_vods.append(kwargs['vod_metadata']['id'])
            return FakeSource(**kwargs)
        result = run_backfill_ingest('Alice', 7, monitor=monitor, build_store=lambda: self._build_state(store), build_ingest=lambda: {'embedder': object()}, source_factory=source_factory, session_factory=FakeSession, out=logs.append)
        assert seen_vods == ['resume', 'fail']
        assert result.ingested == 1
        assert result.resumed == 1
        assert result.skipped == 1
        assert result.failed == 1
        assert any((line.startswith('resume vod=resume') for line in logs))
        assert any((line.startswith('starting vod 1/3 vod=resume') for line in logs))
        assert any((line.startswith('skip processed vod=processed') for line in logs))
        assert any((line.startswith('failed vod=fail') for line in logs))

    def test_main_returns_non_zero_on_failure(self) -> None:
        import runners.run_backfill_ingest as module
        original = module.run_backfill_ingest
        try:
            module.run_backfill_ingest = lambda streamer, days: type('R', (), {'failed': 1})()
            assert main(['--streamer', 'alice', '--days', '3']) == 1
        finally:
            module.run_backfill_ingest = original
