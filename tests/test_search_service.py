from datetime import datetime, timezone

import numpy as np
from search.models import AlignmentResult, FingerprintCandidate, SearchDateRange, SearchSegment
from pipeline.nmfp_inference import NMFPExtractionMetrics, NMFPExtractionResult
from search.search_service import SearchService
from storage.records import VideoRecord, VideoStatus

class FakePreprocessor:

    def __init__(self) -> None:
        self.duration_limits: list[float | None] = []

    def prepare(
        self,
        clip_path: str,
        duration_limit_seconds: float | None = None,
    ) -> str:
        self.duration_limits.append(duration_limit_seconds)
        return clip_path

    def cleanup(self, path: str) -> None:
        return None

class FakeQueryEmbedder:

    def __init__(self, embeddings: np.ndarray, timestamps: np.ndarray):
        self.embeddings = embeddings
        self.timestamps = timestamps
        self.last_result = None

    def embed(self, wav_path: str):
        return (self.embeddings, self.timestamps)

class FakeAlignment:

    def __init__(self, result: AlignmentResult):
        self.result = result

    def align_candidates(self, candidates, *, query_duration_seconds):
        return self.result

class FakeStore:

    def __init__(self):
        self.last_streamer: str | None = None
        self.last_creator_id: int | None = None
        self.last_date_range: SearchDateRange | None = None
        self.last_model_version: str | None = None
        self.last_preprocessing_version: str | None = None

    def get_creator_id_by_name(self, name: str):
        self.last_streamer = name
        return 12

    def query_fingerprint_candidates(
        self,
        query_embeddings: np.ndarray,
        query_timestamps: np.ndarray,
        top_k: int,
        creator_id: int,
        model_version=None,
        preprocessing_version=None,
        date_range: SearchDateRange | None = None,
    ):
        self.last_creator_id = creator_id
        self.last_date_range = date_range
        self.last_model_version = model_version
        self.last_preprocessing_version = preprocessing_version
        return [FingerprintCandidate(0, 0.0, 10, 777, 1368.0, 0.99, 0)]

    def get_video_with_creator(self, video_id: int):
        if video_id == 888:
            return VideoRecord(
                id=888,
                creator_id=12,
                url='https://www.twitch.tv/videos/888',
                title='Second VOD',
                thumbnail_url=None,
                status=VideoStatus.SEARCHABLE,
                processed=True,
                streamed_at=None,
                creator_name='xqc',
                creator_profile_image_url='https://cdn/xqc.png',
            )
        return VideoRecord(
            id=777,
            creator_id=12,
            url='https://www.twitch.tv/videos/2699020769',
            title='Sample title',
            thumbnail_url='https://static-cdn.jtvnw.net/cf_vods/thumb-320x180.jpg',
            status=VideoStatus.SEARCHABLE,
            processed=True,
            streamed_at=None,
            creator_name='xqc',
            creator_profile_image_url='https://cdn/xqc.png',
        )

class TestSearchService:

    def test_found_result_includes_timestamp_url(self) -> None:
        store = FakeStore()
        segment = SearchSegment(0.0, 0.5, 777, 1368.0, 1368.5, 0.9, 1.0, 1368.0, 0.9, 1.0, 1, 1)
        service = SearchService(videos=store, fingerprints=store, preprocessor=FakePreprocessor(), query_embedder=FakeQueryEmbedder(embeddings=np.array([[0.1, 0.2]], dtype=np.float32), timestamps=np.array([0.0], dtype=np.float32)), alignment=FakeAlignment(AlignmentResult(found=True, video_id=777, timestamp_seconds=1368, score=0.9, reason='ok', segments=[segment], query_duration_seconds=0.5)))
        execution = service.search_file('clip.mp4', 'xQc')
        result = execution.result
        assert result.found
        assert store.last_streamer == 'xqc'
        assert store.last_creator_id == 12
        assert store.last_date_range is None
        assert result.video_url_at_timestamp == 'https://www.twitch.tv/videos/2699020769?t=22m48s'
        assert result.thumbnail_url == 'https://static-cdn.jtvnw.net/cf_vods/thumb-320x180.jpg'
        assert result.profile_image_url == 'https://cdn/xqc.png'
        assert len(result.sources) == 1
        assert result.sources[0].title == 'Sample title'
        assert result.sources[0].video_url_at_timestamp == result.video_url_at_timestamp
        assert result.sources[0].segments[0].video_url_at_timestamp == result.video_url_at_timestamp
        assert execution.metadata.found_match is True
        assert execution.metadata.matched_video_id == 777

    def test_groups_segments_by_vod_and_uses_each_vods_strongest_segment(self) -> None:
        store = FakeStore()
        first_vod_early = SearchSegment(
            0.0,
            4.0,
            777,
            100.0,
            104.0,
            0.7,
            4.0,
            100.0,
            0.8,
            1.0,
            8,
            6,
        )
        first_vod_strong = SearchSegment(
            5.0,
            12.0,
            777,
            200.0,
            207.0,
            0.8,
            9.0,
            195.0,
            0.9,
            1.0,
            14,
            12,
        )
        second_vod_segment = SearchSegment(
            15.0,
            21.0,
            888,
            500.0,
            506.0,
            0.75,
            8.0,
            485.0,
            0.85,
            1.0,
            12,
            9,
        )
        alignment = AlignmentResult(
            found=True,
            video_id=777,
            timestamp_seconds=200,
            score=0.8,
            reason='ok',
            segments=[first_vod_early, first_vod_strong, second_vod_segment],
            query_duration_seconds=21.0,
        )
        service = SearchService(
            videos=store,
            fingerprints=store,
            preprocessor=FakePreprocessor(),
            query_embedder=FakeQueryEmbedder(
                embeddings=np.array([[0.1, 0.2]], dtype=np.float32),
                timestamps=np.array([0.0], dtype=np.float32),
            ),
            alignment=FakeAlignment(alignment),
        )

        result = service.search_file('clip.mp4', 'xqc').result

        assert [source.video_id for source in result.sources] == [777, 888]
        assert result.sources[0].title == 'Sample title'
        assert result.sources[0].video_url_at_timestamp == 'https://www.twitch.tv/videos/2699020769?t=3m20s'
        assert [segment.vod_start for segment in result.sources[0].segments] == [100.0, 200.0]
        assert result.sources[1].title == 'Second VOD'
        assert result.sources[1].video_url_at_timestamp == 'https://www.twitch.tv/videos/888?t=8m20s'

    def test_not_found_result_has_no_timestamp_url(self) -> None:
        service = SearchService(videos=FakeStore(), fingerprints=FakeStore(), preprocessor=FakePreprocessor(), query_embedder=FakeQueryEmbedder(embeddings=np.array([[0.1, 0.2]], dtype=np.float32), timestamps=np.array([0.0], dtype=np.float32)), alignment=FakeAlignment(AlignmentResult(found=False, reason='No aligned match found')))
        execution = service.search_file('clip.mp4', 'xqc')
        result = execution.result
        assert not result.found
        assert result.video_url_at_timestamp is None
        assert execution.metadata.found_match is False
        assert execution.metadata.result_reason == 'No aligned match found'

    def test_missing_streamer_returns_not_found_before_knn(self) -> None:

        class MissingStore(FakeStore):

            def get_creator_id_by_name(self, name: str):
                self.last_streamer = name
                return None
        store = MissingStore()
        service = SearchService(videos=store, fingerprints=store, preprocessor=FakePreprocessor(), query_embedder=FakeQueryEmbedder(embeddings=np.array([[0.1, 0.2]], dtype=np.float32), timestamps=np.array([0.0], dtype=np.float32)), alignment=FakeAlignment(AlignmentResult(found=False, reason='No aligned match found')))
        execution = service.search_file('clip.mp4', 'xqc')
        result = execution.result
        assert not result.found
        assert result.reason == 'No indexed clips found for streamer: xqc'
        assert execution.metadata.vector_query_duration_ms is None

    def test_passes_date_range_to_vector_query(self) -> None:
        store = FakeStore()
        date_range = SearchDateRange(
            streamed_from=datetime(2026, 4, 1, tzinfo=timezone.utc),
            streamed_to=datetime(2026, 4, 8, tzinfo=timezone.utc),
        )
        service = SearchService(videos=store, fingerprints=store, preprocessor=FakePreprocessor(), query_embedder=FakeQueryEmbedder(embeddings=np.array([[0.1, 0.2]], dtype=np.float32), timestamps=np.array([0.0], dtype=np.float32)), alignment=FakeAlignment(AlignmentResult(found=False, reason='No aligned match found')))
        service.search_file('clip.mp4', 'xqc', date_range=date_range)
        assert store.last_date_range == date_range

    def test_records_nmfp_worker_metrics_and_exact_index_identity(self) -> None:
        store = FakeStore()
        embedder = FakeQueryEmbedder(
            embeddings=np.array([[0.1, 0.2]], dtype=np.float32),
            timestamps=np.array([0.0], dtype=np.float32),
        )
        embedder.last_result = NMFPExtractionResult(
            embeddings=embedder.embeddings,
            timestamps=embedder.timestamps,
            model_version="nmfp-model",
            preprocessing_version="nmfp-preprocessing",
            embedding_dim=2,
            metrics=NMFPExtractionMetrics(
                cold_start=True,
                model_load_duration_ms=1200,
                preprocessing_duration_ms=14,
                inference_duration_ms=26,
                total_duration_ms=1240,
                audio_duration_seconds=1.0,
                fingerprint_count=1,
            ),
        )
        service = SearchService(
            videos=store,
            fingerprints=store,
            preprocessor=FakePreprocessor(),
            query_embedder=embedder,
            alignment=FakeAlignment(AlignmentResult(found=False, reason="no supported track")),
        )

        execution = service.search_file("clip.mp4", "xqc")

        assert execution.metadata.model_cold_start is True
        assert execution.metadata.model_startup_duration_ms == 1200
        assert execution.metadata.fingerprint_preprocessing_duration_ms == 14
        assert execution.metadata.fingerprint_inference_duration_ms == 26
        assert execution.metadata.fingerprint_duration_ms == 1240
        assert execution.metadata.query_fingerprint_count == 1
        assert execution.metadata.candidate_count == 1
        assert store.last_model_version == "nmfp-model"
        assert store.last_preprocessing_version == "nmfp-preprocessing"
        assert execution.result.query_duration_seconds == 1.0

    def test_passes_query_duration_to_preprocessor(self) -> None:
        preprocessor = FakePreprocessor()
        service = SearchService(
            videos=FakeStore(),
            fingerprints=FakeStore(),
            preprocessor=preprocessor,
            query_embedder=FakeQueryEmbedder(
                embeddings=np.array([[0.1, 0.2]], dtype=np.float32),
                timestamps=np.array([0.0], dtype=np.float32),
            ),
            alignment=FakeAlignment(AlignmentResult(found=False, reason="no match")),
        )

        service.search_file("clip.mp4", "xqc", query_duration_seconds=133.166667)

        assert preprocessor.duration_limits == [133.166667]
