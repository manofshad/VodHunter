from unittest.mock import patch
from datetime import datetime, timezone
import pytest
from services.twitch_monitor import CachedMultiStreamerTwitchMonitor, TwitchMonitor

class TestTwitchMonitor:

    def test_direct_archive_lookup_distinguishes_valid_archive_from_confirmed_absence(self) -> None:
        monitor = TwitchMonitor(client_id='x', client_secret='y')
        row = {
            'id': '2878694994', 'user_id': 'user-1', 'type': 'archive', 'viewable': 'public',
            'duration': '6h19m28s', 'title': 'Saved archive', 'created_at': '2026-09-20T00:00:00Z',
        }
        with patch.object(monitor, '_helix_get', side_effect=[{'data': [row]}, {'data': []}]) as helix:
            vod = monitor.get_archive_vod_by_id('2878694994', expected_user_id='user-1')
            absent = monitor.get_archive_vod_by_id('2878694994', expected_user_id='user-1')
        assert vod['id'] == '2878694994'
        assert vod['url'] == 'https://www.twitch.tv/videos/2878694994'
        assert vod['duration_seconds'] == 22768
        assert absent is None
        assert helix.call_args.args == ('videos', {'id': '2878694994'})

    @pytest.mark.parametrize('payload', [
        None, {}, {'data': None}, {'data': {}}, {'error': 'Unauthorized', 'data': []},
        {'data': [None]}, {'data': [{}, {}]},
    ])
    def test_direct_archive_lookup_rejects_malformed_or_error_response(self, payload) -> None:
        monitor = TwitchMonitor(client_id='x', client_secret='y')
        with patch.object(monitor, '_helix_get', return_value=payload):
            with pytest.raises(RuntimeError, match='Invalid Twitch video'):
                monitor.get_archive_vod_by_id('2878694994', expected_user_id='user-1')

    @pytest.mark.parametrize(('field', 'value'), [
        ('id', 'other'), ('user_id', 'other-owner'), ('type', 'highlight'),
        ('viewable', 'private'), ('duration', None), ('duration', ''),
        ('duration', '0s'), ('duration', '120'), ('duration', '1mgarbage'),
    ])
    def test_direct_archive_lookup_rejects_wrong_identity_or_invalid_archive(self, field, value) -> None:
        monitor = TwitchMonitor(client_id='x', client_secret='y')
        row = {
            'id': '2878694994', 'user_id': 'user-1', 'type': 'archive',
            'viewable': 'public', 'duration': '6h19m28s',
        }
        row[field] = value
        with patch.object(monitor, '_helix_get', return_value={'data': [row]}):
            with pytest.raises(RuntimeError, match='Invalid Twitch archive'):
                monitor.get_archive_vod_by_id('2878694994', expected_user_id='user-1')

    def test_direct_archive_lookup_propagates_transport_failure(self) -> None:
        monitor = TwitchMonitor(client_id='x', client_secret='y')
        with patch.object(monitor, '_helix_get', side_effect=RuntimeError('HTTP 503')):
            with pytest.raises(RuntimeError, match='HTTP 503'):
                monitor.get_archive_vod_by_id('2878694994', expected_user_id='user-1')

    def test_parse_duration_to_seconds(self) -> None:
        assert TwitchMonitor.parse_duration_to_seconds('1h2m3s') == 3723
        assert TwitchMonitor.parse_duration_to_seconds('45m') == 2700
        assert TwitchMonitor.parse_duration_to_seconds('59s') == 59
        assert TwitchMonitor.parse_duration_to_seconds('') == 0

    def test_selects_latest_archive_vod(self) -> None:
        monitor = TwitchMonitor(client_id='x', client_secret='y')
        payload = {'data': [{'id': '111', 'title': 'Older', 'duration': '1h0m0s', 'created_at': '2026-02-15T10:00:00Z', 'thumbnail_url': 'https://static-cdn.jtvnw.net/cf_vods/d2/vod-thumb-%{width}x%{height}.jpg'}, {'id': '222', 'title': 'Newest', 'duration': '2h3m4s', 'created_at': '2026-02-15T12:00:00Z', 'thumbnail_url': 'https://static-cdn.jtvnw.net/cf_vods/d2/new-thumb-%{width}x%{height}.jpg'}]}
        with patch.object(monitor, '_helix_get', return_value=payload):
            vod = monitor.get_latest_archive_vod('user-1')
        assert vod is not None
        assert vod is not None
        assert vod['id'] == '222'
        assert vod['url'] == 'https://www.twitch.tv/videos/222'
        assert vod['duration_seconds'] == 7384
        assert vod['thumbnail_url'] == 'https://static-cdn.jtvnw.net/cf_vods/d2/new-thumb-320x180.jpg'

    def test_get_user_profile_normalizes_profile_image_url(self) -> None:
        monitor = TwitchMonitor(client_id='x', client_secret='y')
        payload = {'data': [{'id': 'user-1', 'login': 'xqc', 'display_name': 'xQc', 'profile_image_url': 'https://static-cdn.jtvnw.net/jtv_user_pictures/xqc-profile_image.png'}]}
        with patch.object(monitor, '_helix_get', return_value=payload):
            profile = monitor.get_user_profile('xQc')
        assert profile == {
            'id': 'user-1',
            'login': 'xqc',
            'display_name': 'xQc',
            'profile_image_url': 'https://static-cdn.jtvnw.net/jtv_user_pictures/xqc-profile_image.png',
        }

    def test_list_archive_vods_since_paginates_filters_and_sorts(self) -> None:
        monitor = TwitchMonitor(client_id='x', client_secret='y')
        first_page = {'data': [{'id': '300', 'title': 'Newest', 'duration': '2h0m0s', 'created_at': '2026-03-09T13:00:00Z', 'thumbnail_url': 'https://cdn/%{width}x%{height}.jpg'}, {'id': '200', 'title': 'Middle', 'duration': '1h0m0s', 'created_at': '2026-03-08T13:00:00Z', 'thumbnail_url': 'https://cdn/%{width}x%{height}.jpg'}], 'pagination': {'cursor': 'next-page'}}
        second_page = {'data': [{'id': '100', 'title': 'Too old', 'duration': '30m', 'created_at': '2026-03-01T13:00:00Z', 'thumbnail_url': 'https://cdn/%{width}x%{height}.jpg'}], 'pagination': {}}
        with patch.object(monitor, '_helix_get', side_effect=[first_page, second_page]) as helix_get:
            vods = monitor.list_archive_vods_since('user-1', datetime(2026, 3, 5, 0, 0, tzinfo=timezone.utc))
        assert [vod['id'] for vod in vods] == ['300', '200']
        assert helix_get.call_args_list[0].args[1]['first'] == '100'
        assert helix_get.call_args_list[1].args[1]['after'] == 'next-page'

    def test_get_live_streamers_batches_and_normalizes_up_to_one_hundred_logins(self) -> None:
        monitor = TwitchMonitor(client_id='x', client_secret='y')
        payload = {'data': [{'user_login': 'StableRonaldo'}]}

        with patch.object(monitor, '_helix_get', return_value=payload) as helix_get:
            live = monitor.get_live_streamers(['Jasontheween', 'stableronaldo', 'jasontheween'])

        assert live == {'stableronaldo'}
        assert helix_get.call_args.args == (
            'streams',
            {'user_login': ['jasontheween', 'stableronaldo'], 'first': '100'},
        )

    def test_cached_multi_streamer_monitor_shares_one_live_lookup(self) -> None:
        class BatchMonitor:
            def __init__(self) -> None:
                self.calls: list[tuple[str, ...]] = []

            def get_live_streamers(self, streamers: list[str]) -> set[str]:
                self.calls.append(tuple(streamers))
                return {'stableronaldo'}

        base = BatchMonitor()
        monitor = CachedMultiStreamerTwitchMonitor(  # type: ignore[arg-type]
            base,
            ['jasontheween', 'stableronaldo'],
            cache_seconds=60,
        )

        assert monitor.is_live('jasontheween') is False
        assert monitor.is_live('stableronaldo') is True
        assert base.calls == [('jasontheween', 'stableronaldo')]
