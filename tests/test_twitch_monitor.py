from unittest.mock import patch
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from email.message import Message
import io
import json
import socket
import threading
import urllib.error
import pytest
from services.twitch_monitor import CachedMultiStreamerTwitchMonitor, TwitchMonitor, TwitchRequestError

class TestTwitchMonitor:

    @pytest.mark.parametrize('failure', [
        urllib.error.URLError(socket.gaierror(-3, 'DNS detail with secret')),
        TimeoutError('signed URL must not be logged'),
        ConnectionResetError('connection lost'),
        urllib.error.HTTPError('https://api.twitch.tv/secret', 503, 'private body', {}, None),
    ])
    def test_request_failures_are_retryable_and_sanitized(self, failure) -> None:
        monitor = TwitchMonitor('client', 'secret')
        monitor._access_token = 'token'
        with patch('urllib.request.urlopen', side_effect=failure):
            with pytest.raises(TwitchRequestError) as raised:
                monitor.get_latest_archive_vod('user-1')
        error = raised.value
        assert error.retryable
        assert error.operation == 'helix_videos'
        assert error.hostname == 'api.twitch.tv'
        assert 'secret' not in str(error)
        assert 'signed' not in str(error)
        assert 'private' not in str(error)

    def test_rate_limit_is_shared_by_other_helix_operations_without_sleeping(self) -> None:
        monitor = TwitchMonitor('client', 'secret')
        monitor._access_token = 'token'
        headers = Message()
        headers['Ratelimit-Reset'] = '130'
        error = urllib.error.HTTPError('https://api.twitch.tv/videos?secret=private', 429, '', headers, None)
        with patch('services.twitch_monitor.time.time', return_value=100), patch(
            'urllib.request.urlopen', side_effect=error,
        ) as request:
            with pytest.raises(TwitchRequestError) as first:
                monitor.get_latest_archive_vod('user-1')
            with pytest.raises(TwitchRequestError) as second:
                monitor.is_live('alice')
        assert first.value.retry_at == second.value.retry_at == 130
        assert second.value.operation == 'helix_streams'
        assert request.call_count == 1
        with patch('services.twitch_monitor.time.time', return_value=131), patch(
            'urllib.request.urlopen', return_value=io.BytesIO(b'{"data": []}'),
        ) as request:
            assert monitor.is_live('alice') is False
        assert request.call_count == 1

    @pytest.mark.parametrize('header', ['inf', 'NaN', '-inf', 'invalid'])
    def test_invalid_rate_limit_deadline_does_not_disable_requests_forever(self, header) -> None:
        monitor = TwitchMonitor('client', 'secret')
        monitor._access_token = 'token'
        headers = Message()
        headers['Ratelimit-Reset'] = header
        error = urllib.error.HTTPError('https://api.twitch.tv/videos', 429, '', headers, None)
        with patch('services.twitch_monitor.time.time', return_value=100), patch(
            'urllib.request.urlopen', side_effect=error,
        ):
            with pytest.raises(TwitchRequestError) as raised:
                monitor.get_latest_archive_vod('user-1')
        assert raised.value.retry_at == 105

    def test_unauthorized_request_refreshes_once_then_surfaces_permanent_auth_failure(self) -> None:
        monitor = TwitchMonitor('client', 'secret')
        monitor._access_token = 'old'
        failure = lambda: urllib.error.HTTPError('https://api.twitch.tv/secret', 401, 'private', {}, None)
        with patch('urllib.request.urlopen', side_effect=[
            failure(), io.BytesIO(b'{"access_token": "new"}'), failure(),
        ]) as request:
            with pytest.raises(TwitchRequestError) as raised:
                monitor.is_live('alice')
        assert not raised.value.retryable
        assert raised.value.status == 401
        assert request.call_count == 3

    def test_concurrent_startup_and_unauthorized_requests_share_tokens(self) -> None:
        monitor = TwitchMonitor('client', 'secret')
        rejected = threading.Barrier(2)
        token_requests = []

        def request(req, **kwargs):
            if req.full_url.startswith('https://id.twitch.tv'):
                token = 'old' if not token_requests else 'new'
                token_requests.append(token)
                return io.BytesIO(json.dumps({'access_token': token}).encode())
            if req.get_header('Authorization') == 'Bearer old':
                rejected.wait(timeout=1)
                raise urllib.error.HTTPError(req.full_url, 401, '', {}, None)
            return io.BytesIO(b'{"data": []}')

        with patch('urllib.request.urlopen', side_effect=request), ThreadPoolExecutor(2) as executor:
            results = list(executor.map(monitor.is_live, ['alice', 'bob']))
        assert results == [False, False]
        assert token_requests == ['old', 'new']

    def test_oauth_transport_and_auth_errors_have_token_operation(self) -> None:
        monitor = TwitchMonitor('client', 'secret')
        with patch('urllib.request.urlopen', side_effect=TimeoutError('private')):
            with pytest.raises(TwitchRequestError) as raised:
                monitor.is_live('alice')
        assert raised.value.operation == 'oauth_token'
        assert raised.value.hostname == 'id.twitch.tv'
        assert raised.value.retryable
        with patch('urllib.request.urlopen', side_effect=urllib.error.HTTPError(
            'https://id.twitch.tv/oauth2/token?secret=private', 400, 'bad client', {}, None,
        )):
            with pytest.raises(TwitchRequestError) as raised:
                monitor.is_live('alice')
        assert not raised.value.retryable
        assert raised.value.status == 400

    @pytest.mark.parametrize('body', [
        b'{broken json', b'[]', b'\xff', b'', b'{}', b'{"data": null}',
        b'{"error": "temporary error", "data": []}',
    ])
    def test_malformed_successful_response_is_retryable(self, body) -> None:
        monitor = TwitchMonitor('client', 'secret')
        monitor._access_token = 'token'
        with patch('urllib.request.urlopen', return_value=io.BytesIO(body)):
            with pytest.raises(TwitchRequestError) as raised:
                monitor.is_live('alice')
        assert raised.value.retryable
        assert raised.value.error_class == 'InvalidResponse'

    @pytest.mark.parametrize(('method', 'body', 'error_class'), [
        ('is_live', b'{}', 'MissingAccessToken'),
        ('get_user_profile', b'{"data": [{"login": "alice"}]}', 'MissingUserId'),
        ('get_user_profile', b'{"data": []}', 'StreamerNotFound'),
    ])
    def test_missing_provider_fields_distinguish_protocol_error_from_unknown_streamer(self, method, body, error_class) -> None:
        monitor = TwitchMonitor('client', 'secret')
        if method != 'is_live':
            monitor._access_token = 'token'
        with patch('urllib.request.urlopen', return_value=io.BytesIO(body)):
            with pytest.raises(TwitchRequestError) as raised:
                getattr(monitor, method)('alice')
        assert raised.value.error_class == error_class
        assert raised.value.retryable is (error_class != 'StreamerNotFound')

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

        assert monitor.get_live_streamers(['jasontheween']) == {'stableronaldo'}
        assert base.calls == [('jasontheween', 'stableronaldo'), ('jasontheween',)]
        assert monitor.is_live('stableronaldo') is True
        assert len(base.calls) == 2
