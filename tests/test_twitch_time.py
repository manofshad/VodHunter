from search.twitch_time import build_twitch_timestamp_url, format_twitch_offset

class TestTwitchTime:

    def test_format_twitch_offset_examples(self) -> None:
        assert format_twitch_offset(1368) == '22m48s'
        assert format_twitch_offset(59) == '59s'
        assert format_twitch_offset(60) == '1m0s'
        assert format_twitch_offset(3723) == '1h2m3s'

    def test_build_twitch_timestamp_url_handles_invalid_seconds(self) -> None:
        assert build_twitch_timestamp_url('https://www.twitch.tv/videos/1', None) is None
        assert build_twitch_timestamp_url('https://www.twitch.tv/videos/1', -1) is None

    def test_build_twitch_timestamp_url_appends_or_replaces_t(self) -> None:
        assert build_twitch_timestamp_url('https://www.twitch.tv/videos/2699020769', 1368) == 'https://www.twitch.tv/videos/2699020769?t=22m48s'
        assert build_twitch_timestamp_url('https://www.twitch.tv/videos/1?foo=bar&t=1s', 60) == 'https://www.twitch.tv/videos/1?foo=bar&t=1m0s'
