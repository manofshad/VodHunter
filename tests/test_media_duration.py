import pytest
import subprocess
from unittest.mock import patch
from backend.services.media_duration import (
    MediaDurationError,
    probe_media_duration_seconds,
    probe_media_durations,
)

class TestMediaDuration:

    def test_probe_returns_duration_seconds(self) -> None:
        ok = subprocess.CompletedProcess(args=['ffprobe'], returncode=0, stdout='123.45\n', stderr='')
        with patch('backend.services.media_duration.subprocess.run', return_value=ok):
            duration = probe_media_duration_seconds('/tmp/input.mp4')
        assert duration == 123.45

    def test_probe_raises_when_ffprobe_fails(self) -> None:
        failed = subprocess.CompletedProcess(args=['ffprobe'], returncode=1, stdout='', stderr='err')
        with patch('backend.services.media_duration.subprocess.run', return_value=failed):
            with pytest.raises(MediaDurationError):
                probe_media_duration_seconds('/tmp/input.mp4')

    def test_probe_raises_when_duration_missing(self) -> None:
        ok = subprocess.CompletedProcess(args=['ffprobe'], returncode=0, stdout='', stderr='')
        with patch('backend.services.media_duration.subprocess.run', return_value=ok):
            with pytest.raises(MediaDurationError):
                probe_media_duration_seconds('/tmp/input.mp4')

    def test_probe_prefers_video_duration_when_audio_is_longer(self) -> None:
        ok = subprocess.CompletedProcess(
            args=['ffprobe'],
            returncode=0,
            stdout=(
                '{"streams": ['
                '{"codec_type":"video","duration":"133.166667"},'
                '{"codec_type":"audio","duration":"257.541224"}'
                '], "format": {"duration":"257.541224"}}'
            ),
            stderr='',
        )
        with patch('backend.services.media_duration.subprocess.run', return_value=ok):
            durations = probe_media_durations('/tmp/input.mp4')

        assert durations.video_seconds == 133.166667
        assert durations.audio_seconds == 257.541224
        assert durations.format_seconds == 257.541224
        assert durations.presentation_seconds == 133.166667

    def test_probe_falls_back_to_audio_for_audio_only_media(self) -> None:
        ok = subprocess.CompletedProcess(
            args=['ffprobe'],
            returncode=0,
            stdout='{"streams":[{"codec_type":"audio","duration":"12.5"}]}',
            stderr='',
        )
        with patch('backend.services.media_duration.subprocess.run', return_value=ok):
            assert probe_media_duration_seconds('/tmp/audio.mp3') == 12.5
