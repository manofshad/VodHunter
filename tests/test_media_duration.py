import pytest
import subprocess
from unittest.mock import patch
from backend.services.media_duration import MediaDurationError, probe_media_duration_seconds

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
