from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from search.query_preprocessor import QueryPreprocessor


def test_prepare_outputs_exact_nmfp_wav_contract(tmp_path: Path) -> None:
    source = tmp_path / "input.mp4"
    source.write_bytes(b"media")
    preprocessor = QueryPreprocessor(str(tmp_path / "queries"))
    calls: list[list[str]] = []

    def fake_run(cmd, capture_output, text):
        assert capture_output is True
        assert text is True
        calls.append(cmd)
        Path(cmd[-3]).write_bytes(b"wav")
        return SimpleNamespace(returncode=0, stderr="")

    with patch("search.query_preprocessor.subprocess.run", side_effect=fake_run):
        result = Path(preprocessor.prepare(str(source)))

    assert result.exists()
    command = calls[0]
    assert command[command.index("-ar") + 1] == "8000"
    assert command[command.index("-ac") + 1] == "1"
    assert command[command.index("-c:a") + 1] == "pcm_s16le"


def test_prepare_caps_audio_at_video_duration(tmp_path: Path) -> None:
    source = tmp_path / "input.mp4"
    source.write_bytes(b"media")
    preprocessor = QueryPreprocessor(str(tmp_path / "queries"))
    calls: list[list[str]] = []

    def fake_run(cmd, capture_output, text):
        calls.append(cmd)
        Path(cmd[-3]).write_bytes(b"wav")
        return SimpleNamespace(returncode=0, stderr="")

    with patch("search.query_preprocessor.subprocess.run", side_effect=fake_run):
        result = Path(
            preprocessor.prepare(
                str(source),
                duration_limit_seconds=133.166667,
            )
        )

    assert result.exists()
    command = calls[0]
    assert command[command.index("-t") + 1] == "133.166667"
    assert "-vn" in command
