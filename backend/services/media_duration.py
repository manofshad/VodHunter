import json
import math
import subprocess
from dataclasses import dataclass


class MediaDurationError(Exception):
    pass


@dataclass(frozen=True)
class MediaDurations:
    """Durations reported by the individual media streams and container."""

    video_seconds: float | None = None
    audio_seconds: float | None = None
    format_seconds: float | None = None

    @property
    def presentation_seconds(self) -> float:
        """Return the duration the user sees/hears as the clip length."""
        for duration in (self.video_seconds, self.audio_seconds, self.format_seconds):
            if duration is not None:
                return duration
        raise MediaDurationError("Could not determine input media duration")


def _positive_duration(value: object) -> float | None:
    try:
        duration = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(duration) or duration <= 0:
        return None
    return duration


def probe_media_durations(path: str) -> MediaDurations:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "stream=codec_type,duration:format=duration",
        "-of",
        "json",
        path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise MediaDurationError("Could not determine input media duration")

    raw_duration = (result.stdout or "").strip()
    try:
        payload = json.loads(raw_duration)
    except json.JSONDecodeError as exc:
        raise MediaDurationError("Could not determine input media duration") from exc

    # Keep accepting the old scalar ffprobe output for callers/tests that provide
    # a custom ffprobe wrapper. It represents the container duration.
    if isinstance(payload, (int, float)):
        format_seconds = _positive_duration(payload)
        if format_seconds is None:
            raise MediaDurationError("Could not determine input media duration")
        return MediaDurations(format_seconds=format_seconds)

    if not isinstance(payload, dict):
        raise MediaDurationError("Could not determine input media duration")

    streams = payload.get("streams") or []
    video_seconds = next(
        (
            duration
            for stream in streams
            if isinstance(stream, dict)
            and stream.get("codec_type") == "video"
            and (duration := _positive_duration(stream.get("duration"))) is not None
        ),
        None,
    )
    audio_seconds = next(
        (
            duration
            for stream in streams
            if isinstance(stream, dict)
            and stream.get("codec_type") == "audio"
            and (duration := _positive_duration(stream.get("duration"))) is not None
        ),
        None,
    )
    format_seconds = _positive_duration((payload.get("format") or {}).get("duration"))

    durations = MediaDurations(
        video_seconds=video_seconds,
        audio_seconds=audio_seconds,
        format_seconds=format_seconds,
    )
    durations.presentation_seconds
    return durations


def probe_media_duration_seconds(path: str) -> float:
    """Return video duration, falling back to audio/container duration."""
    return probe_media_durations(path).presentation_seconds
