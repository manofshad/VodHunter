import subprocess


class MediaDurationError(Exception):
    pass


def probe_media_duration_seconds(path: str) -> float:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise MediaDurationError("Could not determine input video duration")

    raw_duration = (result.stdout or "").strip()
    try:
        duration_seconds = float(raw_duration)
    except ValueError as exc:
        raise MediaDurationError("Could not determine input video duration") from exc

    if duration_seconds <= 0:
        raise MediaDurationError("Could not determine input video duration")

    return duration_seconds
