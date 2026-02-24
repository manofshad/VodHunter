from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


def format_twitch_offset(seconds: int) -> str:
    whole_seconds = int(seconds)
    if whole_seconds < 0:
        raise ValueError("seconds must be non-negative")

    hours, remainder = divmod(whole_seconds, 3600)
    minutes, secs = divmod(remainder, 60)

    if hours > 0:
        return f"{hours}h{minutes}m{secs}s"
    if minutes > 0:
        return f"{minutes}m{secs}s"
    return f"{secs}s"


def build_twitch_timestamp_url(video_url: str, seconds: int | None) -> str | None:
    if seconds is None:
        return None

    whole_seconds = int(seconds)
    if whole_seconds < 0:
        return None

    parsed = urlparse(video_url)
    if not parsed.scheme or not parsed.netloc:
        return None

    params = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k != "t"]
    params.append(("t", format_twitch_offset(whole_seconds)))

    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            urlencode(params),
            parsed.fragment,
        )
    )
