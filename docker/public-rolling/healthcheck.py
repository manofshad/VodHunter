import json
import sys
from urllib.error import URLError
from urllib.request import urlopen


def main() -> None:
    with urlopen("http://127.0.0.1/api/health", timeout=3) as response:
        health = json.load(response)
    if health.get("ok") is not True or health.get("nmfp_loaded") is not True:
        raise ValueError("The API model is not ready")
    with urlopen("http://127.0.0.1/api/search/streamers", timeout=3) as response:
        streamers = json.load(response)
    if not isinstance(streamers, list):
        raise ValueError("The database-backed streamer list is not ready")


if __name__ == "__main__":
    try:
        main()
    except (URLError, TimeoutError, ValueError, OSError) as exc:
        print(f"Public API readiness failed: {exc}", file=sys.stderr)
        sys.exit(1)
