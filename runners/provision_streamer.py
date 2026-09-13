"""Provision a Twitch creator and its embedding partition."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")

from backend.bootstrap_shared import build_repositories
from services.twitch_monitor import TwitchMonitor


def provision_streamer(streamer: str) -> tuple[int, str]:
    normalized = streamer.strip().lower()
    if not normalized:
        raise ValueError("streamer is required")

    repositories = build_repositories()
    monitor = TwitchMonitor.from_env()
    profile = monitor.get_user_profile(normalized)
    creator_id = repositories.videos.create_or_get_creator(
        normalized,
        f"https://twitch.tv/{normalized}",
        profile_image_url=profile.get("profile_image_url"),
    )
    partition_name = repositories.embedding_partitions.ensure_creator_partition(
        creator_id
    )
    return creator_id, partition_name


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Provision a Twitch creator and LIST-partitioned HNSW index."
    )
    parser.add_argument("--streamer", required=True, help="Twitch login name")
    args = parser.parse_args(argv)
    creator_id, partition_name = provision_streamer(args.streamer)
    print(
        f"provisioned streamer={args.streamer.strip().lower()} "
        f"creator_id={creator_id} partition={partition_name}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
