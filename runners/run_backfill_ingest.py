from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from typing import Callable

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")

from backend import config
from backend.bootstrap_ingest import build_ingest_state
from backend.bootstrap_shared import build_store_state, prepare_runtime_dirs
from pipeline.ingest_session import IngestSession
from services.twitch_monitor import TwitchMonitor
from sources.historical_archive_vod_source import HistoricalArchiveVODSource


@dataclass
class BackfillResult:
    ingested: int = 0
    resumed: int = 0
    skipped: int = 0
    failed: int = 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill Twitch archive VODs for a streamer.")
    parser.add_argument("--streamer", required=True, help="Twitch login name")
    parser.add_argument("--days", required=True, type=int, help="Number of past days to ingest")
    return parser


def run_backfill_ingest(
    streamer: str,
    days: int,
    *,
    monitor: TwitchMonitor | None = None,
    build_store: Callable[[], dict[str, object]] = build_store_state,
    build_ingest: Callable[[], dict[str, object]] = build_ingest_state,
    source_factory: Callable[..., HistoricalArchiveVODSource] = HistoricalArchiveVODSource,
    session_factory: Callable[..., IngestSession] = IngestSession,
    out: Callable[[str], None] = print,
) -> BackfillResult:
    normalized_streamer = streamer.strip().lower()
    if not normalized_streamer:
        raise ValueError("streamer is required")
    if int(days) < 1:
        raise ValueError("days must be >= 1")

    prepare_runtime_dirs()
    store_state = build_store()
    ingest_state = build_ingest()
    store = store_state["store"]
    embedder = ingest_state["embedder"]

    twitch_monitor = monitor or TwitchMonitor.from_env()
    creator_metadata = twitch_monitor.get_user_profile(normalized_streamer)
    user_id = str(creator_metadata["id"])
    cutoff = datetime.now(timezone.utc) - timedelta(days=int(days))
    vods = twitch_monitor.list_archive_vods_since(user_id=user_id, created_after=cutoff)

    result = BackfillResult()
    total_vods = len(vods)
    for index, vod in enumerate(vods, start=1):
        existing_video = store.get_video_by_url(str(vod["url"]))
        if existing_video is not None and bool(existing_video[5]):
            result.skipped += 1
            out(f"starting vod {index}/{total_vods} vod={vod['id']} status=skip_check")
            out(f"skip processed vod={vod['id']} url={vod['url']}")
            continue

        existing_state = store.get_vod_ingest_state(str(vod["id"]))
        starting_cursor = 0
        if existing_state is not None and int(existing_state.get("last_ingested_seconds", 0)) > 0:
            result.resumed += 1
            starting_cursor = int(existing_state["last_ingested_seconds"])
            out(f"resume vod={vod['id']} cursor={existing_state['last_ingested_seconds']}")

        out(
            f"starting vod {index}/{total_vods} vod={vod['id']} "
            f"url={vod['url']} cursor={starting_cursor}"
        )

        def emit_progress(event: dict[str, object]) -> None:
            event_type = str(event.get("event") or "")
            if event_type == "chunk_start":
                out(
                    "processing "
                    f"vod={event['vod_id']} chunk={int(event['start_seconds'])}-{int(event['end_seconds'])} "
                    f"progress={float(event['percent_complete']):.1f}% "
                    f"overall={index}/{total_vods}"
                )
                return
            if event_type == "vod_complete":
                out(f"completed vod={event['vod_id']} progress=100.0% overall={index}/{total_vods}")

        source = source_factory(
            streamer=normalized_streamer,
            vod_metadata=vod,
            creator_metadata=creator_metadata,
            store=store,
            chunk_seconds=config.INGEST_CHUNK_SECONDS,
            temp_dir=config.TEMP_BACKFILL_DIR,
            progress_callback=emit_progress,
        )

        session = session_factory(
            source=source,
            embedder=embedder,
            store=store,
            poll_interval=config.SESSION_POLL_INTERVAL,
        )

        try:
            session.run()
            result.ingested += 1
            out(f"ingested vod={vod['id']} url={vod['url']}")
        except Exception as exc:
            result.failed += 1
            out(f"failed vod={vod['id']} url={vod['url']} error={exc}")

    out(
        "summary "
        f"streamer={normalized_streamer} ingested={result.ingested} resumed={result.resumed} "
        f"skipped={result.skipped} failed={result.failed}"
    )
    return result


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_backfill_ingest(args.streamer, args.days)
    return 1 if result.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
