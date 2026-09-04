from __future__ import annotations

from datetime import datetime, timedelta, timezone
import argparse
import logging
from pathlib import Path
import sys
import time
from typing import Callable

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")

from backend import config
from services.vod_retention import VodRetentionResult, purge_expired_vods


logger = logging.getLogger("vodhunter.retention")
DAILY_RUN_HOUR_UTC = 3
DAILY_RUN_MINUTE_UTC = 0
MAX_RUN_ATTEMPTS = 4
RETRY_BASE_DELAY_SECONDS = 60.0
RETRY_MAX_DELAY_SECONDS = 15 * 60.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Delete expired local VOD search data.")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one retention pass and exit instead of waiting for the daily schedule.",
    )
    return parser


def run_once(
    *,
    database_url: str | None = None,
    retention_days: int | None = None,
    purge: Callable[..., VodRetentionResult] = purge_expired_vods,
) -> VodRetentionResult:
    if database_url is None or retention_days is None:
        config.validate_vod_retention_config()
    resolved_database_url = config.DATABASE_URL if database_url is None else database_url
    resolved_retention_days = config.VOD_RETENTION_DAYS if retention_days is None else retention_days

    result = purge(
        resolved_database_url,
        resolved_retention_days,
    )
    logger.info(
        "event=vod_retention_completed retention_days=%d deleted_count=%d deleted_video_ids=%s",
        result.retention_days,
        result.deleted_count,
        ",".join(str(video_id) for video_id in result.deleted_video_ids) or "none",
    )
    return result


def seconds_until_next_daily_run(
    now: datetime | None = None,
    *,
    run_hour_utc: int = DAILY_RUN_HOUR_UTC,
    run_minute_utc: int = DAILY_RUN_MINUTE_UTC,
) -> float:
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    next_run = current.replace(
        hour=int(run_hour_utc),
        minute=int(run_minute_utc),
        second=0,
        microsecond=0,
    )
    if next_run <= current:
        next_run += timedelta(days=1)
    return max((next_run - current).total_seconds(), 0.0)


def run_with_retries(
    *,
    run_pass: Callable[[], VodRetentionResult] = run_once,
    sleep: Callable[[float], None] = time.sleep,
    should_stop: Callable[[], bool] | None = None,
    max_attempts: int = MAX_RUN_ATTEMPTS,
    retry_base_delay_seconds: float = RETRY_BASE_DELAY_SECONDS,
    retry_max_delay_seconds: float = RETRY_MAX_DELAY_SECONDS,
) -> VodRetentionResult | None:
    """Run one scheduled pass with bounded exponential retry delays."""

    stop_requested = should_stop or (lambda: False)
    normalized_max_attempts = max(int(max_attempts), 1)
    normalized_base_delay = max(float(retry_base_delay_seconds), 0.0)
    normalized_max_delay = max(float(retry_max_delay_seconds), normalized_base_delay)

    for attempt in range(1, normalized_max_attempts + 1):
        if stop_requested():
            return None
        try:
            return run_pass()
        except Exception:
            if attempt >= normalized_max_attempts:
                logger.exception(
                    "event=vod_retention_failed attempt=%d max_attempts=%d next_retry=next_daily_run",
                    attempt,
                    normalized_max_attempts,
                )
                return None

            retry_delay_seconds = min(
                normalized_base_delay * (2 ** (attempt - 1)),
                normalized_max_delay,
            )
            logger.exception(
                "event=vod_retention_retry_scheduled attempt=%d max_attempts=%d retry_in_seconds=%.0f",
                attempt,
                normalized_max_attempts,
                retry_delay_seconds,
            )
            sleep(retry_delay_seconds)

    return None


def run_forever(
    *,
    run_pass: Callable[[], VodRetentionResult] = run_once,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], datetime] | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> None:
    clock = now or (lambda: datetime.now(timezone.utc))
    stop_requested = should_stop or (lambda: False)

    while not stop_requested():
        delay_seconds = seconds_until_next_daily_run(clock())
        logger.info(
            "event=vod_retention_scheduled next_run_in_seconds=%.0f run_hour_utc=%02d:%02d",
            delay_seconds,
            DAILY_RUN_HOUR_UTC,
            DAILY_RUN_MINUTE_UTC,
        )
        sleep(delay_seconds)
        if stop_requested():
            return
        run_with_retries(
            run_pass=run_pass,
            sleep=sleep,
            should_stop=stop_requested,
        )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        if args.once:
            run_once()
        else:
            run_forever()
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
