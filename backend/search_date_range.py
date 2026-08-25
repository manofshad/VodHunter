from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
import re

from fastapi import HTTPException

from search.models import SearchDateRange


_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def parse_search_date_range(
    streamed_from: str | None,
    streamed_to: str | None,
) -> SearchDateRange | None:
    from_at = _parse_utc_day_start(streamed_from, field_name="streamed_from")
    to_at = _parse_utc_exclusive_day_end(streamed_to, field_name="streamed_to")
    if from_at is None and to_at is None:
        return None
    if from_at is not None and to_at is not None and from_at >= to_at:
        raise _invalid_date_range("streamed_from must be on or before streamed_to")
    return SearchDateRange(streamed_from=from_at, streamed_to=to_at)


def _parse_utc_day_start(value: str | None, *, field_name: str) -> datetime | None:
    parsed = _parse_iso_date(value, field_name=field_name)
    if parsed is None:
        return None
    return datetime.combine(parsed, time.min, tzinfo=timezone.utc)


def _parse_utc_exclusive_day_end(value: str | None, *, field_name: str) -> datetime | None:
    parsed = _parse_iso_date(value, field_name=field_name)
    if parsed is None:
        return None
    return datetime.combine(parsed + timedelta(days=1), time.min, tzinfo=timezone.utc)


def _parse_iso_date(value: str | None, *, field_name: str) -> date | None:
    raw_value = (value or "").strip()
    if not raw_value:
        return None
    if _ISO_DATE_RE.fullmatch(raw_value) is None:
        raise _invalid_date_range(f"{field_name} must be a valid YYYY-MM-DD date")
    try:
        return date.fromisoformat(raw_value)
    except ValueError as exc:
        raise _invalid_date_range(f"{field_name} must be a valid YYYY-MM-DD date") from exc


def _invalid_date_range(message: str) -> HTTPException:
    return HTTPException(
        status_code=400,
        detail={
            "code": "INVALID_DATE_RANGE",
            "message": message,
        },
    )
