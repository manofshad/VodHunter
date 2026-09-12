from __future__ import annotations

import json
import logging

from backend.json_logging import JsonFormatter


def _record(
    *,
    name: str = "uvicorn.error",
    level: int = logging.INFO,
    message: str = "API ready",
    args: tuple[object, ...] = (),
) -> logging.LogRecord:
    return logging.LogRecord(name, level, __file__, 1, message, args, None)


def test_json_formatter_emits_canonical_fields() -> None:
    payload = json.loads(JsonFormatter().format(_record()))

    assert payload["timestamp"].endswith("Z")
    assert payload["level"] == "info"
    assert payload["logger"] == "uvicorn.error"
    assert payload["message"] == "API ready"


def test_json_formatter_structures_access_log_and_maps_http_severity() -> None:
    record = _record(
        name="uvicorn.access",
        message='%s - "%s %s HTTP/%s" %d',
        args=("192.0.2.4:1234", "POST", "/api/search/clip", "1.1", 503),
    )

    payload = json.loads(JsonFormatter().format(record))

    assert payload["event"] == "http_request"
    assert payload["level"] == "error"
    assert payload["client_address"] == "192.0.2.4:1234"
    assert payload["http_method"] == "POST"
    assert payload["url_path"] == "/api/search/clip"
    assert payload["http_version"] == "1.1"
    assert payload["status_code"] == 503


def test_json_formatter_keeps_exception_in_one_json_record() -> None:
    try:
        raise ValueError("bad input")
    except ValueError as error:
        record = _record(level=logging.ERROR, message="Search failed")
        record.exc_info = (type(error), error, error.__traceback__)

    payload = json.loads(JsonFormatter().format(record))

    assert payload["level"] == "error"
    assert payload["exception"]["type"] == "ValueError"
    assert payload["exception"]["message"] == "bad input"
    assert "ValueError: bad input" in payload["exception"]["stacktrace"]
