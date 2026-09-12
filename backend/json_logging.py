"""JSON log formatting for the public API process."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
from typing import Any


class JsonFormatter(logging.Formatter):
    """Render Python and Uvicorn records as one JSON object per line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.name == "uvicorn.access" and isinstance(record.args, tuple):
            self._add_access_fields(payload, record.args)

        if record.exc_info and record.exc_info[1] is not None:
            exception = record.exc_info[1]
            payload["exception"] = {
                "type": type(exception).__name__,
                "message": str(exception),
                "stacktrace": self.formatException(record.exc_info),
            }

        if record.stack_info:
            payload["stacktrace"] = self.formatStack(record.stack_info)

        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def _add_access_fields(payload: dict[str, Any], args: tuple[object, ...]) -> None:
        if len(args) != 5:
            return

        client_address, method, path, http_version, raw_status_code = args
        try:
            status_code = int(raw_status_code)
        except (TypeError, ValueError):
            return

        payload.update(
            {
                "event": "http_request",
                "client_address": str(client_address),
                "http_method": str(method),
                "url_path": str(path),
                "http_version": str(http_version),
                "status_code": status_code,
            }
        )
        if status_code >= 500:
            payload["level"] = "error"
        elif status_code >= 400:
            payload["level"] = "warning"
