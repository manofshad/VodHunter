from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import json
import time
from typing import Any

from backend.services.monitor_manager import MonitorManager


class EventSubAuthError(Exception):
    pass


@dataclass
class EventSubResult:
    status_code: int
    body: str = ""
    media_type: str = "application/json"


class EventSubHandler:
    def __init__(
        self,
        monitor_manager: MonitorManager,
        secret: str,
        message_ttl_seconds: int = 600,
        max_clock_skew_seconds: int = 600,
    ):
        self.monitor_manager = monitor_manager
        self.secret = secret.strip()
        self.message_ttl_seconds = int(message_ttl_seconds)
        self.max_clock_skew_seconds = int(max_clock_skew_seconds)
        self._seen_message_ids: dict[str, float] = {}

    def process(
        self,
        headers: dict[str, str],
        raw_body: bytes,
    ) -> EventSubResult:
        self.verify_signature(headers=headers, raw_body=raw_body)

        message_id = headers.get("twitch-eventsub-message-id", "").strip()
        if self._is_duplicate(message_id):
            return EventSubResult(status_code=204, body="", media_type="text/plain")

        payload = json.loads(raw_body.decode("utf-8"))
        message_type = headers.get("twitch-eventsub-message-type", "").strip().lower()
        self.monitor_manager.mark_eventsub_healthy()
        self.monitor_manager.note_eventsub_event()

        if message_type == "webhook_callback_verification":
            challenge = str(payload.get("challenge", "")).strip()
            return EventSubResult(status_code=200, body=challenge, media_type="text/plain")

        if message_type == "notification":
            self._handle_notification(payload)
            return EventSubResult(status_code=204, body="", media_type="text/plain")

        if message_type == "revocation":
            sub = payload.get("subscription") or {}
            reason = str(sub.get("status", "")).strip() or "revoked"
            self.monitor_manager.mark_eventsub_degraded(f"EventSub revocation: {reason}")
            return EventSubResult(status_code=204, body="", media_type="text/plain")

        return EventSubResult(status_code=204, body="", media_type="text/plain")

    def verify_signature(self, headers: dict[str, str], raw_body: bytes) -> None:
        if not self.secret:
            raise EventSubAuthError("EventSub secret is not configured")

        message_id = headers.get("twitch-eventsub-message-id", "").strip()
        timestamp = headers.get("twitch-eventsub-message-timestamp", "").strip()
        received_signature = headers.get("twitch-eventsub-message-signature", "").strip()

        if not message_id or not timestamp or not received_signature:
            raise EventSubAuthError("Missing EventSub signature headers")

        ts = self._parse_timestamp(timestamp)
        now = time.time()
        if abs(now - ts) > self.max_clock_skew_seconds:
            raise EventSubAuthError("EventSub timestamp is outside allowed skew")

        message = (message_id + timestamp).encode("utf-8") + raw_body
        expected = "sha256=" + hmac.new(
            self.secret.encode("utf-8"),
            message,
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(expected, received_signature):
            raise EventSubAuthError("Invalid EventSub signature")

    def _handle_notification(self, payload: dict[str, Any]) -> None:
        sub = payload.get("subscription") or {}
        event = payload.get("event") or {}

        sub_type = str(sub.get("type", "")).strip()
        streamer = str(event.get("broadcaster_user_login", "")).strip().lower()
        if not streamer:
            return

        if sub_type == "stream.online":
            self.monitor_manager.on_stream_online(streamer=streamer, event_payload=event)
        elif sub_type == "stream.offline":
            self.monitor_manager.on_stream_offline(streamer=streamer, event_payload=event)

    def _is_duplicate(self, message_id: str) -> bool:
        if not message_id:
            return False

        now = time.time()
        expired = [mid for mid, deadline in self._seen_message_ids.items() if deadline <= now]
        for mid in expired:
            self._seen_message_ids.pop(mid, None)

        if message_id in self._seen_message_ids:
            return True

        self._seen_message_ids[message_id] = now + self.message_ttl_seconds
        return False

    @staticmethod
    def _parse_timestamp(value: str) -> float:
        raw = value.strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
