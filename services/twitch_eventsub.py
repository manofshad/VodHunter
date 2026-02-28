from __future__ import annotations

from typing import Any

from services.twitch_monitor import TwitchMonitor


class EventSubClient:
    def __init__(self, twitch_monitor: TwitchMonitor):
        self.twitch_monitor = twitch_monitor

    def create_stream_online_subscription(
        self,
        user_id: str,
        callback_url: str,
        secret: str,
    ) -> dict[str, Any]:
        return self._create_stream_subscription("stream.online", user_id, callback_url, secret)

    def create_stream_offline_subscription(
        self,
        user_id: str,
        callback_url: str,
        secret: str,
    ) -> dict[str, Any]:
        return self._create_stream_subscription("stream.offline", user_id, callback_url, secret)

    def list_subscriptions(self) -> list[dict[str, Any]]:
        payload = self.twitch_monitor._helix_request("eventsub/subscriptions", method="GET")
        rows = payload.get("data") or []
        return [row for row in rows if isinstance(row, dict)]

    def delete_subscription(self, subscription_id: str) -> None:
        subscription_id = subscription_id.strip()
        if not subscription_id:
            raise ValueError("subscription_id is required")
        self.twitch_monitor._helix_request(
            "eventsub/subscriptions",
            method="DELETE",
            params={"id": subscription_id},
        )

    def ensure_stream_subscriptions(
        self,
        user_id: str,
        callback_url: str,
        secret: str,
    ) -> dict[str, str]:
        user_id = user_id.strip()
        callback_url = callback_url.strip()
        secret = secret.strip()
        if not user_id:
            raise ValueError("user_id is required")
        if not callback_url:
            raise ValueError("callback_url is required")
        if not secret:
            raise ValueError("secret is required")

        rows = self.list_subscriptions()
        desired_types = {"stream.online", "stream.offline"}
        valid_statuses = {"enabled", "webhook_callback_verification_pending"}
        kept_ids: dict[str, str] = {}
        duplicates_to_delete: list[str] = []

        for row in rows:
            sub_type = str(row.get("type", "")).strip()
            if sub_type not in desired_types:
                continue

            condition = row.get("condition") or {}
            broadcaster_user_id = str(condition.get("broadcaster_user_id", "")).strip()
            if broadcaster_user_id != user_id:
                continue

            sub_id = str(row.get("id", "")).strip()
            if not sub_id:
                continue

            transport = row.get("transport") or {}
            method = str(transport.get("method", "")).strip()
            callback = str(transport.get("callback", "")).strip()
            status = str(row.get("status", "")).strip()

            is_valid = method == "webhook" and callback == callback_url and status in valid_statuses
            if not is_valid:
                duplicates_to_delete.append(sub_id)
                continue

            if sub_type in kept_ids:
                duplicates_to_delete.append(sub_id)
                continue

            kept_ids[sub_type] = sub_id

        for sub_id in duplicates_to_delete:
            self.delete_subscription(sub_id)

        if "stream.online" not in kept_ids:
            created = self.create_stream_online_subscription(user_id, callback_url, secret)
            created_id = str(created.get("id", "")).strip()
            if created_id:
                kept_ids["stream.online"] = created_id

        if "stream.offline" not in kept_ids:
            created = self.create_stream_offline_subscription(user_id, callback_url, secret)
            created_id = str(created.get("id", "")).strip()
            if created_id:
                kept_ids["stream.offline"] = created_id

        return kept_ids

    def cleanup_for_broadcaster(self, user_id: str) -> None:
        user_id = user_id.strip()
        if not user_id:
            return

        rows = self.list_subscriptions()
        for row in rows:
            sub_type = str(row.get("type", "")).strip()
            if sub_type not in {"stream.online", "stream.offline"}:
                continue
            condition = row.get("condition") or {}
            broadcaster_user_id = str(condition.get("broadcaster_user_id", "")).strip()
            if broadcaster_user_id != user_id:
                continue
            sub_id = str(row.get("id", "")).strip()
            if sub_id:
                self.delete_subscription(sub_id)

    def _create_stream_subscription(
        self,
        sub_type: str,
        user_id: str,
        callback_url: str,
        secret: str,
    ) -> dict[str, Any]:
        payload = self.twitch_monitor._helix_request(
            "eventsub/subscriptions",
            method="POST",
            body={
                "type": sub_type,
                "version": "1",
                "condition": {"broadcaster_user_id": user_id},
                "transport": {
                    "method": "webhook",
                    "callback": callback_url,
                    "secret": secret,
                },
            },
        )
        rows = payload.get("data") or []
        if not rows or not isinstance(rows[0], dict):
            raise RuntimeError(f"Failed to create EventSub subscription for {sub_type}")
        return rows[0]
