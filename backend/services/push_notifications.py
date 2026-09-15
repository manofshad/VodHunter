"""Best-effort Web Push delivery for terminal public search jobs."""

from __future__ import annotations

import base64
from dataclasses import dataclass
import json
import logging
import os
from typing import Callable

from storage.notification_repository import NotificationRepository, PushSubscriptionRecord


logger = logging.getLogger("uvicorn.error")


@dataclass(frozen=True)
class WebPushConfig:
    public_key: str
    private_key: str
    subject: str
    site_url: str
    shortcut_name: str

    @property
    def enabled(self) -> bool:
        return bool(
            self.public_key
            and self.private_key
            and self.subject
            and self.site_url
            and self.shortcut_name
        )

    @classmethod
    def from_env(cls) -> "WebPushConfig":
        return cls(
            public_key=os.getenv("WEB_PUSH_VAPID_PUBLIC_KEY", "").strip(),
            private_key=os.getenv("WEB_PUSH_VAPID_PRIVATE_KEY", "").strip().replace("\\n", "\n"),
            subject=os.getenv("WEB_PUSH_SUBJECT", "").strip(),
            site_url=os.getenv("PUBLIC_SITE_URL", "https://vodhunter.com").strip().rstrip("/"),
            shortcut_name=os.getenv("VODHUNTER_SHORTCUT_NAME", "VodHunter Search").strip(),
        )


PushSender = Callable[[PushSubscriptionRecord, str, WebPushConfig], None]


def _send_web_push(
    subscription: PushSubscriptionRecord,
    payload: str,
    config: WebPushConfig,
) -> None:
    from pywebpush import webpush  # Imported lazily so disabled Push remains optional in local tooling.

    private_key = config.private_key
    if "-----BEGIN" in private_key:
        from cryptography.hazmat.primitives import serialization

        loaded_key = serialization.load_pem_private_key(private_key.encode("utf-8"), password=None)
        private_der = loaded_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        private_key = base64.urlsafe_b64encode(private_der).decode("ascii").rstrip("=")

    webpush(
        subscription_info={
            "endpoint": subscription.endpoint,
            "keys": {"p256dh": subscription.p256dh, "auth": subscription.auth},
        },
        data=payload,
        vapid_private_key=private_key,
        vapid_claims={"sub": config.subject},
        timeout=10,
        ttl=86_400,
    )


class PushNotificationService:
    def __init__(
        self,
        notifications: NotificationRepository,
        config: WebPushConfig,
        *,
        sender: PushSender | None = None,
    ) -> None:
        self.notifications = notifications
        self.config = config
        self.sender = sender or _send_web_push

    @property
    def enabled(self) -> bool:
        return self.config.enabled

    def notify_search_finished(
        self,
        *,
        search_id: int,
        status: str,
        streamer: str | None,
        found: bool | None = None,
        error_message: str | None = None,
    ) -> bool:
        if not self.enabled:
            return False

        subscriptions = self.notifications.list_search_subscriptions(search_id)
        if not subscriptions:
            return False

        streamer_name = streamer or "the selected streamer"
        if status == "failed":
            title = "VodHunter search failed"
            body = error_message or "Open VodHunter to try the search again."
        elif found:
            title = "VodHunter found the Twitch moment"
            body = f"Tap to view the match for {streamer_name}."
        else:
            title = "VodHunter search finished"
            body = f"No exact match was found for {streamer_name}."

        result_url = f"{self.config.site_url}/share?search_id={int(search_id)}"
        payload = json.dumps(
            {
                "title": title,
                "body": body,
                "url": result_url,
                "tag": f"vodhunter-search-{int(search_id)}",
            },
            separators=(",", ":"),
        )

        delivered = False
        for subscription in subscriptions:
            try:
                self.sender(subscription, payload, self.config)
                delivered = True
            except Exception as exc:
                status_code = getattr(exc, "status_code", None) or getattr(
                    getattr(exc, "response", None), "status_code", None
                )
                if status_code in {404, 410}:
                    self.notifications.mark_subscription_revoked(subscription.id)
                logger.warning(
                    "Web Push delivery failed search_id=%s subscription_id=%s status=%s: %s",
                    search_id,
                    subscription.id,
                    status_code,
                    exc,
                )

        if delivered:
            self.notifications.mark_search_notification_sent(search_id)
        return delivered
