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

VODHUNTER_SITE_URL = "https://vodhunter.com"
VODHUNTER_SHORTCUT_NAME = "VodHunter Search"
VODHUNTER_VAPID_SUBJECT = "https://vodhunter.com"


def _load_vapid_private_key(value: str):
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec

    encoded = value.encode("utf-8")
    if "-----BEGIN" in value:
        return serialization.load_pem_private_key(encoded, password=None)

    padding = "=" * (-len(value) % 4)
    private_bytes = base64.urlsafe_b64decode(value + padding)
    try:
        return serialization.load_der_private_key(private_bytes, password=None)
    except ValueError:
        if len(private_bytes) != 32:
            raise
        return ec.derive_private_key(int.from_bytes(private_bytes, "big"), ec.SECP256R1())


def _public_vapid_key(private_key: str) -> str:
    from cryptography.hazmat.primitives import serialization

    public_bytes = _load_vapid_private_key(private_key).public_key().public_bytes(
        encoding=serialization.Encoding.X962,
        format=serialization.PublicFormat.UncompressedPoint,
    )
    return base64.urlsafe_b64encode(public_bytes).decode("ascii").rstrip("=")


def _private_vapid_key_as_der(private_key: str) -> str:
    from cryptography.hazmat.primitives import serialization

    private_der = _load_vapid_private_key(private_key).private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return base64.urlsafe_b64encode(private_der).decode("ascii").rstrip("=")


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
        private_key = os.getenv("WEB_PUSH_VAPID_PRIVATE_KEY", "").strip().replace("\\n", "\n")
        return cls(
            public_key=_public_vapid_key(private_key) if private_key else "",
            private_key=private_key,
            subject=VODHUNTER_VAPID_SUBJECT,
            site_url=VODHUNTER_SITE_URL,
            shortcut_name=VODHUNTER_SHORTCUT_NAME,
        )


PushSender = Callable[[PushSubscriptionRecord, str, WebPushConfig], None]


def _send_web_push(
    subscription: PushSubscriptionRecord,
    payload: str,
    config: WebPushConfig,
) -> None:
    from pywebpush import webpush  # Imported lazily so disabled Push remains optional in local tooling.

    webpush(
        subscription_info={
            "endpoint": subscription.endpoint,
            "keys": {"p256dh": subscription.p256dh, "auth": subscription.auth},
        },
        data=payload,
        vapid_private_key=_private_vapid_key_as_der(config.private_key),
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
