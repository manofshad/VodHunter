import base64
import json

import pytest

from backend.services import push_notifications
from backend.services.push_notifications import PushNotificationService, WebPushConfig
from storage.notification_repository import PushSubscriptionRecord


class StubNotifications:
    def __init__(self) -> None:
        self.subscriptions = [
            PushSubscriptionRecord(
                id=8,
                endpoint="https://push.example/device",
                p256dh="public-key",
                auth="auth-secret",
            )
        ]
        self.sent = []
        self.revoked = []

    def list_search_subscriptions(self, search_id: int):
        return list(self.subscriptions)

    def mark_search_notification_sent(self, search_id: int):
        self.sent.append(search_id)

    def mark_subscription_revoked(self, subscription_id: int):
        self.revoked.append(subscription_id)


def config() -> WebPushConfig:
    return WebPushConfig(
        public_key="public",
        private_key="private",
        subject="mailto:test@vodhunter.com",
        site_url="https://vodhunter.com",
        shortcut_name="VodHunter Search",
    )


def test_runtime_config_derives_public_key_and_keeps_public_values_in_code(monkeypatch) -> None:
    monkeypatch.setattr(push_notifications, "_public_vapid_key", lambda value: "derived-key")
    monkeypatch.setenv("WEB_PUSH_VAPID_PRIVATE_KEY", "private-key")
    monkeypatch.setenv("WEB_PUSH_VAPID_PUBLIC_KEY", "ignored-public-key")
    monkeypatch.setenv("WEB_PUSH_SUBJECT", "mailto:ignored@example.com")
    monkeypatch.setenv("PUBLIC_SITE_URL", "https://ignored.example")
    monkeypatch.setenv("VODHUNTER_SHORTCUT_NAME", "Ignored Shortcut")

    runtime_config = WebPushConfig.from_env()

    assert runtime_config.public_key == "derived-key"
    assert runtime_config.private_key == "private-key"
    assert runtime_config.subject == "https://vodhunter.com"
    assert runtime_config.site_url == "https://vodhunter.com"
    assert runtime_config.shortcut_name == "VodHunter Search"
    assert runtime_config.enabled is True


def test_public_vapid_key_is_derived_from_private_key() -> None:
    serialization = pytest.importorskip("cryptography.hazmat.primitives.serialization")
    ec = pytest.importorskip("cryptography.hazmat.primitives.asymmetric.ec")
    private_key = ec.generate_private_key(ec.SECP256R1())
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("ascii")
    expected_public_key = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.X962,
        format=serialization.PublicFormat.UncompressedPoint,
    )

    assert push_notifications._public_vapid_key(private_pem) == base64.urlsafe_b64encode(
        expected_public_key
    ).decode("ascii").rstrip("=")


def test_runtime_config_is_disabled_without_private_key(monkeypatch) -> None:
    monkeypatch.delenv("WEB_PUSH_VAPID_PRIVATE_KEY", raising=False)

    runtime_config = WebPushConfig.from_env()

    assert runtime_config.public_key == ""
    assert runtime_config.private_key == ""
    assert runtime_config.enabled is False


def test_matching_search_sends_a_tappable_result_notification() -> None:
    notifications = StubNotifications()
    deliveries = []
    service = PushNotificationService(
        notifications,
        config(),
        sender=lambda subscription, payload, push_config: deliveries.append(
            (subscription, json.loads(payload), push_config)
        ),
    )

    delivered = service.notify_search_finished(
        search_id=321,
        status="completed",
        streamer="jason",
        found=True,
    )

    assert delivered is True
    assert deliveries[0][1]["url"] == "https://vodhunter.com/share?search_id=321"
    assert deliveries[0][1]["title"] == "VodHunter found the Twitch moment"
    assert notifications.sent == [321]


def test_gone_push_subscription_is_revoked_without_marking_delivery() -> None:
    notifications = StubNotifications()

    class GoneError(Exception):
        response = type("Response", (), {"status_code": 410})()

    def fail(*_args):
        raise GoneError("gone")

    service = PushNotificationService(notifications, config(), sender=fail)

    assert service.notify_search_finished(
        search_id=322,
        status="failed",
        streamer="jason",
        error_message="Download failed",
    ) is False
    assert notifications.revoked == [8]
    assert notifications.sent == []


def test_disabled_push_does_not_query_subscriptions() -> None:
    notifications = StubNotifications()
    disabled = WebPushConfig("", "", "", "https://vodhunter.com", "VodHunter Search")
    service = PushNotificationService(notifications, disabled, sender=lambda *_args: None)

    assert service.notify_search_finished(
        search_id=323,
        status="completed",
        streamer="jason",
        found=False,
    ) is False
