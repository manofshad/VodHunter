import json

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
