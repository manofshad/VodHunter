from dataclasses import dataclass

from fastapi.testclient import TestClient

from backend.apps.public import create_public_app
from backend.services.push_notifications import WebPushConfig
from storage.notification_repository import (
    InvalidNotificationTokenError,
    InvalidPairingCodeError,
    PairingRecord,
)


@dataclass
class StubPushService:
    enabled: bool = True
    config: WebPushConfig = WebPushConfig(
        public_key="public-key",
        private_key="private-key",
        subject="mailto:test@vodhunter.com",
        site_url="https://vodhunter.com",
        shortcut_name="VodHunter Search",
    )


class StubNotifications:
    def __init__(self) -> None:
        self.registration = None
        self.valid_browser_token = "browser-token"
        self.pairing_status = "connected"

    def register_subscription(self, **kwargs):
        self.registration = kwargs
        if kwargs["browser_token"] == "bad-token":
            raise InvalidNotificationTokenError("invalid")
        return 12, None if kwargs["browser_token"] else self.valid_browser_token

    def create_pairing(self, browser_token: str):
        if browser_token != self.valid_browser_token:
            raise InvalidNotificationTokenError("invalid")
        return PairingRecord(
            id="9f7cf239-5b35-4e56-96e8-1f61a4f7c3c0",
            code="one-time-code",
            expires_at="2026-09-15T12:05:00+00:00",
        )

    def get_pairing_status(self, pairing_id: str, browser_token: str):
        if browser_token != self.valid_browser_token:
            raise InvalidNotificationTokenError("invalid")
        return self.pairing_status if pairing_id == "9f7cf239-5b35-4e56-96e8-1f61a4f7c3c0" else None

    def claim_pairing(self, pairing_code: str):
        if pairing_code != "one-time-code":
            raise InvalidPairingCodeError("invalid")
        return "shortcut-secret"


def build_client(*, enabled: bool = True):
    app = create_public_app(enable_lifespan=False)
    app.state.notifications = StubNotifications()
    service = StubPushService(enabled=enabled)
    app.state.push_notification_service = service
    return app, TestClient(app)


def test_notification_config_only_exposes_the_public_vapid_key() -> None:
    _, client = build_client()
    with client:
        response = client.get("/api/notifications/config")

    assert response.status_code == 200
    assert response.json() == {"enabled": True, "vapid_public_key": "public-key"}
    assert "private-key" not in response.text


def test_register_subscription_creates_a_browser_installation_token() -> None:
    app, client = build_client()
    with client:
        response = client.post(
            "/api/notifications/subscriptions",
            json={
                "endpoint": "https://web.push.apple.com/subscription",
                "expirationTime": None,
                "keys": {"p256dh": "device-public-key", "auth": "auth-secret"},
            },
        )

    assert response.status_code == 200
    assert response.json() == {"enabled": True, "installation_token": "browser-token"}
    assert app.state.notifications.registration["endpoint"] == "https://web.push.apple.com/subscription"


def test_subscription_rejects_non_apple_endpoints_to_prevent_server_side_request_forgery() -> None:
    _, client = build_client()
    with client:
        response = client.post(
            "/api/notifications/subscriptions",
            json={
                "endpoint": "https://internal.example/push",
                "keys": {"p256dh": "device-public-key", "auth": "auth-secret"},
            },
        )

    assert response.status_code == 400
    assert "Only Apple Web Push" in response.text


def test_pairing_url_contains_only_a_short_lived_code_and_callbacks() -> None:
    _, client = build_client()
    with client:
        response = client.post(
            "/api/notifications/pairings",
            headers={"Authorization": "Bearer browser-token"},
        )

    assert response.status_code == 200
    shortcut_url = response.json()["shortcut_url"]
    assert shortcut_url.startswith("shortcuts://x-callback-url/run-shortcut?")
    assert "VodHunter%20Search" in shortcut_url
    assert "vodhunter-setup%3Aone-time-code" in shortcut_url
    assert "shortcut-secret" not in shortcut_url
    assert "x-success=https%3A%2F%2Fvodhunter.com%2Fnotifications%2Fconnected" in shortcut_url


def test_shortcut_claim_is_form_encoded() -> None:
    _, client = build_client()
    with client:
        response = client.post(
            "/api/notifications/pairings/claim",
            data={"pairing_code": "one-time-code"},
        )

    assert response.status_code == 200
    assert response.json() == {"status": "connected", "shortcut_token": "shortcut-secret"}


def test_invalid_or_reused_pairing_code_is_rejected() -> None:
    _, client = build_client()
    with client:
        response = client.post(
            "/api/notifications/pairings/claim",
            data={"pairing_code": "already-used"},
        )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "INVALID_PAIRING_CODE"


def test_pairing_status_requires_the_browser_installation_token() -> None:
    _, client = build_client()
    with client:
        response = client.get(
            "/api/notifications/pairings/9f7cf239-5b35-4e56-96e8-1f61a4f7c3c0"
        )

    assert response.status_code == 401
