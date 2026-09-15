from __future__ import annotations

from urllib.parse import quote, urlencode, urlparse
from uuid import UUID

from fastapi import APIRouter, Form, Header, HTTPException, Request, status

from backend.schemas import (
    NotificationConfigResponse,
    NotificationPairingClaimResponse,
    NotificationPairingResponse,
    NotificationPairingStatusResponse,
    PushSubscriptionRequest,
    PushSubscriptionResponse,
)
from storage.notification_repository import (
    InvalidNotificationTokenError,
    InvalidPairingCodeError,
)


router = APIRouter(prefix="/api/notifications", tags=["notifications"])


def _bearer_token(authorization: str | None) -> str:
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "INVALID_NOTIFICATION_TOKEN", "message": "Notification token is required"},
        )
    return token.strip()


def _require_enabled(request: Request):
    push_service = request.app.state.push_notification_service
    if not push_service.enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "NOTIFICATIONS_NOT_CONFIGURED",
                "message": "Push notifications are not configured on this server",
            },
        )
    return push_service


def _validate_push_endpoint(endpoint: str) -> None:
    parsed = urlparse(endpoint)
    hostname = (parsed.hostname or "").lower()
    if parsed.scheme != "https" or not hostname.endswith(".push.apple.com"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "code": "INVALID_PUSH_ENDPOINT",
                "message": "Only Apple Web Push subscription endpoints are accepted",
            },
        )


@router.get("/config", response_model=NotificationConfigResponse)
def get_notification_config(request: Request) -> NotificationConfigResponse:
    push_service = request.app.state.push_notification_service
    return NotificationConfigResponse(
        enabled=push_service.enabled,
        vapid_public_key=push_service.config.public_key if push_service.enabled else None,
    )


@router.post("/subscriptions", response_model=PushSubscriptionResponse)
def register_push_subscription(
    payload: PushSubscriptionRequest,
    request: Request,
    authorization: str | None = Header(default=None),
) -> PushSubscriptionResponse:
    _require_enabled(request)
    _validate_push_endpoint(payload.endpoint)
    browser_token = None
    if authorization:
        browser_token = _bearer_token(authorization)
    try:
        _, new_token = request.app.state.notifications.register_subscription(
            endpoint=payload.endpoint,
            p256dh=payload.keys.p256dh,
            auth=payload.keys.auth,
            expiration_time=payload.expirationTime,
            browser_token=browser_token,
        )
    except InvalidNotificationTokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "INVALID_NOTIFICATION_TOKEN", "message": str(exc)},
        ) from exc
    return PushSubscriptionResponse(
        enabled=True,
        installation_token=new_token,
    )


@router.post("/pairings", response_model=NotificationPairingResponse)
def create_notification_pairing(
    request: Request,
    authorization: str | None = Header(default=None),
) -> NotificationPairingResponse:
    push_service = _require_enabled(request)
    browser_token = _bearer_token(authorization)
    try:
        pairing = request.app.state.notifications.create_pairing(browser_token)
    except InvalidNotificationTokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "INVALID_NOTIFICATION_TOKEN", "message": str(exc)},
        ) from exc

    site_url = push_service.config.site_url
    success_url = f"{site_url}/notifications/connected?pairing_id={quote(pairing.id)}"
    cancel_url = f"{site_url}/notifications/cancelled?pairing_id={quote(pairing.id)}"
    error_url = f"{site_url}/notifications/error?pairing_id={quote(pairing.id)}"
    shortcut_url = "shortcuts://x-callback-url/run-shortcut?" + urlencode(
        {
            "name": push_service.config.shortcut_name,
            "input": "text",
            "text": f"vodhunter-setup:{pairing.code}",
            "x-success": success_url,
            "x-cancel": cancel_url,
            "x-error": error_url,
        },
        quote_via=quote,
    )
    return NotificationPairingResponse(
        pairing_id=pairing.id,
        expires_at=pairing.expires_at,
        shortcut_url=shortcut_url,
    )


@router.get("/pairings/{pairing_id}", response_model=NotificationPairingStatusResponse)
def get_notification_pairing(
    pairing_id: UUID,
    request: Request,
    authorization: str | None = Header(default=None),
) -> NotificationPairingStatusResponse:
    browser_token = _bearer_token(authorization)
    try:
        pairing_status = request.app.state.notifications.get_pairing_status(str(pairing_id), browser_token)
    except InvalidNotificationTokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "INVALID_NOTIFICATION_TOKEN", "message": str(exc)},
        ) from exc
    if pairing_status is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "PAIRING_NOT_FOUND", "message": "Shortcut pairing was not found"},
        )
    return NotificationPairingStatusResponse(status=pairing_status)


@router.post("/pairings/claim", response_model=NotificationPairingClaimResponse)
def claim_notification_pairing(
    request: Request,
    pairing_code: str = Form(..., min_length=1, max_length=256),
) -> NotificationPairingClaimResponse:
    _require_enabled(request)
    try:
        shortcut_token = request.app.state.notifications.claim_pairing(pairing_code)
    except InvalidPairingCodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "INVALID_PAIRING_CODE", "message": str(exc)},
        ) from exc
    return NotificationPairingClaimResponse(status="connected", shortcut_token=shortcut_token)
