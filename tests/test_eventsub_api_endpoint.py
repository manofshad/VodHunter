import asyncio

import pytest
from fastapi import HTTPException

from backend.apps.admin import create_admin_app
from backend.routers.eventsub import handle_twitch_eventsub
from backend.services.eventsub_handler import EventSubAuthError, EventSubResult


class StubEventSubHandler:
    def __init__(self):
        self.raise_exc: Exception | None = None
        self.calls = 0

    def process(self, headers: dict[str, str], raw_body: bytes) -> EventSubResult:
        self.calls += 1
        if self.raise_exc is not None:
            raise self.raise_exc
        return EventSubResult(status_code=204, body="", media_type="text/plain")


class StubMonitorManager:
    def __init__(self):
        self.degraded_calls: list[str] = []

    def mark_eventsub_degraded(self, reason: str) -> None:
        self.degraded_calls.append(reason)


class FakeRequest:
    def __init__(self, app, payload: bytes, headers: dict[str, str]):
        self.app = app
        self._payload = payload
        self.headers = headers

    async def body(self) -> bytes:
        return self._payload


@pytest.fixture
def eventsub_app_state():
    app = create_admin_app(enable_lifespan=False)
    handler = StubEventSubHandler()
    monitor = StubMonitorManager()
    app.state.eventsub_handler = handler
    app.state.monitor_manager = monitor
    return app, handler, monitor


def test_eventsub_success_returns_handler_response(eventsub_app_state) -> None:
    app, handler, _monitor = eventsub_app_state
    req = FakeRequest(app=app, payload=b"{}", headers={"x-test": "1"})

    response = asyncio.run(handle_twitch_eventsub(req))  # type: ignore[arg-type]

    assert response.status_code == 204
    assert handler.calls == 1


def test_eventsub_auth_error_maps_403(eventsub_app_state) -> None:
    app, handler, monitor = eventsub_app_state
    handler.raise_exc = EventSubAuthError("bad signature")
    req = FakeRequest(app=app, payload=b"{}", headers={})

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(handle_twitch_eventsub(req))  # type: ignore[arg-type]

    assert exc_info.value.status_code == 403
    assert exc_info.value.detail["code"] == "EVENTSUB_AUTH_FAILED"
    assert monitor.degraded_calls


def test_eventsub_generic_error_maps_400(eventsub_app_state) -> None:
    app, handler, monitor = eventsub_app_state
    handler.raise_exc = RuntimeError("broken payload")
    req = FakeRequest(app=app, payload=b"{}", headers={})

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(handle_twitch_eventsub(req))  # type: ignore[arg-type]

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["code"] == "EVENTSUB_HANDLER_ERROR"
    assert monitor.degraded_calls
