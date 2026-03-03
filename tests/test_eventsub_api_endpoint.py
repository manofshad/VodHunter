import asyncio
import unittest
from pathlib import Path
import sys

from fastapi import HTTPException

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

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


class TestEventSubApiEndpoint(unittest.TestCase):
    def setUp(self) -> None:
        self.app = create_admin_app(enable_lifespan=False)
        self.handler = StubEventSubHandler()
        self.monitor = StubMonitorManager()
        self.app.state.eventsub_handler = self.handler
        self.app.state.monitor_manager = self.monitor

    def test_eventsub_success_returns_handler_response(self) -> None:
        req = FakeRequest(app=self.app, payload=b"{}", headers={"x-test": "1"})
        response = asyncio.run(handle_twitch_eventsub(req))  # type: ignore[arg-type]
        self.assertEqual(response.status_code, 204)
        self.assertEqual(self.handler.calls, 1)

    def test_eventsub_auth_error_maps_403(self) -> None:
        self.handler.raise_exc = EventSubAuthError("bad signature")
        req = FakeRequest(app=self.app, payload=b"{}", headers={})
        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(handle_twitch_eventsub(req))  # type: ignore[arg-type]
        self.assertEqual(ctx.exception.status_code, 403)
        self.assertEqual(ctx.exception.detail["code"], "EVENTSUB_AUTH_FAILED")
        self.assertTrue(self.monitor.degraded_calls)

    def test_eventsub_generic_error_maps_400(self) -> None:
        self.handler.raise_exc = RuntimeError("broken payload")
        req = FakeRequest(app=self.app, payload=b"{}", headers={})
        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(handle_twitch_eventsub(req))  # type: ignore[arg-type]
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.detail["code"], "EVENTSUB_HANDLER_ERROR")
        self.assertTrue(self.monitor.degraded_calls)


if __name__ == "__main__":
    unittest.main()
