import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
from threading import Event

import pytest
from starlette.requests import Request
from starlette.responses import JSONResponse

from backend.upload_body_limit import UploadBodyLimitMiddleware


def scope(headers=()):
    return {"type": "http", "method": "POST", "path": "/api/search/clip", "headers": list(headers)}


async def response_from(middleware, request_scope=None):
    sent = []

    async def receive():
        raise AssertionError("Rejected request must not read its body")

    async def send(message):
        sent.append(message)

    await middleware(request_scope or scope(), receive, send)
    return sent


def assert_capacity_response(sent):
    assert sent[0]["status"] == 429
    assert (b"cache-control", b"no-store") in sent[0]["headers"]
    assert json.loads(sent[1]["body"]) == {"detail": {
        "code": "UPLOAD_CAPACITY_EXCEEDED",
        "message": "Too many search requests are receiving or processing. Please try again shortly.",
    }}


@pytest.mark.parametrize("phase", ["receive", "parsing", "staging", "response"])
@pytest.mark.parametrize("content_type", ["multipart/form-data; boundary=clip", "application/x-www-form-urlencoded"])
def test_ingress_rejects_before_receive_and_holds_through_response(phase, content_type):
    async def run():
        release = asyncio.Event()
        entered = [asyncio.Event(), asyncio.Event()]
        body = (b"--clip\r\nContent-Disposition: form-data; name=\"streamer\"\r\n\r\nxqc\r\n"
                b"--clip\r\nContent-Disposition: form-data; name=\"clip_file\"; filename=\"clip.mp4\"\r\n\r\nvideo\r\n--clip--\r\n"
                if content_type.startswith("multipart") else b"streamer=xqc")
        request_scope = scope([(b"content-type", content_type.encode())])

        def middleware_for(index):
            async def pause():
                entered[index].set()
                await release.wait()

            async def app(request_scope, receive, send):
                async def parser_receive():
                    if phase == "parsing":
                        await pause()
                    return await receive()

                async with Request(request_scope, parser_receive).form() as form:
                    assert form["streamer"] == "xqc"
                    if content_type.startswith("multipart"):
                        assert await form["clip_file"].read() == b"video"
                    if phase == "staging":
                        await pause()
                await JSONResponse({"accepted": True})(request_scope, receive, send)

            async def receive():
                if phase == "receive":
                    await pause()
                return {"type": "http.request", "body": body, "more_body": False}

            async def send(message):
                if phase == "response" and message["type"] == "http.response.start":
                    await pause()

            return UploadBodyLimitMiddleware(app), receive, send

        requests = [middleware_for(index) for index in range(2)]
        tasks = [asyncio.create_task(middleware(request_scope, receive, send))
                 for middleware, receive, send in requests]
        try:
            for event in entered:
                await asyncio.wait_for(event.wait(), 5)

            async def forbidden_app(*args):
                raise AssertionError("Rejected request must not invoke the app")

            assert_capacity_response(await response_from(UploadBodyLimitMiddleware(forbidden_app), request_scope))
        finally:
            release.set()
            await asyncio.gather(*tasks)

        sent = []
        async def receive():
            return {"type": "http.request", "body": body, "more_body": False}
        async def send(message):
            sent.append(message)
        await requests[0][0](request_scope, receive, send)
        assert sent[0]["status"] == 200
        assert json.loads(sent[1]["body"]) == {"accepted": True}

    asyncio.run(run())


@pytest.mark.parametrize("outcome", [
    "content_length", "body_limit", "encoding", "disconnect", "receive_exception",
    "downstream_exception", "send_exception", "cancel_receive", "cancel_downstream",
])
def test_ingress_slot_recovers_after_every_exit(outcome):
    async def run():
        held = asyncio.Event()
        release = asyncio.Event()
        failing_entered = asyncio.Event()

        async def receive():
            return {"type": "http.request", "body": b"x", "more_body": False}

        async def send(message):
            pass

        async def holding_app(*args):
            held.set()
            await release.wait()

        holder = asyncio.create_task(UploadBodyLimitMiddleware(holding_app)(scope(), receive, send))
        try:
            await asyncio.wait_for(held.wait(), 5)

            async def failing_receive():
                if outcome == "disconnect":
                    return {"type": "http.disconnect"}
                if outcome == "receive_exception":
                    raise RuntimeError("receive failed")
                if outcome == "cancel_receive":
                    failing_entered.set()
                    await asyncio.Event().wait()
                return {"type": "http.request", "body": b"xx", "more_body": False}

            async def failing_app(request_scope, receive, send):
                if outcome == "downstream_exception":
                    raise RuntimeError("downstream failed")
                if outcome == "cancel_downstream":
                    failing_entered.set()
                    await asyncio.Event().wait()
                await JSONResponse({"accepted": True})(request_scope, receive, send)

            failed_sent = []
            async def failing_send(message):
                if outcome == "send_exception":
                    raise RuntimeError("send failed")
                failed_sent.append(message)

            headers = ([(b"content-length", b"2")] if outcome == "content_length" else
                       [(b"content-encoding", b"gzip")] if outcome == "encoding" else [])
            middleware = UploadBodyLimitMiddleware(failing_app, max_bytes=1 if outcome in {"content_length", "body_limit"} else 5)
            task = asyncio.create_task(middleware(scope(headers), failing_receive, failing_send))
            try:
                if outcome.startswith("cancel_"):
                    await asyncio.wait_for(failing_entered.wait(), 5)
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
                elif outcome.endswith("_exception"):
                    with pytest.raises(RuntimeError, match="failed"):
                        await task
                else:
                    await task
                    if outcome != "disconnect":
                        assert failed_sent[0]["status"] == (415 if outcome == "encoding" else 413)
            finally:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)

            recovered = []
            async def recovered_app(request_scope, receive, send):
                await JSONResponse({"recovered": True})(request_scope, receive, send)
            async def recovered_send(message):
                recovered.append(message)
            await UploadBodyLimitMiddleware(recovered_app)(scope(), receive, recovered_send)
            assert recovered[0]["status"] == 200
            assert json.loads(recovered[1]["body"]) == {"recovered": True}
        finally:
            release.set()
            await holder

    asyncio.run(run())


def test_ingress_is_process_wide_across_event_loops_and_other_routes_pass():
    entered = [Event(), Event()]
    release = Event()

    def hold(index):
        async def run():
            async def receive():
                entered[index].set()
                assert await asyncio.to_thread(release.wait, 5)
                return {"type": "http.disconnect"}
            async def app(*args):
                raise AssertionError("Disconnected request must not invoke the app")
            await UploadBodyLimitMiddleware(app)(scope(), receive, None)
        asyncio.run(run())

    with ThreadPoolExecutor(max_workers=2) as executor:
        tasks = [executor.submit(hold, index) for index in range(2)]
        try:
            for event in entered:
                assert event.wait(5)
            async def app(request_scope, receive, send):
                await JSONResponse({"path": request_scope["path"]})(request_scope, receive, send)
            middleware = UploadBodyLimitMiddleware(app)
            assert_capacity_response(asyncio.run(response_from(middleware)))
            for method, path in [("GET", "/api/search/clip"), ("POST", "/api/other"), ("GET", "/")]:
                sent = asyncio.run(response_from(middleware, {**scope(), "method": method, "path": path}))
                assert sent[0]["status"] == 200
                assert json.loads(sent[1]["body"]) == {"path": path}
        finally:
            release.set()
            for task in tasks:
                task.result(timeout=5)
