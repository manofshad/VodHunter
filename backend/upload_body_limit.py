import tempfile
from threading import BoundedSemaphore

from starlette.concurrency import run_in_threadpool
from starlette.responses import JSONResponse

from backend.services.uploaded_clip_store import MAX_MULTIPART_BYTES


MAX_INGRESS_REQUESTS = 2
_ingress_slots = BoundedSemaphore(MAX_INGRESS_REQUESTS)


class UploadBodyLimitMiddleware:
    def __init__(self, app, max_bytes: int = MAX_MULTIPART_BYTES):
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["method"] != "POST" or scope["path"] != "/api/search/clip":
            await self.app(scope, receive, send)
            return

        if not _ingress_slots.acquire(blocking=False):
            response = JSONResponse(
                {"detail": {
                    "code": "UPLOAD_CAPACITY_EXCEEDED",
                    "message": "Too many search requests are receiving or processing. Please try again shortly.",
                }},
                status_code=429,
                headers={"Cache-Control": "no-store"},
            )
            await response(scope, receive, send)
            return

        try:
            async def too_large():
                response = JSONResponse(
                    {"detail": {"code": "UPLOAD_TOO_LARGE", "message": "Search request must be 101 MiB or smaller."}},
                    status_code=413,
                    headers={"Cache-Control": "no-store"},
                )
                await response(scope, receive, send)

            for name, value in scope.get("headers", []):
                if name.lower() == b"content-encoding" and value.strip().lower() not in {b"", b"identity"}:
                    response = JSONResponse(
                        {"detail": {"code": "INVALID_SEARCH_INPUT", "message": "Encoded search request bodies are unsupported."}},
                        status_code=415,
                    )
                    await response(scope, receive, send)
                    return
                if name.lower() == b"content-length":
                    try:
                        if int(value) > self.max_bytes:
                            await too_large()
                            return
                    except ValueError:
                        pass
            with tempfile.SpooledTemporaryFile(max_size=1024 * 1024) as body:
                size = 0
                while True:
                    message = await receive()
                    if message["type"] == "http.disconnect":
                        return
                    chunk = message.get("body", b"")
                    size += len(chunk)
                    if size > self.max_bytes:
                        await too_large()
                        return
                    await run_in_threadpool(body.write, chunk)
                    if not message.get("more_body", False):
                        break
                body.seek(0)
                remaining = size
                replayed = False

                async def replay():
                    nonlocal remaining, replayed
                    if replayed:
                        return await receive()
                    chunk = await run_in_threadpool(body.read, 1024 * 1024)
                    remaining -= len(chunk)
                    replayed = remaining == 0
                    return {"type": "http.request", "body": chunk, "more_body": remaining > 0}

                await self.app(scope, replay, send)
        finally:
            _ingress_slots.release()
