from fastapi import APIRouter, HTTPException, Request, Response

from backend.services.eventsub_handler import EventSubAuthError

router = APIRouter(prefix="/api", tags=["eventsub"])


@router.post("/twitch/eventsub")
async def handle_twitch_eventsub(request: Request) -> Response:
    raw_body = await request.body()
    headers = {k.lower(): v for k, v in request.headers.items()}
    try:
        result = request.app.state.eventsub_handler.process(headers=headers, raw_body=raw_body)
    except EventSubAuthError as exc:
        request.app.state.monitor_manager.mark_eventsub_degraded(str(exc))
        raise HTTPException(
            status_code=403,
            detail={"code": "EVENTSUB_AUTH_FAILED", "message": str(exc)},
        ) from exc
    except Exception as exc:
        request.app.state.monitor_manager.mark_eventsub_degraded(f"EventSub handler error: {exc}")
        raise HTTPException(
            status_code=400,
            detail={"code": "EVENTSUB_HANDLER_ERROR", "message": str(exc)},
        ) from exc

    return Response(
        status_code=result.status_code,
        content=result.body,
        media_type=result.media_type,
    )
