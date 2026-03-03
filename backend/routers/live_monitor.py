from fastapi import APIRouter, HTTPException, Query, Request

from backend import config
from backend.schemas import (
    ErrorResponse,
    LiveSessionItem,
    LiveStartRequest,
    LiveStartResponse,
    LiveStatusResponse,
    LiveStopResponse,
)
from backend.services.monitor_manager import MonitorConflictError

router = APIRouter(prefix="/api", tags=["live"])


@router.get("/live/status", response_model=LiveStatusResponse)
def get_live_status(request: Request) -> LiveStatusResponse:
    status = request.app.state.monitor_manager.get_status()
    return LiveStatusResponse(**status.__dict__)


@router.post(
    "/live/start",
    response_model=LiveStartResponse,
    responses={409: {"model": ErrorResponse}},
)
def start_live_monitor(request: Request, payload: LiveStartRequest) -> LiveStartResponse:
    try:
        status = request.app.state.monitor_manager.start(payload.streamer)
    except MonitorConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": "MONITOR_RUNNING", "message": str(exc)},
        ) from exc
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_STREAMER", "message": str(exc)},
        ) from exc

    return LiveStartResponse(status=LiveStatusResponse(**status.__dict__))


@router.post("/live/stop", response_model=LiveStopResponse)
def stop_live_monitor(request: Request) -> LiveStopResponse:
    stopped = request.app.state.monitor_manager.stop()
    status = request.app.state.monitor_manager.get_status()
    return LiveStopResponse(stopped=stopped, status=LiveStatusResponse(**status.__dict__))


@router.get("/live/sessions", response_model=list[LiveSessionItem])
def list_live_sessions(
    request: Request,
    limit: int = Query(default=config.LIVE_SESSIONS_DEFAULT_LIMIT, ge=1, le=config.LIVE_SESSIONS_MAX_LIMIT),
    offset: int = Query(default=0, ge=0),
) -> list[LiveSessionItem]:
    rows = request.app.state.session_query.list_live_sessions(limit=limit, offset=offset)
    return [LiveSessionItem(**row) for row in rows]
