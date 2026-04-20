from __future__ import annotations

from secrets import compare_digest

from fastapi import APIRouter, Header, HTTPException, Request

from backend import config
from backend.schemas import (
    ErrorResponse,
    InternalVideoMutationRequest,
    InternalVideoMutationResponse,
)
from storage.vector_store import (
    InvalidVideoStateTransitionError,
    VideoNotFoundError,
    VideoOwnerMismatchError,
)


router = APIRouter(prefix="/internal/videos", tags=["internal_videos"])


def _raise_api_error(status_code: int, code: str, message: str) -> None:
    raise HTTPException(status_code=status_code, detail={"code": code, "message": message})


def _require_internal_api_key(x_internal_api_key: str | None = Header(default=None)) -> None:
    configured_key = config.INTERNAL_API_KEY
    provided_key = x_internal_api_key or ""
    if not configured_key or not compare_digest(provided_key, configured_key):
        _raise_api_error(
            status_code=401,
            code="INVALID_INTERNAL_API_KEY",
            message="X-Internal-Api-Key is missing or invalid",
        )


def _translate_store_error(exc: Exception) -> None:
    if isinstance(exc, VideoNotFoundError):
        _raise_api_error(status_code=404, code="VIDEO_NOT_FOUND", message="Video was not found")
    if isinstance(exc, VideoOwnerMismatchError):
        _raise_api_error(
            status_code=403,
            code="VIDEO_OWNER_MISMATCH",
            message="Video does not belong to actor_creator_id",
        )
    if isinstance(exc, InvalidVideoStateTransitionError):
        _raise_api_error(
            status_code=409,
            code="INVALID_VIDEO_STATE_TRANSITION",
            message=f"Cannot apply requested transition from status '{exc.current_status}'",
        )
    raise exc


@router.post(
    "/{video_id}/delete-index",
    response_model=InternalVideoMutationResponse,
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
    },
)
def delete_video_index(
    video_id: int,
    payload: InternalVideoMutationRequest,
    request: Request,
    x_internal_api_key: str | None = Header(default=None, alias="X-Internal-Api-Key"),
) -> InternalVideoMutationResponse:
    _require_internal_api_key(x_internal_api_key)
    try:
        status = request.app.state.store.delete_video_index(video_id, payload.actor_creator_id)
    except Exception as exc:
        _translate_store_error(exc)
        raise
    return InternalVideoMutationResponse(video_id=video_id, status=status)


@router.post(
    "/{video_id}/request-reindex",
    response_model=InternalVideoMutationResponse,
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
    },
)
def request_video_reindex(
    video_id: int,
    payload: InternalVideoMutationRequest,
    request: Request,
    x_internal_api_key: str | None = Header(default=None, alias="X-Internal-Api-Key"),
) -> InternalVideoMutationResponse:
    _require_internal_api_key(x_internal_api_key)
    try:
        status = request.app.state.store.request_video_reindex(video_id, payload.actor_creator_id)
    except Exception as exc:
        _translate_store_error(exc)
        raise
    return InternalVideoMutationResponse(video_id=video_id, status=status)
