import secrets

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status

from backend import config
from backend.schemas import InternalVideoMutationRequest, InternalVideoMutationResponse


router = APIRouter(prefix="/internal", tags=["internal"])


def _require_internal_api_key(x_internal_api_key: str | None = Header(default=None)) -> None:
    expected_api_key = config.SEARCH_INTERNAL_API_KEY
    if not expected_api_key:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal API key is not configured",
        )
    if not x_internal_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing internal API key",
        )
    if not secrets.compare_digest(str(x_internal_api_key), expected_api_key):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid internal API key",
        )


@router.post("/videos/{video_id}/delete-index", response_model=InternalVideoMutationResponse)
def delete_video_index(
    video_id: int,
    payload: InternalVideoMutationRequest,
    request: Request,
    _: None = Depends(_require_internal_api_key),
) -> InternalVideoMutationResponse:
    deleted = request.app.state.store.delete_video_index(
        video_id=int(video_id),
        actor_creator_id=int(payload.actor_creator_id),
    )
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video not found")
    return InternalVideoMutationResponse(video_id=int(video_id), status="deleted")


@router.post("/videos/{video_id}/request-reindex", response_model=InternalVideoMutationResponse)
def request_video_reindex(
    video_id: int,
    payload: InternalVideoMutationRequest,
    request: Request,
    _: None = Depends(_require_internal_api_key),
) -> InternalVideoMutationResponse:
    reindex_requested = request.app.state.store.request_video_reindex(
        video_id=int(video_id),
        actor_creator_id=int(payload.actor_creator_id),
    )
    if not reindex_requested:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video not found")
    return InternalVideoMutationResponse(video_id=int(video_id), status="reindex_requested")
