from fastapi import APIRouter, Form, HTTPException, Request, status

from backend.routers.admin_search import _normalize_and_validate_streamer, _resolve_creator_id
from backend.schemas import (
    ErrorResponse,
    SearchJobCreatedResponse,
    SearchJobError,
    SearchJobResponse,
    SearchResponse,
    StreamerListItem,
)

router = APIRouter(prefix="/api", tags=["search"])


@router.post(
    "/search/clip",
    response_model=SearchJobCreatedResponse,
    responses={400: {"model": ErrorResponse}},
    status_code=status.HTTP_202_ACCEPTED,
)
def create_search_clip_job(
    request: Request,
    tiktok_url: str | None = Form(default=None),
    streamer: str | None = Form(default=None),
) -> SearchJobCreatedResponse:
    has_url = bool((tiktok_url or "").strip())
    if not has_url:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_SEARCH_INPUT",
                "message": "tiktok_url is required",
            },
        )

    normalized_streamer = _normalize_and_validate_streamer(request, streamer)
    creator_id = _resolve_creator_id(request, normalized_streamer)
    search_id = request.app.state.search_job_service.create_public_search_job(
        tiktok_url=str(tiktok_url).strip(),
        streamer=normalized_streamer,
        creator_id=creator_id,
    )
    return SearchJobCreatedResponse(search_id=search_id, status="queued", stage="validating")


@router.get(
    "/search/clip/{search_id}",
    response_model=SearchJobResponse,
    responses={404: {"model": ErrorResponse}},
)
def get_search_clip_job(request: Request, search_id: int) -> SearchJobResponse:
    job = request.app.state.search_job_service.get_public_search_job(search_id)
    if job is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "SEARCH_NOT_FOUND",
                "message": "Search job was not found",
            },
        )

    return SearchJobResponse(
        search_id=job.id,
        status=job.status,
        stage=job.stage,
        created_at=job.created_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        result=SearchResponse.from_result(job.result) if job.result is not None else None,
        error=SearchJobError(code=job.error_code, message=job.error_message)
        if job.error_code and job.error_message
        else None,
    )


@router.get("/search/streamers", response_model=list[StreamerListItem])
def list_searchable_streamers(request: Request) -> list[StreamerListItem]:
    streamers = request.app.state.store.list_searchable_streamers()
    return [
        StreamerListItem(
            name=str(item["name"]),
            profile_image_url=str(item["profile_image_url"]) if item.get("profile_image_url") else None,
        )
        for item in streamers
    ]
