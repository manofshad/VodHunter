from fastapi import APIRouter, Form, Header, HTTPException, Request, Response, status

from backend.search_date_range import parse_search_date_range
from backend.schemas import (
    ErrorResponse,
    SearchJobCreatedResponse,
    SearchJobError,
    SearchJobResponse,
    SearchResponse,
    StreamerListItem,
)
from backend.services.remote_clip_downloader import InvalidTikTokUrlError, validate_tiktok_url
from search.access import is_search_token

router = APIRouter(prefix="/api", tags=["search"])
STREAMER_LIST_CACHE_CONTROL = "public, max-age=300, stale-while-revalidate=3600"
SEARCH_ACCESS_HEADERS = {"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"}


def _normalize_and_validate_streamer(request: Request, streamer: str | None) -> str:
    normalized_streamer = (streamer or "").strip().lower()
    if not normalized_streamer:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_STREAMER",
                "message": "streamer is required",
            },
        )

    searchable_streamers = request.app.state.videos.list_searchable_streamers()
    searchable_streamer_names = {item.name.strip().lower() for item in searchable_streamers}
    if normalized_streamer not in searchable_streamer_names:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_STREAMER",
                "message": f"Streamer is not searchable: {normalized_streamer}",
            },
        )

    return normalized_streamer


def _resolve_creator_id(request: Request, streamer: str | None) -> int | None:
    normalized_streamer = (streamer or "").strip().lower()
    if not normalized_streamer:
        return None
    return request.app.state.videos.get_creator_id_by_name(normalized_streamer)


@router.post(
    "/search/clip",
    response_model=SearchJobCreatedResponse,
    responses={400: {"model": ErrorResponse}},
    status_code=status.HTTP_202_ACCEPTED,
)
def create_search_clip_job(
    request: Request,
    response: Response,
    tiktok_url: str | None = Form(default=None),
    streamer: str | None = Form(default=None),
    streamed_from: str | None = Form(default=None),
    streamed_to: str | None = Form(default=None),
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

    try:
        normalized_tiktok_url = validate_tiktok_url(str(tiktok_url))
    except InvalidTikTokUrlError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_TIKTOK_URL",
                "message": str(exc),
            },
        ) from exc

    normalized_streamer = _normalize_and_validate_streamer(request, streamer)
    creator_id = _resolve_creator_id(request, normalized_streamer)
    date_range = parse_search_date_range(streamed_from, streamed_to)
    search_token = request.app.state.search_job_service.create_public_search_job(
        tiktok_url=normalized_tiktok_url,
        streamer=normalized_streamer,
        creator_id=creator_id,
        date_range=date_range,
    )
    response.headers.update(SEARCH_ACCESS_HEADERS)
    return SearchJobCreatedResponse(search_token=search_token, status="queued", stage="validating")


@router.get(
    "/search/clip",
    response_model=SearchJobResponse,
    responses={404: {"model": ErrorResponse}},
)
def get_search_clip_job(
    request: Request,
    response: Response,
    authorization: str | None = Header(default=None),
) -> SearchJobResponse:
    scheme, _, search_token = (authorization or "").partition(" ")
    job = None
    if scheme.lower() == "bearer" and is_search_token(search_token):
        job = request.app.state.search_job_service.get_public_search_job(search_token)
    if job is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "SEARCH_NOT_FOUND",
                "message": "Search job was not found",
            },
            headers=SEARCH_ACCESS_HEADERS,
        )

    response.headers.update(SEARCH_ACCESS_HEADERS)
    return SearchJobResponse(
        search_token=search_token,
        status=job.status,
        stage=job.stage,
        tiktok_url=job.tiktok_url,
        streamer=job.streamer,
        created_at=job.created_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        result=SearchResponse.from_result(job.result) if job.result is not None else None,
        error=SearchJobError(code=job.error_code, message=job.error_message)
        if job.error_code and job.error_message
        else None,
    )


@router.get("/search/streamers", response_model=list[StreamerListItem])
def list_searchable_streamers(request: Request, response: Response) -> list[StreamerListItem]:
    response.headers["Cache-Control"] = STREAMER_LIST_CACHE_CONTROL
    streamers = request.app.state.videos.list_searchable_streamers()
    return [
        StreamerListItem(
            name=item.name,
            profile_image_url=item.profile_image_url,
        )
        for item in streamers
    ]
