from fastapi import APIRouter, Header, HTTPException, Request, Response, status
from starlette.concurrency import run_in_threadpool
from starlette.datastructures import UploadFile
from starlette.exceptions import HTTPException as StarletteHTTPException

from backend.search_date_range import parse_search_date_range
from backend.schemas import (
    ErrorResponse,
    SearchJobCreatedResponse,
    SearchJobError,
    SearchJobResponse,
    SearchFeedbackRequest,
    SearchFeedbackResponse,
    SearchResponse,
    StreamerListItem,
)
from backend.services.remote_clip_downloader import InvalidClipUrlError, validate_clip_url
from backend.services.uploaded_clip_store import UploadError
from search.access import is_search_token

router = APIRouter(prefix="/api", tags=["search"])
STREAMER_LIST_CACHE_CONTROL = "public, max-age=300, stale-while-revalidate=3600"
SEARCH_ACCESS_HEADERS = {"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"}
SEARCH_FORM_STRING_LIMITS = {"tiktok_url": 2048, "streamer": 100, "streamed_from": 40, "streamed_to": 40}
SEARCH_FORM_PROPERTIES = {
    name: {"type": "string", "maxLength": limit}
    for name, limit in SEARCH_FORM_STRING_LIMITS.items()
}


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
    openapi_extra={
        "requestBody": {
            "content": {
                "multipart/form-data": {"schema": {
                    "type": "object",
                    "properties": {**SEARCH_FORM_PROPERTIES, "clip_file": {"type": "string", "format": "binary"}},
                    "required": ["streamer"],
                    "description": "Provide exactly one tiktok_url or clip_file.",
                }},
                "application/x-www-form-urlencoded": {"schema": {
                    "type": "object", "properties": SEARCH_FORM_PROPERTIES,
                    "required": ["tiktok_url", "streamer"],
                }},
            },
        },
    },
)
async def create_search_clip_job(
    request: Request,
    response: Response,
) -> SearchJobCreatedResponse:
    try:
        form = await request.form(max_files=1, max_fields=len(SEARCH_FORM_STRING_LIMITS))
    except (StarletteHTTPException, ValueError) as exc:
        raise HTTPException(status_code=400, detail={"code": "INVALID_SEARCH_INPUT", "message": "Invalid search form."}) from exc
    try:
        for key, value in form.multi_items():
            if key not in {*SEARCH_FORM_STRING_LIMITS, "clip_file"} or len(form.getlist(key)) != 1:
                raise HTTPException(status_code=400, detail={"code": "INVALID_SEARCH_INPUT", "message": "Search form contains unknown or repeated fields."})
            if key == "clip_file":
                valid = isinstance(value, UploadFile)
            else:
                valid = isinstance(value, str) and len(value) <= SEARCH_FORM_STRING_LIMITS[key]
            if not valid:
                raise HTTPException(status_code=400, detail={"code": "INVALID_SEARCH_INPUT", "message": "Invalid search form field."})
        return await _submit_search_form(request, response, form)
    finally:
        await form.close()


async def _submit_search_form(request: Request, response: Response, form) -> SearchJobCreatedResponse:
    tiktok_url = form.get("tiktok_url")
    clip_file = form.get("clip_file")
    has_url = bool((tiktok_url or "").strip())
    if has_url == (clip_file is not None):
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_SEARCH_INPUT",
                "message": "Provide exactly one tiktok_url or clip_file.",
            },
        )

    if has_url:
        try:
            normalized_tiktok_url = validate_clip_url(str(tiktok_url))
        except InvalidClipUrlError as exc:
            raise HTTPException(status_code=400, detail={"code": "INVALID_TIKTOK_URL", "message": str(exc)}) from exc

    normalized_streamer = await run_in_threadpool(_normalize_and_validate_streamer, request, form.get("streamer"))
    creator_id = await run_in_threadpool(_resolve_creator_id, request, normalized_streamer)
    date_range = parse_search_date_range(form.get("streamed_from"), form.get("streamed_to"))
    if clip_file is not None:
        try:
            search_token = await request.app.state.search_job_service.create_uploaded_search_job(
                clip_file=clip_file, streamer=normalized_streamer, creator_id=creator_id, date_range=date_range,
            )
        except UploadError as exc:
            raise HTTPException(status_code=exc.status_code, detail={"code": exc.code, "message": str(exc)}) from exc
    else:
        search_token = await run_in_threadpool(
            request.app.state.search_job_service.create_public_search_job,
            tiktok_url=normalized_tiktok_url, streamer=normalized_streamer,
            creator_id=creator_id, date_range=date_range,
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
        input_type=job.input_type,
        clip_filename=job.clip_filename,
        streamer=job.streamer,
        created_at=job.created_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        feedback=SearchFeedbackResponse(rating=job.feedback.rating, updated_at=job.feedback.updated_at)
        if job.feedback is not None else None,
        result=SearchResponse.from_result(job.result) if job.result is not None else None,
        error=SearchJobError(code=job.error_code, message=job.error_message)
        if job.error_code and job.error_message
        else None,
    )


@router.put(
    "/search/clip/feedback",
    response_model=SearchFeedbackResponse,
    responses={404: {"model": ErrorResponse}},
)
def save_search_clip_feedback(
    feedback: SearchFeedbackRequest,
    request: Request,
    response: Response,
    authorization: str | None = Header(default=None),
) -> SearchFeedbackResponse:
    scheme, _, search_token = (authorization or "").partition(" ")
    saved = None
    if scheme.lower() == "bearer" and is_search_token(search_token):
        saved = request.app.state.search_job_service.save_public_search_feedback(search_token, feedback.rating)
    if saved is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "SEARCH_NOT_FOUND", "message": "Search job was not found"},
            headers=SEARCH_ACCESS_HEADERS,
        )
    response.headers.update(SEARCH_ACCESS_HEADERS)
    return SearchFeedbackResponse(rating=saved.rating, updated_at=saved.updated_at)


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
