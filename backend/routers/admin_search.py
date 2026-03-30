from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile

from backend.routers.search_logging import (
    SEARCH_ROUTE,
    build_log_from_outcome,
    extract_download_host,
    infer_input_type,
    normalize_streamer_value,
    persist_search_log,
)
from backend.schemas import ErrorResponse, SearchResponse, StreamerListItem
from backend.services.remote_clip_downloader import DownloadError, InvalidTikTokUrlError
from backend.services.search_manager import InputDurationExceededError, SearchInputError
from search.models import SearchRequestLog

router = APIRouter(prefix="/api", tags=["search"])


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

    searchable_streamers = request.app.state.store.list_searchable_streamers()
    searchable_streamer_names = {str(item["name"]).strip().lower() for item in searchable_streamers}
    if normalized_streamer not in searchable_streamer_names:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_STREAMER",
                "message": f"Streamer is not searchable: {normalized_streamer}",
            },
        )

    return normalized_streamer


@router.post(
    "/search/clip",
    response_model=SearchResponse,
    responses={400: {"model": ErrorResponse}},
)
def search_clip(
    request: Request,
    file: UploadFile | None = File(default=None),
    tiktok_url: str | None = Form(default=None),
    streamer: str | None = Form(default=None),
) -> SearchResponse:
    has_file = file is not None
    has_url = bool((tiktok_url or "").strip())
    input_type = infer_input_type(has_file=has_file, has_url=has_url)
    normalized_streamer = normalize_streamer_value(streamer)
    if has_file == has_url:
        persist_search_log(
            request,
            SearchRequestLog(
                source_app="admin",
                route=SEARCH_ROUTE,
                input_type=input_type,
                streamer=normalized_streamer,
                success=False,
                http_status=400,
                error_code="INVALID_SEARCH_INPUT",
                error_message="Provide exactly one of file or tiktok_url",
                clip_filename=file.filename if file is not None else None,
                download_source="tiktok" if has_url else None,
                download_host=extract_download_host(tiktok_url),
            ),
        )
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_SEARCH_INPUT",
                "message": "Provide exactly one of file or tiktok_url",
            },
        )

    try:
        normalized_streamer = _normalize_and_validate_streamer(request, streamer)
    except HTTPException as exc:
        detail = exc.detail if isinstance(exc.detail, dict) else {}
        persist_search_log(
            request,
            SearchRequestLog(
                source_app="admin",
                route=SEARCH_ROUTE,
                input_type=input_type,
                streamer=normalized_streamer,
                success=False,
                http_status=exc.status_code,
                error_code=str(detail.get("code") or "HTTP_ERROR"),
                error_message=str(detail.get("message") or exc.detail),
                clip_filename=file.filename if file is not None else None,
                download_source="tiktok" if has_url else None,
                download_host=extract_download_host(tiktok_url),
            ),
        )
        raise
    search_manager = request.app.state.search_manager

    try:
        if has_file:
            assert file is not None
            outcome = search_manager.search_upload(file, normalized_streamer)
        else:
            assert tiktok_url is not None
            outcome = search_manager.search_tiktok_url(tiktok_url, normalized_streamer)
        persist_search_log(
            request,
            build_log_from_outcome(
                source_app="admin",
                streamer=normalized_streamer,
                outcome=outcome,
            ),
        )
        return SearchResponse.from_result(outcome.result)
    except InputDurationExceededError as exc:
        persist_search_log(
            request,
            SearchRequestLog(
                source_app="admin",
                route=SEARCH_ROUTE,
                input_type=input_type,
                streamer=normalized_streamer,
                success=False,
                http_status=400,
                error_code="INPUT_DURATION_EXCEEDED",
                error_message=str(exc),
                clip_filename=file.filename if file is not None else None,
                download_source="tiktok" if has_url else None,
                download_host=extract_download_host(tiktok_url),
                input_duration_seconds=exc.duration_seconds,
            ),
        )
        raise HTTPException(
            status_code=400,
            detail={"code": "INPUT_DURATION_EXCEEDED", "message": str(exc)},
        ) from exc
    except SearchInputError as exc:
        persist_search_log(
            request,
            SearchRequestLog(
                source_app="admin",
                route=SEARCH_ROUTE,
                input_type=input_type,
                streamer=normalized_streamer,
                success=False,
                http_status=400,
                error_code="INVALID_UPLOAD",
                error_message=str(exc),
                clip_filename=file.filename if file is not None else None,
                download_source="tiktok" if has_url else None,
                download_host=extract_download_host(tiktok_url),
            ),
        )
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_UPLOAD", "message": str(exc)},
        ) from exc
    except InvalidTikTokUrlError as exc:
        persist_search_log(
            request,
            SearchRequestLog(
                source_app="admin",
                route=SEARCH_ROUTE,
                input_type=input_type,
                streamer=normalized_streamer,
                success=False,
                http_status=400,
                error_code="INVALID_TIKTOK_URL",
                error_message=str(exc),
                clip_filename=file.filename if file is not None else None,
                download_source="tiktok" if has_url else None,
                download_host=extract_download_host(tiktok_url),
            ),
        )
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_TIKTOK_URL", "message": str(exc)},
        ) from exc
    except DownloadError as exc:
        persist_search_log(
            request,
            SearchRequestLog(
                source_app="admin",
                route=SEARCH_ROUTE,
                input_type=input_type,
                streamer=normalized_streamer,
                success=False,
                http_status=400,
                error_code="DOWNLOAD_ERROR",
                error_message=str(exc),
                clip_filename=file.filename if file is not None else None,
                download_source="tiktok" if has_url else None,
                download_host=extract_download_host(tiktok_url),
            ),
        )
        raise HTTPException(
            status_code=400,
            detail={"code": "DOWNLOAD_ERROR", "message": str(exc)},
        ) from exc
    except RuntimeError as exc:
        persist_search_log(
            request,
            SearchRequestLog(
                source_app="admin",
                route=SEARCH_ROUTE,
                input_type=input_type,
                streamer=normalized_streamer,
                success=False,
                http_status=400,
                error_code="PROCESSING_ERROR",
                error_message=str(exc),
                clip_filename=file.filename if file is not None else None,
                download_source="tiktok" if has_url else None,
                download_host=extract_download_host(tiktok_url),
            ),
        )
        raise HTTPException(
            status_code=400,
            detail={"code": "PROCESSING_ERROR", "message": str(exc)},
        ) from exc


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
