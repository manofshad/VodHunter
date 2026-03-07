from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile

from backend.schemas import ErrorResponse, SearchResponse, StreamerListItem
from backend.services.remote_clip_downloader import DownloadError, InvalidTikTokUrlError
from backend.services.search_manager import InputDurationExceededError, SearchInputError

router = APIRouter(prefix="/api", tags=["search"])


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
    normalized_streamer = (streamer or "").strip().lower()
    if has_file == has_url:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_SEARCH_INPUT",
                "message": "Provide exactly one of file or tiktok_url",
            },
        )
    if not normalized_streamer:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_STREAMER",
                "message": "streamer is required",
            },
        )

    search_manager = request.app.state.search_manager
    searchable_streamers = request.app.state.store.list_searchable_streamers()
    searchable_streamer_names = {name.strip().lower() for name in searchable_streamers}
    if normalized_streamer not in searchable_streamer_names:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_STREAMER",
                "message": f"Streamer is not searchable: {normalized_streamer}",
            },
        )

    try:
        if has_file:
            assert file is not None
            result = search_manager.search_upload(file, normalized_streamer)
        else:
            assert tiktok_url is not None
            result = search_manager.search_tiktok_url(tiktok_url, normalized_streamer)
        return SearchResponse.from_result(result)
    except InputDurationExceededError as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": "INPUT_DURATION_EXCEEDED", "message": str(exc)},
        ) from exc
    except SearchInputError as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_UPLOAD", "message": str(exc)},
        ) from exc
    except InvalidTikTokUrlError as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_TIKTOK_URL", "message": str(exc)},
        ) from exc
    except DownloadError as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": "DOWNLOAD_ERROR", "message": str(exc)},
        ) from exc
    except RuntimeError as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": "PROCESSING_ERROR", "message": str(exc)},
        ) from exc


@router.get("/search/streamers", response_model=list[StreamerListItem])
def list_searchable_streamers(request: Request) -> list[StreamerListItem]:
    names = request.app.state.store.list_searchable_streamers()
    return [StreamerListItem(name=name) for name in names]
