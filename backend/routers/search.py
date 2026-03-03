from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile

from backend.schemas import ErrorResponse, SearchResponse
from backend.services.remote_clip_downloader import DownloadError, InvalidTikTokUrlError
from backend.services.search_manager import SearchInputError

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
) -> SearchResponse:
    has_file = file is not None
    has_url = bool((tiktok_url or "").strip())
    if has_file == has_url:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_SEARCH_INPUT",
                "message": "Provide exactly one of file or tiktok_url",
            },
        )

    search_manager = request.app.state.search_manager

    try:
        if has_file:
            assert file is not None
            result = search_manager.search_upload(file)
        else:
            assert tiktok_url is not None
            result = search_manager.search_tiktok_url(tiktok_url)
        return SearchResponse.from_result(result)
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
