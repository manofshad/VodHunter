from fastapi import APIRouter
from fastapi.responses import Response

from backend.observability import metrics_response


router = APIRouter(prefix="/internal", tags=["internal"])


@router.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    payload, content_type = metrics_response()
    return Response(content=payload, media_type=content_type)

