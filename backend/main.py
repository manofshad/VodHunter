from contextlib import asynccontextmanager
import os
import sys
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, Query, Request, Response, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

load_dotenv(Path(ROOT_DIR) / ".env")

from backend import config
from backend.schemas import (
    ErrorResponse,
    LiveSessionItem,
    LiveStartRequest,
    LiveStartResponse,
    LiveStatusResponse,
    LiveStopResponse,
    SearchResponse,
)
from backend.services.eventsub_handler import EventSubAuthError, EventSubHandler
from backend.services.monitor_manager import MonitorConflictError, MonitorManager
from backend.services.remote_clip_downloader import DownloadError, InvalidTikTokUrlError, RemoteClipDownloader
from backend.services.search_manager import SearchInputError, SearchManager
from backend.services.session_query import SessionQueryService
from pipeline.embedder import Embedder
from search.alignment_service import AlignmentConfig, AlignmentService
from search.query_embedder import QueryEmbedder
from search.query_preprocessor import QueryPreprocessor
from search.search_service import SearchService
from search.vector_matcher import VectorMatcher
from services.twitch_eventsub import EventSubClient
from services.twitch_monitor import TwitchMonitor
from storage.vector_store import VectorStore


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(config.DATA_DIR, exist_ok=True)
    os.makedirs(config.TEMP_SEARCH_UPLOAD_DIR, exist_ok=True)
    os.makedirs(config.TEMP_SEARCH_DOWNLOAD_DIR, exist_ok=True)

    config.validate_storage_config()

    store = VectorStore(
        database_url=config.DATABASE_URL,
        vector_dim=config.VECTOR_DIM,
        pgvector_probes=config.PGVECTOR_PROBES,
    )
    store.init_db()

    embedder = Embedder()

    search_service = SearchService(
        store=store,
        preprocessor=QueryPreprocessor(temp_dir=config.TEMP_SEARCH_PREPROCESS_DIR),
        query_embedder=QueryEmbedder(embedder=embedder),
        matcher=VectorMatcher(top_k=10),
        alignment=AlignmentService(
            store=store,
            config=AlignmentConfig(min_vote_count=3, min_vote_ratio=0.08),
        ),
    )

    monitor_manager = MonitorManager(
        store=store,
        embedder=embedder,
        chunk_seconds=config.INGEST_CHUNK_SECONDS,
        monitor_poll_seconds=config.MONITOR_POLL_SECONDS,
        session_poll_interval=config.SESSION_POLL_INTERVAL,
        monitor_retry_seconds=config.MONITOR_RETRY_SECONDS,
        temp_dir=config.TEMP_LIVE_DIR,
        archive_lag_seconds=config.LIVE_ARCHIVE_LAG_SECONDS,
        archive_poll_seconds=config.LIVE_ARCHIVE_POLL_SECONDS,
        archive_finalize_checks=config.LIVE_ARCHIVE_FINALIZE_CHECKS,
        eventsub_client=EventSubClient(TwitchMonitor.from_env()),
        eventsub_callback_url=config.TWITCH_EVENTSUB_CALLBACK_URL,
        eventsub_secret=config.TWITCH_EVENTSUB_SECRET,
        eventsub_reconcile_seconds=config.EVENTSUB_RECONCILE_SECONDS,
        eventsub_fallback_poll_seconds=config.EVENTSUB_FALLBACK_POLL_SECONDS,
    )
    eventsub_handler = EventSubHandler(
        monitor_manager=monitor_manager,
        secret=config.TWITCH_EVENTSUB_SECRET,
        message_ttl_seconds=config.EVENTSUB_MESSAGE_TTL_SECONDS,
        max_clock_skew_seconds=config.EVENTSUB_MAX_CLOCK_SKEW_SECONDS,
    )

    search_manager = SearchManager(
        search_service=search_service,
        upload_temp_dir=config.TEMP_SEARCH_UPLOAD_DIR,
        remote_downloader=RemoteClipDownloader(
            temp_dir=config.TEMP_SEARCH_DOWNLOAD_DIR,
            timeout_seconds=config.TIKTOK_DOWNLOAD_TIMEOUT_SECONDS,
            max_file_mb=config.TIKTOK_MAX_FILE_MB,
        ),
    )

    session_query = SessionQueryService(store=store)

    app.state.store = store
    app.state.embedder = embedder
    app.state.monitor_manager = monitor_manager
    app.state.eventsub_handler = eventsub_handler
    app.state.search_manager = search_manager
    app.state.session_query = session_query

    try:
        yield
    finally:
        monitor_manager.stop()


app = FastAPI(title="VodHunter API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"^https?://(localhost|127\.0\.0\.1|[0-9]{1,3}(?:\.[0-9]{1,3}){3})(:[0-9]+)?$",
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health() -> dict[str, bool]:
    return {"ok": True}


@app.get("/api/live/status", response_model=LiveStatusResponse)
def get_live_status() -> LiveStatusResponse:
    status = app.state.monitor_manager.get_status()
    return LiveStatusResponse(**status.__dict__)


@app.post(
    "/api/live/start",
    response_model=LiveStartResponse,
    responses={409: {"model": ErrorResponse}},
)
def start_live_monitor(payload: LiveStartRequest) -> LiveStartResponse:
    try:
        status = app.state.monitor_manager.start(payload.streamer)
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


@app.post("/api/live/stop", response_model=LiveStopResponse)
def stop_live_monitor() -> LiveStopResponse:
    stopped = app.state.monitor_manager.stop()
    status = app.state.monitor_manager.get_status()
    return LiveStopResponse(stopped=stopped, status=LiveStatusResponse(**status.__dict__))


@app.post("/api/twitch/eventsub")
async def handle_twitch_eventsub(request: Request) -> Response:
    raw_body = await request.body()
    headers = {k.lower(): v for k, v in request.headers.items()}
    try:
        result = app.state.eventsub_handler.process(headers=headers, raw_body=raw_body)
    except EventSubAuthError as exc:
        app.state.monitor_manager.mark_eventsub_degraded(str(exc))
        raise HTTPException(
            status_code=403,
            detail={"code": "EVENTSUB_AUTH_FAILED", "message": str(exc)},
        ) from exc
    except Exception as exc:
        app.state.monitor_manager.mark_eventsub_degraded(f"EventSub handler error: {exc}")
        raise HTTPException(
            status_code=400,
            detail={"code": "EVENTSUB_HANDLER_ERROR", "message": str(exc)},
        ) from exc

    return Response(
        status_code=result.status_code,
        content=result.body,
        media_type=result.media_type,
    )


@app.get("/api/live/sessions", response_model=list[LiveSessionItem])
def list_live_sessions(
    limit: int = Query(default=config.LIVE_SESSIONS_DEFAULT_LIMIT, ge=1, le=config.LIVE_SESSIONS_MAX_LIMIT),
    offset: int = Query(default=0, ge=0),
) -> list[LiveSessionItem]:
    rows = app.state.session_query.list_live_sessions(limit=limit, offset=offset)
    return [LiveSessionItem(**row) for row in rows]


@app.post(
    "/api/search/clip",
    response_model=SearchResponse,
    responses={400: {"model": ErrorResponse}},
)
def search_clip(
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

    try:
        if has_file:
            assert file is not None
            result = app.state.search_manager.search_upload(file)
        else:
            assert tiktok_url is not None
            result = app.state.search_manager.search_tiktok_url(tiktok_url)
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
