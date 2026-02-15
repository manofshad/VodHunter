from contextlib import asynccontextmanager
import os
import sys
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
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
from backend.services.monitor_manager import MonitorConflictError, MonitorManager
from backend.services.search_manager import SearchBusyError, SearchInputError, SearchManager
from backend.services.session_query import SessionQueryService
from pipeline.embedder import Embedder
from search.alignment_service import AlignmentConfig, AlignmentService
from search.query_embedder import QueryEmbedder
from search.query_preprocessor import QueryPreprocessor
from search.search_service import SearchService
from search.vector_matcher import VectorMatcher
from storage.vector_store import VectorStore


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(config.DATA_DIR, exist_ok=True)
    os.makedirs(config.TEMP_SEARCH_UPLOAD_DIR, exist_ok=True)

    store = VectorStore(
        db_path=config.DB_PATH,
        vector_file=config.VECTOR_FILE,
        id_file=config.ID_FILE,
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
    )

    search_manager = SearchManager(
        search_service=search_service,
        monitor_manager=monitor_manager,
        upload_temp_dir=config.TEMP_SEARCH_UPLOAD_DIR,
    )

    session_query = SessionQueryService(db_path=config.DB_PATH)

    app.state.store = store
    app.state.embedder = embedder
    app.state.monitor_manager = monitor_manager
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
    responses={400: {"model": ErrorResponse}, 409: {"model": ErrorResponse}},
)
def search_clip(file: UploadFile = File(...)) -> SearchResponse:
    try:
        result = app.state.search_manager.search_upload(file)
        return SearchResponse.from_result(result)
    except SearchBusyError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": "SEARCH_BLOCKED", "message": str(exc)},
        ) from exc
    except SearchInputError as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_UPLOAD", "message": str(exc)},
        ) from exc
    except RuntimeError as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": "PROCESSING_ERROR", "message": str(exc)},
        ) from exc
