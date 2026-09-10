from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import sys

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

load_dotenv(Path(ROOT_DIR) / ".env")

from backend import bootstrap_shared
from backend.routers.health import router as health_router
from backend.routers.internal_videos import router as internal_videos_router
from backend.routers.search import router as search_router
from backend.services.search_jobs import SearchJobService


PUBLIC_MAX_DURATION_SECONDS = 180


def _configure_cors(app: FastAPI) -> None:
    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=r"^https?://(localhost|127\.0\.0\.1|[0-9]{1,3}(?:\.[0-9]{1,3}){3})(:[0-9]+)?$",
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )


def create_public_app(
    enable_lifespan: bool = True,
    *,
    internal_api_key: str | None = None,
) -> FastAPI:
    configured_internal_api_key = (
        internal_api_key.strip()
        if internal_api_key is not None
        else os.getenv("INTERNAL_API_KEY", "").strip()
    )

    if enable_lifespan:

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            data_dir = Path(ROOT_DIR) / "data"
            repositories = bootstrap_shared.build_repositories(
                database_url=os.getenv("DATABASE_URL", "").strip()
            )
            search_stack = bootstrap_shared.build_search_stack(
                repositories=repositories,
                max_duration_seconds=PUBLIC_MAX_DURATION_SECONDS,
                download_temp_dir=str(data_dir / "temp_search_downloads"),
                preprocess_temp_dir=str(data_dir / "temp_search"),
            )
            search_job_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="public-search")
            search_job_service = SearchJobService(
                jobs=repositories.search_jobs,
                search_manager=search_stack.search_manager,
                executor=search_job_executor,
            )
            search_job_service.fail_incomplete_public_search_jobs()

            for key, value in {
                "repositories": repositories,
                "videos": repositories.videos,
                "fingerprints": repositories.fingerprints,
                "ingest_states": repositories.ingest_states,
                "search_jobs": repositories.search_jobs,
                "query_embedder": search_stack.query_embedder,
                "search_service": search_stack.search_service,
                "search_manager": search_stack.search_manager,
                "search_job_executor": search_job_executor,
                "search_job_service": search_job_service,
            }.items():
                setattr(app.state, key, value)

            try:
                yield
            finally:
                search_job_executor.shutdown(wait=True, cancel_futures=True)
                search_stack.query_embedder.close()

        app = FastAPI(title="VodHunter Public API", lifespan=lifespan)
    else:
        app = FastAPI(title="VodHunter Public API")

    app.state.internal_api_key = configured_internal_api_key
    _configure_cors(app)
    app.include_router(health_router)
    app.include_router(search_router)
    app.include_router(internal_videos_router)
    return app


app = create_public_app()
