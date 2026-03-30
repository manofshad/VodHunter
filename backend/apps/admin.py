from contextlib import asynccontextmanager
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

load_dotenv(Path(ROOT_DIR) / ".env")

from backend import config
from backend import bootstrap_admin, bootstrap_ingest, bootstrap_shared
from backend.routers.admin_search import router as admin_search_router
from backend.routers.eventsub import router as eventsub_router
from backend.routers.health import router as health_router
from backend.routers.live_monitor import router as live_monitor_router


def _configure_cors(app: FastAPI) -> None:
    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=r"^https?://(localhost|127\.0\.0\.1|[0-9]{1,3}(?:\.[0-9]{1,3}){3})(:[0-9]+)?$",
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )


def create_admin_app(enable_lifespan: bool = True) -> FastAPI:
    if enable_lifespan:

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            bootstrap_shared.prepare_admin_runtime_dirs()

            common_state = bootstrap_shared.build_store_state()
            ingest_state = bootstrap_ingest.build_ingest_state()
            search_state = bootstrap_shared.build_search_stack(
                store=common_state["store"],
                max_duration_seconds=config.SEARCH_MAX_DURATION_SECONDS_ADMIN,
                upload_temp_dir=config.TEMP_SEARCH_UPLOAD_DIR,
            )
            monitor_state = bootstrap_admin.build_monitor_stack(
                store=common_state["store"],
                embedder=ingest_state["embedder"],
            )

            for key, value in {**common_state, **ingest_state, **search_state, **monitor_state}.items():
                setattr(app.state, key, value)

            try:
                yield
            finally:
                app.state.monitor_manager.stop()

        app = FastAPI(title="VodHunter Admin API", lifespan=lifespan)
    else:
        app = FastAPI(title="VodHunter Admin API")

    _configure_cors(app)
    app.include_router(health_router)
    app.include_router(admin_search_router)
    app.include_router(live_monitor_router)
    app.include_router(eventsub_router)
    return app


app = create_admin_app()
