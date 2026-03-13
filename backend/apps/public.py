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
from backend import bootstrap_shared
from backend.routers.health import router as health_router
from backend.routers.search import router as search_router


def _configure_cors(app: FastAPI) -> None:
    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=r"^https?://(localhost|127\.0\.0\.1|[0-9]{1,3}(?:\.[0-9]{1,3}){3})(:[0-9]+)?$",
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )


def create_public_app(enable_lifespan: bool = True) -> FastAPI:
    if enable_lifespan:

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            bootstrap_shared.prepare_runtime_dirs()

            common_state = bootstrap_shared.build_store_state()
            search_state = bootstrap_shared.build_search_stack(
                store=common_state["store"],
                max_duration_seconds=config.SEARCH_MAX_DURATION_SECONDS_PUBLIC,
            )

            for key, value in {**common_state, **search_state}.items():
                setattr(app.state, key, value)

            yield

        app = FastAPI(title="VodHunter Public API", lifespan=lifespan)
    else:
        app = FastAPI(title="VodHunter Public API")

    _configure_cors(app)
    app.include_router(health_router)
    app.include_router(search_router)
    return app


app = create_public_app()
