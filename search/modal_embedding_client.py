from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError

from search.modal_types import ModalEmbeddingRequest, ModalEmbeddingResponse

logger = logging.getLogger("uvicorn.error")

try:
    import modal
except ImportError:  # pragma: no cover - exercised in config/bootstrap tests instead
    modal = None


class ModalEmbeddingError(RuntimeError):
    pass


class ModalEmbeddingClient:
    def __init__(
        self,
        app_name: str,
        function_name: str,
        timeout_seconds: float,
    ):
        if modal is None:
            raise ModalEmbeddingError("modal package is not installed")
        self.app_name = app_name
        self.function_name = function_name
        self.timeout_seconds = timeout_seconds
        self._function = None

    def _get_function(self):
        if self._function is None:
            self._function = modal.Function.from_name(self.app_name, self.function_name)
        return self._function

    def embed(self, request: ModalEmbeddingRequest) -> ModalEmbeddingResponse:
        started_at = time.perf_counter()

        def _invoke() -> dict:
            fn = self._get_function()
            return fn.remote(
                wav_bytes=request.wav_bytes,
                request_id=request.request_id,
                filename=request.filename,
                offset_seconds=request.offset_seconds,
                model_version=request.model_version,
            )

        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                raw = executor.submit(_invoke).result(timeout=self.timeout_seconds)
        except FutureTimeoutError as exc:
            raise ModalEmbeddingError(
                f"Modal embedding request timed out after {self.timeout_seconds:.1f}s"
            ) from exc
        except Exception as exc:
            raise ModalEmbeddingError(f"Modal embedding request failed: {exc}") from exc

        elapsed = time.perf_counter() - started_at
        logger.info(
            "timing event=modal_embed seconds=%.2f request_id=%s filename=%s",
            elapsed,
            request.request_id,
            request.filename,
        )
        return ModalEmbeddingResponse(
            embeddings=raw["embeddings"],
            timestamps=raw["timestamps"],
            model_name=raw["model_name"],
            embedding_dim=int(raw["embedding_dim"]),
            duration_seconds=raw.get("duration_seconds"),
        )
