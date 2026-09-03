from fastapi import APIRouter, Request

from pipeline.nmfp_inference import model_artifact_identity

router = APIRouter(prefix="/api", tags=["health"])


@router.get("/health")
def health(request: Request) -> dict[str, bool | int | str]:
    response: dict[str, bool | int | str] = {"ok": True}
    query_embedder = getattr(request.app.state, "query_embedder", None)
    if query_embedder is not None:
        loaded = bool(query_embedder.is_loaded)
        response.update(
            {
                "ok": loaded,
                "nmfp_loaded": loaded,
                "embedding_dim": int(query_embedder.embedding_dim),
                "model_version": str(query_embedder.model_version),
                "preprocessing_version": str(query_embedder.preprocessing_version),
                "artifact_identity": model_artifact_identity(),
            }
        )
    return response
