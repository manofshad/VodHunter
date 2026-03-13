from pipeline.embedder import Embedder


def build_ingest_state() -> dict[str, object]:
    return {"embedder": Embedder()}
