from __future__ import annotations


def normalize_database_url(url: str) -> str:
    normalized = (url or "").strip()
    if normalized.startswith("postgresql+psycopg://"):
        return "postgresql://" + normalized[len("postgresql+psycopg://") :]
    return normalized


def normalize_sqlalchemy_database_url(url: str) -> str:
    normalized = (url or "").strip()
    if normalized.startswith("postgresql://"):
        return "postgresql+psycopg://" + normalized[len("postgresql://") :]
    return normalized
