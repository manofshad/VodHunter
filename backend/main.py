"""Compatibility ASGI entrypoint for the public API."""

from backend.apps.public import app, create_public_app

__all__ = ["app", "create_public_app"]
