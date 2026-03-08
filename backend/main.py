"""Compatibility ASGI entrypoint.

Use backend.apps.public:app (public) or backend.apps.admin:app (admin) for new deployments.
"""

from backend.apps.public import app, create_public_app

__all__ = ["app", "create_public_app"]
