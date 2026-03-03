"""Compatibility ASGI entrypoint.

Use backend.apps.admin:app (admin) or backend.apps.public:app (public) for new deployments.
"""

from backend.apps.admin import app, create_admin_app

__all__ = ["app", "create_admin_app"]
