"""Compatibility bootstrap exports.

Prefer importing from backend.bootstrap_shared or backend.bootstrap_admin directly.
"""

from backend.bootstrap_admin import build_monitor_stack
from backend.bootstrap_ingest import build_ingest_state
from backend.bootstrap_shared import build_search_stack, build_store_state, prepare_runtime_dirs

__all__ = [
    "build_ingest_state",
    "build_monitor_stack",
    "build_search_stack",
    "build_store_state",
    "prepare_runtime_dirs",
]
