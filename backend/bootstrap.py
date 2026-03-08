"""Compatibility bootstrap exports.

Prefer importing from backend.bootstrap_shared or backend.bootstrap_admin directly.
"""

from backend.bootstrap_admin import build_monitor_stack
from backend.bootstrap_shared import build_common_state, build_search_stack, prepare_runtime_dirs

__all__ = [
    "build_common_state",
    "build_monitor_stack",
    "build_search_stack",
    "prepare_runtime_dirs",
]
