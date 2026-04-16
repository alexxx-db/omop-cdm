"""Shared helpers for the OMOP Explorer apps.

Both `src/app` (FastAPI) and `src/app_gradio` (Gradio) import from this package.
Keeps warehouse connection, query wrappers, and diagnostic probes in one place.
"""

from .config import Settings, reset, settings
from .exceptions import (
    OmopConnectionError,
    OmopError,
    OmopNotFound,
    OmopSqlError,
)

__all__ = [
    "Settings",
    "settings",
    "reset",
    "OmopError",
    "OmopNotFound",
    "OmopSqlError",
    "OmopConnectionError",
]
