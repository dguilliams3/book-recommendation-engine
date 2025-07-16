"""
Shared utilities for the Book‑Recommendation Platform.
Import surface: `from common import settings, models`.
For settings, always import directly from `common.settings` for reliability.
"""

from .settings import settings
from . import models

__all__ = [
    "settings",
    "models",
]
