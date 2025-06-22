"""
Shared utilities for the Book‑Recommendation Platform.
Keeps import surface small: `from common import settings, models`.
"""
from .settings import Settings, SettingsInstance as S, settings
from . import models
__all__ = [
    "Settings",
    "S",
    "settings",
    "models",
] 