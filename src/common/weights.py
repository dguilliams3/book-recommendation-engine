"""Runtime-configurable scalar weights for scoring.

The module loads a JSON file (path comes from Settings) and keeps it in
memory, hot-reloading every few seconds in a daemon thread so the service
can pick up tweaks without a restart.
"""

from __future__ import annotations

import json, threading, time
from pathlib import Path
from typing import Any, Dict

from .settings import settings as S

_DEFAULT_WEIGHTS: Dict[str, Any] = {
    "reading_match": 1.0,
    "recency_half_life_days": 30,
    "social_boost": 0.1,
    "staff_pick_bonus": 0.0,
    "cold_start_k": 20,
}

_path = Path(S.weights_path)
_mtime = 0.0
_weights = _DEFAULT_WEIGHTS.copy()


def _load_once() -> None:
    global _weights, _mtime
    try:
        if not _path.exists():
            _weights = _DEFAULT_WEIGHTS.copy()
            return
        m = _path.stat().st_mtime
        if m == _mtime:
            return
        _weights = {**_DEFAULT_WEIGHTS, **json.loads(_path.read_text())}
        _mtime = m
    except Exception:  # noqa: BLE001
        # keep previous weights on error
        pass


_load_once()


def get() -> Dict[str, Any]:
    """Return the current weight mapping (read-only)."""

    return _weights.copy()


# Hot-reload thread ---------------------------------------------------------


def _watch():
    while True:
        time.sleep(3)
        _load_once()


threading.Thread(target=_watch, daemon=True).start()
