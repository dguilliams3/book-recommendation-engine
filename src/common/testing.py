from __future__ import annotations

import os
import random
import uuid
from typing import Optional

try:
    import numpy as np
except ImportError:  # numpy is optional for this project
    np = None  # type: ignore

__all__ = ["seed_random"]


def seed_random(seed: int | None = None) -> int:
    """Seed Python, NumPy (if available), and UUID generation for determinism.

    If *seed* is ``None`` a new deterministic seed is derived from an
    environment variable ``TEST_RANDOM_SEED`` or falls back to *42*.

    Returns the seed used so callers can log or assert against it.
    """
    if seed is None:
        seed = int(os.getenv("TEST_RANDOM_SEED", "42"))

    random.seed(seed)

    if np is not None:
        np.random.seed(seed % (2**32 - 1))

    # There is no official UUID seeding in stdlib; monkey-patch uuid4 for tests
    rand = random.Random(seed)

    def _uuid4() -> uuid.UUID:  # type: ignore
        return uuid.UUID(int=rand.getrandbits(128))

    uuid.uuid4 = _uuid4  # type: ignore

    return seed 