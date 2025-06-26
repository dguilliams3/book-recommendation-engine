import sys
from pathlib import Path
from common.testing import seed_random

# Ensure the project's `src/` directory is on sys.path so test modules
# can import `common`, `recommendation_api`, etc. without installing the
# package first.
root_dir = Path(__file__).resolve().parents[1]
src_dir = root_dir / "src"
if src_dir.exists() and str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

seed_random(123)

# -----------------------------------------------------------------------------
# Integration-test helpers (Postgres Testcontainer, shared event loop)
# -----------------------------------------------------------------------------

import asyncio, os

import pytest


@pytest.fixture(scope="session")
def event_loop():
    """Create a session-scoped event loop so containers stay alive across tests."""

    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def pg_container(event_loop):
    """Spin up a pgvector-enabled Postgres container for tests that need a DB."""

    try:
        from testcontainers.postgres import PostgresContainer
    except ImportError as exc:
        pytest.skip("testcontainers not installed", allow_module_level=True)

    image = os.getenv("PGVECTOR_IMAGE", "ankane/pgvector:latest")
    container = PostgresContainer(image)
    container.start()
    try:
        yield container
    finally:
        container.stop() 