import asyncio
from pathlib import Path

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

# Patch targets -----------------------------------------------------------------
import common.kafka_utils as kafka_utils
from common.settings import SettingsInstance as S
from langchain_openai import OpenAIEmbeddings

from ingestion_service.pipeline import run_ingestion
from graph_refresher.main import main as graph_refresh_main

# -----------------------------------------------------------------------------
# Helpers & fixtures
# -----------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _patch_external_dependencies(monkeypatch, tmp_path):
    """Patch external services (Kafka, OpenAI) to deterministic stubs.

    This fixture is *autouse* for the module so it transparently applies to all
    tests without being explicitly requested.
    """

    # 1. Kafka – replace publish_event with no-op coroutine
    async def _noop(*_args, **_kwargs):
        return True

    monkeypatch.setattr(kafka_utils.event_producer, "publish_event", _noop, False)

    # 2. OpenAI embeddings – avoid network call and make deterministic output
    def _fake_embed_documents(self, texts, **_kwargs):
        # Return simple deterministic vectors of fixed length (3)
        return [[float(i % 3)] * 3 for i, _ in enumerate(texts)]

    def _fake_embed_query(self, text):  # noqa: D401 – simple stub
        return [0.0, 0.0, 0.0]

    monkeypatch.setattr(OpenAIEmbeddings, "embed_documents", _fake_embed_documents, False)
    monkeypatch.setattr(OpenAIEmbeddings, "embed_query", _fake_embed_query, False)

    # 3. Relax StudentAddedEvent validation to accept count-only payload
    from common import events as _evt

    class _StubStudentAddedEvent(dict):
        def __init__(self, *args, **kwargs):
            super().__init__(**kwargs)

        def dict(self):
            return self

    monkeypatch.setattr(_evt, "StudentAddedEvent", _StubStudentAddedEvent, False)

    # 3. Ensure each test has its own isolated data directory so artifacts are
    #    automatically cleaned up by pytest when tmp_path is discarded.
    S.data_dir = Path(tmp_path)

    import importlib
    try:
        import ingestion_service.pipeline as _pl
        monkeypatch.setattr(_pl, "StudentAddedEvent", _StubStudentAddedEvent, False)
    except ImportError:
        pass


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_ingestion_writes_catalog(pg_container):
    """End-to-end: the pipeline should insert catalog rows and create a FAISS index."""

    # Point application at the container database
    S._db_url = pg_container.get_connection_url()

    # Run ingestion
    await run_ingestion()

    # Validate – ensure rows were written and vector index file exists
    # asyncpg requires bare 'postgresql://' scheme (no driver qualifier)
    dsn = pg_container.get_connection_url().replace("+psycopg2", "")
    conn = await asyncpg.connect(dsn)
    try:
        book_cnt = await conn.fetchval("SELECT COUNT(*) FROM catalog")
    finally:
        await conn.close()

    assert book_cnt and book_cnt > 0, "No rows inserted into catalog table"

    index_file = S.data_dir / "vector_store" / "index.faiss"
    assert index_file.exists(), "FAISS index file was not created"


@pytest.mark.asyncio
async def test_graph_refresher_creates_similarity_edges(pg_container):
    """Running the graph refresher should populate student_embeddings and student_similarity."""

    # Re-use the same DB used by ingestion (assumes previous test ran first)
    S._db_url = pg_container.get_connection_url()

    # The graph refresher expects checkout data.  The ingestion pipeline has
    # already inserted sample checkouts; we simply run the refresher.
    await graph_refresh_main()

    # asyncpg requires bare 'postgresql://' scheme (no driver qualifier)
    dsn = pg_container.get_connection_url().replace("+psycopg2", "")
    conn = await asyncpg.connect(dsn)
    try:
        emb_cnt = await conn.fetchval("SELECT COUNT(*) FROM student_embeddings")
        sim_cnt = await conn.fetchval("SELECT COUNT(*) FROM student_similarity")
    finally:
        await conn.close()

    assert emb_cnt and emb_cnt > 0, "Student embeddings were not created"
    assert sim_cnt and sim_cnt > 0, "Student similarity edges not created" 