import asyncio, json

import pytest
from fastapi.testclient import TestClient
from langchain_core.messages import AIMessage

from recommendation_api import main as api_module
from recommendation_api.service import BookRecommendation


@pytest.fixture
def client(monkeypatch):
    """Return a TestClient with `generate_recommendations` patched to a stub."""

    async def _fake_generate(student_id, query, n, request_id):
        # Very small deterministic result set
        recs = [BookRecommendation(
            book_id="b42", 
            title="Test Book", 
            author="Test Author",
            reading_level=3.5,
            librarian_blurb="Enjoy!",
            justification="Great book for testing"
        )]
        meta = {"tool_count": 0}
        return recs, meta

    monkeypatch.setattr(api_module, "generate_recommendations", _fake_generate)

    # FastAPI TestClient can work with async apps transparently
    return TestClient(api_module.app)


def test_health_endpoint(client):
    res = client.get("/health")
    # Health check may return 503 if Redis is unavailable (expected in test environment)
    assert res.status_code in [200, 503]
    data = res.json()
    assert "status" in data
    assert "timestamp" in data
    assert "components" in data
    # Should have checks for database, redis, openai, vector_store
    assert "database" in data["components"]
    assert "redis" in data["components"]
    assert "openai" in data["components"]
    assert "vector_store" in data["components"]


def test_recommend_endpoint_success(client):
    payload = {"student_id": "s99", "n": 1, "query": "space"}
    res = client.post("/recommend", params=payload)
    assert res.status_code == 200
    data = res.json()

    assert data["recommendations"][0]["book_id"] == "b42"
    assert data["duration_sec"] >= 0
    assert data["request_id"] 