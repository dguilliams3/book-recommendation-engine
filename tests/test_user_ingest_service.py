"""
Test suite for User Ingest Service.
"""
import pytest
import json
from unittest.mock import Mock, patch, AsyncMock
from fastapi.testclient import TestClient
from io import BytesIO

from src.user_ingest_service.main import app, hash_user_identifier


@pytest.fixture
def client():
    """Create test client for FastAPI app."""
    return TestClient(app)


@pytest.fixture
def sample_books_json():
    """Sample JSON data for testing."""
    return [
        {
            "title": "The Great Gatsby",
            "author": "F. Scott Fitzgerald",
            "rating": 5,
            "notes": "Great classic novel"
        },
        {
            "title": "To Kill a Mockingbird",
            "author": "Harper Lee",
            "rating": 4,
            "notes": "Powerful story"
        }
    ]


def test_hash_user_identifier():
    """Test user identifier hashing function."""
    user_id = "test_user_123"
    hash1 = hash_user_identifier(user_id)
    hash2 = hash_user_identifier(user_id)
    
    # Same input should produce same hash
    assert hash1 == hash2
    
    # Different input should produce different hash
    hash3 = hash_user_identifier("different_user")
    assert hash1 != hash3
    
    # Hash should be proper length for SHA256
    assert len(hash1) == 64  # 256 bits = 64 hex characters


def test_health_check(client):
    """Test health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "service" in data


@patch('src.user_ingest_service.main.store_books')
@patch('src.user_ingest_service.main.upsert_user')
@patch('src.user_ingest_service.main.publish_event')
def test_upload_books_json_success(mock_publish, mock_upsert_user, mock_store_books, client, sample_books_json):
    """Test successful JSON upload."""
    # Mock database operations
    mock_upsert_user.return_value = "test-user-id"
    mock_store_books.return_value = ["book-id-1", "book-id-2"]
    
    # Mock Kafka publishing
    mock_publish.return_value = None
    
    response = client.post(
        "/upload_books",
        json={
            "user_identifier": "test_user_123",
            "books": sample_books_json
        }
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["message"] == "Books uploaded successfully"
    assert data["books_processed"] == 2
    assert "user_hash_id" in data


def test_upload_books_too_many_books(client):
    """Test upload with too many books (>100)."""
    # No mocking needed - this is a validation test
    
    # Create 101 books
    too_many_books = [
        {
            "title": f"Book {i}",
            "author": f"Author {i}",
            "rating": 3
        } for i in range(101)
    ]
    
    response = client.post(
        "/upload_books",
        json={
            "user_identifier": "test_user",
            "books": too_many_books
        }
    )
    
    assert response.status_code == 422  # Pydantic validation error
    error_detail = response.json()["detail"]
    assert len(error_detail) > 0  # Should have validation errors 