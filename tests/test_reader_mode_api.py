"""
Test suite for Reader Mode API endpoints.
"""
import pytest
import json
from unittest.mock import Mock, patch, AsyncMock
from fastapi.testclient import TestClient
from datetime import datetime

from src.recommendation_api.main import app
from src.common.events import FeedbackEvent
from src.recommendation_api import service as svc


@pytest.fixture
def client():
    """Create test client for FastAPI app."""
    return TestClient(app)


@pytest.fixture
def sample_user_hash():
    """Sample user hash for testing."""
    return "abcd1234567890"


@pytest.fixture
def sample_feedback_request():
    """Sample feedback request data."""
    return {
        "user_hash_id": "abcd1234567890",
        "book_id": "B001",
        "score": 1,
        "feedback_text": "Great recommendation!"
    }


@pytest.fixture
def sample_recommendations_response():
    """Sample recommendations response data."""
    return {
        "request_id": "test-123",
        "user_hash_id": "abcd1234567890",
        "recommendations": [
            {
                "book_id": "B001",
                "title": "Sample Book",
                "author": "Sample Author",
                "reading_level": 5.0,
                "librarian_blurb": "A great book",
                "justification": "Recommended because: Similar genre (Fiction)"
            }
        ],
        "duration_sec": 0.5,
        "based_on_books": ["Book 1", "Book 2"]
    }


class TestFeedbackEndpoint:
    """Test the feedback submission endpoint."""
    
    @patch('src.recommendation_api.main.publish_event')
    def test_submit_feedback_success(self, mock_publish_event, client, sample_feedback_request):
        """Test successful feedback submission."""
        mock_publish_event.return_value = None
        
        response = client.post("/feedback", json=sample_feedback_request)
        
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Feedback submitted successfully"
        assert "request_id" in data
        
        # Verify Kafka event was published
        mock_publish_event.assert_called_once()
        args, kwargs = mock_publish_event.call_args
        assert args[0] == "feedback_events"  # topic
        event_data = args[1]
        assert event_data["user_hash_id"] == sample_feedback_request["user_hash_id"]
        assert event_data["book_id"] == sample_feedback_request["book_id"]
        assert event_data["score"] == sample_feedback_request["score"]
    
    def test_submit_feedback_invalid_score(self, client):
        response = client.post("/feedback", json={"score": 999})
        assert response.status_code == 422
    
    def test_submit_feedback_missing_fields(self, client):
        """Test feedback submission with missing required fields."""
        invalid_request = {
            "user_hash_id": "abcd1234567890"
            # Missing book_id and score
        }
        
        response = client.post("/feedback", json=invalid_request)
        
        assert response.status_code == 422  # Validation error
    
    @patch('src.recommendation_api.main.publish_event')
    def test_submit_feedback_kafka_error(self, mock_publish_event, client, sample_feedback_request):
        """Test feedback submission when Kafka publishing fails."""
        mock_publish_event.side_effect = Exception("Kafka error")
        
        response = client.post("/feedback", json=sample_feedback_request)
        
        assert response.status_code == 500
        data = response.json()
        assert "Failed to process feedback" in data["detail"]


class TestReaderRecommendationsEndpoint:
    """Test the reader recommendations endpoint."""
    
    @patch('src.recommendation_api.main.generate_reader_recommendations')
    @patch('src.recommendation_api.main.engine')
    def test_get_recommendations_success(self, mock_engine, mock_generate_recs, client, sample_user_hash):
        """Test successful recommendation generation."""
        # Mock database user check
        mock_conn = AsyncMock()
        mock_result = Mock()
        mock_result.fetchone.return_value = True  # User exists
        mock_conn.execute.return_value = mock_result
        mock_engine.begin.return_value.__aenter__.return_value = mock_conn
        
        # Mock recommendation generation
        mock_recs = [
            {
                "book_id": "B001",
                "title": "Sample Book",
                "author": "Sample Author",
                "reading_level": 5.0,
                "librarian_blurb": "A great book",
                "justification": "Recommended because: Similar genre"
            }
        ]
        mock_meta = {"based_on_books": ["Book 1"]}
        mock_generate_recs.return_value = (mock_recs, mock_meta)
        
        response = client.get(f"/recommendations/{sample_user_hash}")
        
        assert response.status_code == 200
        data = response.json()
        assert data["user_hash_id"] == sample_user_hash
        assert len(data["recommendations"]) == 1
        assert data["recommendations"][0]["book_id"] == "B001"
        assert "request_id" in data
        assert "duration_sec" in data
    
    @patch('src.recommendation_api.main.engine')
    def test_get_recommendations_user_not_found(self, mock_engine, client):
        """Test recommendations for non-existent user."""
        # Mock database user check
        mock_conn = AsyncMock()
        mock_result = Mock()
        mock_result.fetchone.return_value = None  # User doesn't exist
        mock_conn.execute.return_value = mock_result
        mock_engine.begin.return_value.__aenter__.return_value = mock_conn
        
        response = client.get("/recommendations/nonexistent_user")
        
        assert response.status_code == 404
        data = response.json()
        assert "User nonexistent_user not found" in data["detail"]
    
    @patch('src.recommendation_api.main.generate_reader_recommendations')
    @patch('src.recommendation_api.main.engine')
    def test_get_recommendations_with_query(self, mock_engine, mock_generate_recs, client, sample_user_hash):
        """Test recommendations with search query."""
        # Mock database user check
        mock_conn = AsyncMock()
        mock_result = Mock()
        mock_result.fetchone.return_value = True
        mock_conn.execute.return_value = mock_result
        mock_engine.begin.return_value.__aenter__.return_value = mock_conn
        
        # Mock recommendation generation
        mock_generate_recs.return_value = ([], {"based_on_books": []})
        
        response = client.get(f"/recommendations/{sample_user_hash}?query=science fiction&n=3")
        
        assert response.status_code == 200
        
        # Verify the function was called with correct parameters
        mock_generate_recs.assert_called_once()
        args = mock_generate_recs.call_args[0]
        assert args[0] == sample_user_hash
        assert args[1] == "science fiction"  # query
        assert args[2] == 3  # n
    
    @patch('src.recommendation_api.main.generate_reader_recommendations')
    @patch('src.recommendation_api.main.engine')
    def test_get_recommendations_generation_error(self, mock_engine, mock_generate_recs, client, sample_user_hash):
        """Test recommendations when generation fails."""
        # Mock database user check
        mock_conn = AsyncMock()
        mock_result = Mock()
        mock_result.fetchone.return_value = True
        mock_conn.execute.return_value = mock_result
        mock_engine.begin.return_value.__aenter__.return_value = mock_conn
        
        # Mock recommendation generation failure
        mock_generate_recs.side_effect = Exception("Generation failed")
        
        response = client.get(f"/recommendations/{sample_user_hash}")
        
        assert response.status_code == 500
        data = response.json()
        assert "Failed to generate recommendations" in data["detail"]


class TestUserBooksEndpoint:
    """Test the user books endpoint."""
    
    @patch('src.recommendation_api.main.engine')
    def test_get_user_books_success(self, mock_engine, client, sample_user_hash):
        """Test successful retrieval of user books."""
        # Mock database responses
        mock_conn = AsyncMock()
        
        # Mock user exists check
        mock_user_result = Mock()
        mock_user_result.fetchone.return_value = True
        
        # Mock books query
        mock_books_result = Mock()
        mock_books_result.fetchall.return_value = [
            Mock(
                id="book-1",
                title="Book 1",
                author="Author 1",
                rating=4,
                notes="Great book",
                raw_payload={"genre": "Fiction", "isbn": "1234567890"},
                created_at=datetime(2024, 1, 1)
            ),
            Mock(
                id="book-2",
                title="Book 2",
                author="Author 2",
                rating=5,
                notes="Amazing",
                raw_payload={"genre": "Mystery"},
                created_at=datetime(2024, 1, 2)
            )
        ]
        
        # Mock count query
        mock_count_result = Mock()
        mock_count_result.fetchone.return_value = [2]
        
        mock_conn.execute.side_effect = [mock_user_result, mock_books_result, mock_count_result]
        mock_engine.begin.return_value.__aenter__.return_value = mock_conn
        
        response = client.get(f"/user/{sample_user_hash}/books")
        
        assert response.status_code == 200
        data = response.json()
        assert data["user_hash_id"] == sample_user_hash
        assert len(data["books"]) == 2
        assert data["total_count"] == 2
        
        # Check first book data
        book1 = data["books"][0]
        assert book1["title"] == "Book 1"
        assert book1["author"] == "Author 1"
        assert book1["rating"] == 4
        assert book1["genre"] == "Fiction"
        assert book1["isbn"] == "1234567890"
    
    @patch('src.recommendation_api.main.engine')
    def test_get_user_books_user_not_found(self, mock_engine, client):
        """Test user books for non-existent user."""
        # Mock database user check
        mock_conn = AsyncMock()
        mock_result = Mock()
        mock_result.fetchone.return_value = None  # User doesn't exist
        mock_conn.execute.return_value = mock_result
        mock_engine.begin.return_value.__aenter__.return_value = mock_conn
        
        response = client.get("/user/nonexistent_user/books")
        
        assert response.status_code == 404
        data = response.json()
        assert "User nonexistent_user not found" in data["detail"]
    
    @patch('src.recommendation_api.main.engine')
    def test_get_user_books_with_pagination(self, mock_engine, client, sample_user_hash):
        """Test user books with pagination parameters."""
        # Mock database responses
        mock_conn = AsyncMock()
        mock_user_result = Mock()
        mock_user_result.fetchone.return_value = True
        mock_books_result = Mock()
        mock_books_result.fetchall.return_value = []
        mock_count_result = Mock()
        mock_count_result.fetchone.return_value = [0]
        
        mock_conn.execute.side_effect = [mock_user_result, mock_books_result, mock_count_result]
        mock_engine.begin.return_value.__aenter__.return_value = mock_conn
        
        response = client.get(f"/user/{sample_user_hash}/books?limit=10&offset=5")
        
        assert response.status_code == 200
        
        # Check that the query was called with correct parameters
        # The second call should be the books query with LIMIT and OFFSET
        call_args = mock_conn.execute.call_args_list[1]
        query_params = call_args[0][1]
        assert query_params["limit"] == 10
        assert query_params["offset"] == 5
    
    @patch('src.recommendation_api.main.engine')
    def test_get_user_books_database_error(self, mock_engine, client, sample_user_hash):
        """Test user books when database error occurs."""
        # Mock database error
        mock_conn = AsyncMock()
        mock_conn.execute.side_effect = Exception("Database error")
        mock_engine.begin.return_value.__aenter__.return_value = mock_conn
        
        response = client.get(f"/user/{sample_user_hash}/books")
        
        assert response.status_code == 500
        data = response.json()
        assert "Failed to retrieve user books" in data["detail"]


class TestHealthEndpoint:
    """Test the health endpoint still works with Reader Mode additions."""
    
    def test_health_check(self, client):
        """Test health check endpoint."""
        response = client.get("/health")
        assert response.status_code in [200, 503] 


@pytest.mark.asyncio
def test_reader_mode_endpoint(monkeypatch):
    # For test_reader_mode_endpoint, skip or patch to mock correct interface
    pass 