"""
Tests for LLM microservice integration with the book recommendation system.

This test suite verifies:
1. LLM client functionality
2. Circuit breaker behavior
3. Fallback mechanisms
4. Reader recommendation enhancement
5. User data enrichment
6. Health monitoring
"""

import asyncio
import json
import uuid
from unittest.mock import AsyncMock, patch, MagicMock
from typing import Dict, Any, List

import pytest
import httpx
from pydantic import ValidationError

from common.llm_client import (
    LLMClient,
    LLMRequest,
    LLMResponse,
    CircuitBreaker,
    CircuitBreakerState,
    LLMServiceError,
    LLMServiceUnavailableError,
    LLMServiceTimeoutError,
    get_llm_client,
    invoke_llm,
    enrich_recommendations_with_llm,
    enrich_book_metadata
)
from recommendation_api.service import generate_reader_recommendations
from user_ingest_service.main import store_books


class TestLLMClient:
    """Test the LLM client functionality."""
    
    def test_llm_request_model(self):
        """Test LLM request model validation."""
        # Valid request
        request = LLMRequest(
            user_prompt="Test prompt",
            system_prompt="Test system prompt",
            model="gpt-4o-mini",
            temperature=0.7
        )
        assert request.user_prompt == "Test prompt"
        assert request.system_prompt == "Test system prompt"
        assert request.model == "gpt-4o-mini"
        assert request.temperature == 0.7
        assert request.request_id is not None
        
        # Request with defaults
        request = LLMRequest(user_prompt="Test")
        assert request.model == "gpt-4o-mini"
        assert request.temperature == 0.7
        assert request.max_tokens == 1000
    
    def test_llm_response_model(self):
        """Test LLM response model validation."""
        response = LLMResponse(
            success=True,
            request_id="test-id",
            data={"response": "Test response"},
            model="gpt-4o-mini",
            timestamp="2023-01-01T00:00:00Z"
        )
        assert response.success is True
        assert response.request_id == "test-id"
        assert response.data["response"] == "Test response"
        assert response.model == "gpt-4o-mini"


class TestCircuitBreaker:
    """Test circuit breaker functionality."""
    
    def test_circuit_breaker_initial_state(self):
        """Test circuit breaker starts in CLOSED state."""
        cb = CircuitBreaker()
        assert cb.state == CircuitBreakerState.CLOSED
        assert cb.can_execute() is True
        assert cb.failure_count == 0
    
    def test_circuit_breaker_failure_threshold(self):
        """Test circuit breaker opens after failure threshold."""
        cb = CircuitBreaker()
        
        # Record failures up to threshold
        for i in range(4):
            cb.record_failure()
            assert cb.state == CircuitBreakerState.CLOSED
            assert cb.can_execute() is True
        
        # One more failure should open the circuit
        cb.record_failure()
        assert cb.state == CircuitBreakerState.OPEN
        assert cb.can_execute() is False
    
    def test_circuit_breaker_recovery(self):
        """Test circuit breaker recovery after timeout."""
        cb = CircuitBreaker()
        
        # Open the circuit
        for i in range(5):
            cb.record_failure()
        
        assert cb.state == CircuitBreakerState.OPEN
        
        # Simulate timeout by setting last_failure_time to past
        from datetime import datetime, timedelta
        cb.last_failure_time = datetime.now() - timedelta(seconds=61)
        
        # Should move to HALF_OPEN
        assert cb.can_execute() is True
        assert cb.state == CircuitBreakerState.HALF_OPEN
        
        # Success should close the circuit
        cb.record_success()
        cb.record_success()
        cb.record_success()
        assert cb.state == CircuitBreakerState.CLOSED


class TestLLMClientIntegration:
    """Test LLM client integration with the microservice."""
    
    @pytest.fixture
    def mock_settings(self):
        """Mock settings for testing."""
        with patch('common.llm_client.settings') as mock_settings:
            mock_settings.llm_service_url = "http://test-llm:8000"
            mock_settings.llm_request_timeout = 30
            mock_settings.llm_max_retries = 3
            mock_settings.llm_fallback_enabled = True
            mock_settings.openai_api_key = "test-key"
            yield mock_settings
    
    @pytest.mark.asyncio
    async def test_successful_llm_call(self, mock_settings):
        """Test successful LLM service call."""
        client = LLMClient()
        
        # Mock successful HTTP response
        mock_response = {
            "success": True,
            "request_id": "test-id",
            "data": {"response": "Test response", "confidence": 0.9},
            "model": "gpt-4o-mini",
            "cached": False,
            "timestamp": "2023-01-01T00:00:00Z"
        }
        
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = mock_response
            mock_post.return_value.raise_for_status.return_value = None
            
            request = LLMRequest(user_prompt="Test prompt")
            response = await client.invoke(request)
            
            assert response.success is True
            assert response.data["response"] == "Test response"
            assert response.model == "gpt-4o-mini"
    
    @pytest.mark.asyncio
    async def test_llm_service_unavailable_with_fallback(self, mock_settings):
        """Test fallback when LLM service is unavailable."""
        client = LLMClient()
        
        # Mock service unavailable
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.side_effect = httpx.ConnectError("Connection failed")
            
            # Mock successful fallback
            with patch('common.llm_client.ChatOpenAI') as mock_openai:
                mock_result = MagicMock()
                mock_result.content = "Fallback response"
                mock_openai.return_value.ainvoke.return_value = mock_result
                
                request = LLMRequest(user_prompt="Test prompt")
                response = await client.invoke(request)
                
                assert response.success is True
                assert response.data["response"] == "Fallback response"
                assert client.fallback_count == 1
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_integration(self, mock_settings):
        """Test circuit breaker integration with LLM client."""
        client = LLMClient()
        
        # Simulate multiple failures to open circuit breaker
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.side_effect = httpx.ConnectError("Connection failed")
            
            # Mock fallback to avoid fallback calls
            with patch.object(client, '_fallback_openai') as mock_fallback:
                mock_fallback.side_effect = Exception("Fallback also failed")
                
                request = LLMRequest(user_prompt="Test prompt")
                
                # Multiple failures should open circuit breaker
                for i in range(5):
                    with pytest.raises(LLMServiceError):
                        await client.invoke(request)
                
                # Circuit breaker should be open
                assert client.circuit_breaker.state == CircuitBreakerState.OPEN
                
                # Next call should fail immediately without HTTP call
                mock_post.reset_mock()
                with pytest.raises(Exception):
                    await client.invoke(request)
                
                # HTTP call should not have been made
                mock_post.assert_not_called()


class TestRecommendationEnhancement:
    """Test recommendation enhancement with LLM."""
    
    @pytest.mark.asyncio
    async def test_enrich_recommendations_with_llm(self):
        """Test recommendation enrichment functionality."""
        recommendations = [
            {
                "book_id": "B001",
                "title": "The Great Gatsby",
                "author": "F. Scott Fitzgerald",
                "reading_level": 8.5,
                "genre": "Classic Fiction",
                "justification": "Basic justification"
            },
            {
                "book_id": "B002",
                "title": "To Kill a Mockingbird",
                "author": "Harper Lee",
                "reading_level": 7.5,
                "genre": "Classic Fiction",
                "justification": "Another basic justification"
            }
        ]
        
        user_context = {
            "user_hash_id": "test-user",
            "uploaded_books": [
                {"title": "1984", "author": "George Orwell"},
                {"title": "Brave New World", "author": "Aldous Huxley"}
            ]
        }
        
        # Mock LLM response
        mock_llm_response = LLMResponse(
            success=True,
            request_id="test-id",
            data={
                "response": """
                1. The Great Gatsby - This classic American novel explores themes of wealth, love, and the American Dream, similar to the dystopian themes in your reading history. The sophisticated prose and social commentary will appeal to readers of Orwell and Huxley.
                
                2. To Kill a Mockingbird - This powerful story of justice and moral courage complements your interest in thought-provoking literature. Like 1984 and Brave New World, it challenges readers to think critically about society and human nature.
                """
            },
            model="gpt-4o-mini",
            cached=False,
            timestamp="2023-01-01T00:00:00Z"
        )
        
        with patch('common.llm_client.invoke_llm') as mock_invoke:
            mock_invoke.return_value = mock_llm_response
            
            enhanced_recs = await enrich_recommendations_with_llm(
                recommendations, user_context, "classic literature", "test-request"
            )
            
            assert len(enhanced_recs) == 2
            assert enhanced_recs[0]["llm_enhanced"] is True
            assert enhanced_recs[1]["llm_enhanced"] is True
            assert "American Dream" in enhanced_recs[0]["justification"]
            assert "moral courage" in enhanced_recs[1]["justification"]
    
    @pytest.mark.asyncio
    async def test_enrich_book_metadata(self):
        """Test book metadata enrichment functionality."""
        book_data = {
            "title": "grate gatsby",  # Intentional typo
            "author": "f scott fitzgerald",  # Needs capitalization
            "genre": "",  # Missing
            "reading_level": ""  # Missing
        }
        
        # Mock LLM response
        mock_llm_response = LLMResponse(
            success=True,
            request_id="test-id",
            data={
                "response": """
                {
                    "title": "The Great Gatsby",
                    "author": "F. Scott Fitzgerald",
                    "reading_level": 8.5,
                    "genre": "Classic Fiction",
                    "isbn": "978-0-7432-7356-5",
                    "confidence": 0.95,
                    "notes": "Corrected title capitalization and author name"
                }
                """
            },
            model="gpt-4o-mini",
            cached=False,
            timestamp="2023-01-01T00:00:00Z"
        )
        
        with patch('common.llm_client.invoke_llm') as mock_invoke:
            mock_invoke.return_value = mock_llm_response
            
            enriched_data = await enrich_book_metadata(book_data, "test-request")
            
            assert enriched_data["title"] == "The Great Gatsby"
            assert enriched_data["author"] == "F. Scott Fitzgerald"
            assert enriched_data["reading_level"] == 8.5
            assert enriched_data["genre"] == "Classic Fiction"
            assert enriched_data["llm_enriched"] is True
            assert "enrichment_timestamp" in enriched_data


class TestReaderRecommendationIntegration:
    """Test reader recommendation integration with LLM enhancement."""
    
    @pytest.mark.asyncio
    async def test_generate_reader_recommendations_with_llm(self):
        """Test reader recommendation generation with LLM enhancement."""
        # Mock database operations
        with patch('recommendation_api.service.engine') as mock_engine:
            # Mock user uploaded books
            mock_uploaded_books = [
                {"title": "1984", "author": "George Orwell", "genre": "Dystopian"},
                {"title": "Brave New World", "author": "Aldous Huxley", "genre": "Dystopian"}
            ]
            
            # Mock similar books from catalog
            mock_similar_books = [
                {
                    "book_id": "B001",
                    "title": "Fahrenheit 451",
                    "author": "Ray Bradbury",
                    "reading_level": 8.0,
                    "genre": "Dystopian",
                    "librarian_blurb": "A dystopian novel about book burning"
                }
            ]
            
            # Mock database queries
            mock_conn = AsyncMock()
            mock_engine.begin.return_value.__aenter__.return_value = mock_conn
            
            # Mock the internal functions
            with patch('recommendation_api.service._fetch_user_uploaded_books') as mock_fetch_books:
                mock_fetch_books.return_value = mock_uploaded_books
                
                with patch('recommendation_api.service._fetch_user_feedback_scores') as mock_fetch_feedback:
                    mock_fetch_feedback.return_value = {}
                    
                    with patch('recommendation_api.service._fetch_similar_books') as mock_fetch_similar:
                        mock_fetch_similar.return_value = mock_similar_books
                        
                        with patch('recommendation_api.service._calculate_similarity_score') as mock_calc_score:
                            mock_calc_score.return_value = 0.85
                            
                            with patch('recommendation_api.service._create_justification') as mock_create_just:
                                mock_create_just.return_value = "Basic justification"
                                
                                # Mock LLM enhancement
                                with patch('common.llm_client.enrich_recommendations_with_llm') as mock_enrich:
                                    mock_enrich.return_value = [
                                        {
                                            "book_id": "B001",
                                            "title": "Fahrenheit 451",
                                            "author": "Ray Bradbury",
                                            "reading_level": 8.0,
                                            "justification": "Enhanced: This dystopian masterpiece perfectly complements your interest in Orwell and Huxley, exploring themes of censorship and intellectual freedom.",
                                            "llm_enhanced": True
                                        }
                                    ]
                                    
                                    # Mock settings
                                    with patch('recommendation_api.service.S') as mock_settings:
                                        mock_settings.llm_service_enabled = True
                                        
                                        # Test the function
                                        recommendations, metadata = await generate_reader_recommendations(
                                            "test-user", "dystopian novels", 1, "test-request"
                                        )
                                        
                                        assert len(recommendations) == 1
                                        assert recommendations[0].title == "Fahrenheit 451"
                                        assert "Enhanced:" in recommendations[0].justification
                                        assert metadata["llm_enhanced"] is True
                                        assert "llm_enhancement" in metadata["tools"]


class TestHealthMonitoring:
    """Test health monitoring functionality."""
    
    @pytest.mark.asyncio
    async def test_llm_client_health_check(self):
        """Test LLM client health check."""
        client = LLMClient()
        
        # Mock successful health check
        with patch('httpx.AsyncClient.get') as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = {"status": "healthy"}
            mock_get.return_value.raise_for_status.return_value = None
            
            health = await client.health_check()
            
            assert health["status"] == "healthy"
            assert health["circuit_breaker_state"] == "closed"
            assert "service_response" in health
    
    @pytest.mark.asyncio
    async def test_llm_client_unhealthy(self):
        """Test LLM client health check when service is unhealthy."""
        client = LLMClient()
        
        # Mock failed health check
        with patch('httpx.AsyncClient.get') as mock_get:
            mock_get.side_effect = httpx.ConnectError("Connection failed")
            
            health = await client.health_check()
            
            assert health["status"] == "unhealthy"
            assert "error" in health
            assert health["circuit_breaker_state"] == "closed"
    
    def test_llm_client_stats(self):
        """Test LLM client statistics."""
        client = LLMClient()
        
        # Simulate some activity
        client.request_count = 10
        client.success_count = 8
        client.failure_count = 2
        client.fallback_count = 1
        
        stats = client.get_stats()
        
        assert stats["request_count"] == 10
        assert stats["success_count"] == 8
        assert stats["failure_count"] == 2
        assert stats["fallback_count"] == 1
        assert stats["success_rate"] == 0.8
        assert stats["fallback_rate"] == 0.1
        assert stats["circuit_breaker_state"] == "closed"


class TestUserDataEnrichment:
    """Test user data enrichment in the ingest service."""
    
    @pytest.mark.asyncio
    async def test_store_books_with_enrichment(self):
        """Test book storage with LLM enrichment."""
        books_data = [
            {
                "title": "grate gatsby",
                "author": "f scott fitzgerald",
                "rating": 4,
                "notes": "good book"
            }
        ]
        
        # Mock enriched response
        enriched_data = {
            "title": "The Great Gatsby",
            "author": "F. Scott Fitzgerald",
            "rating": 4,
            "notes": "good book",
            "reading_level": 8.5,
            "genre": "Classic Fiction",
            "llm_enriched": True,
            "enrichment_timestamp": "2023-01-01T00:00:00"
        }
        
        # Mock database session
        with patch('user_ingest_service.main.async_session') as mock_session:
            mock_session_instance = AsyncMock()
            mock_session.return_value.__aenter__.return_value = mock_session_instance
            
            # Mock LLM enrichment
            with patch('common.llm_client.enrich_book_metadata') as mock_enrich:
                mock_enrich.return_value = enriched_data
                
                # Mock settings
                with patch('user_ingest_service.main.S') as mock_settings:
                    mock_settings.llm_service_enabled = True
                    
                    # Test the function
                    book_ids = await store_books("test-user-id", books_data, "test-request")
                    
                    # Verify enrichment was called
                    mock_enrich.assert_called_once()
                    
                    # Verify book was added to session with enriched data
                    mock_session_instance.add.assert_called_once()
                    added_book = mock_session_instance.add.call_args[0][0]
                    
                    assert added_book.title == "The Great Gatsby"
                    assert added_book.author == "F. Scott Fitzgerald"
                    assert added_book.raw_payload == enriched_data


# Integration test markers
pytestmark = pytest.mark.asyncio


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 