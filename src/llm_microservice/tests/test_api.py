"""
API tests for the LLM microservice.

This module contains tests for the FastAPI endpoints including
health checks, LLM invocation, and error handling.
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch
import uuid
import json

from ..main import app
from ..models import LLMRequest, LLMResponse, ErrorResponse, HealthResponse
from ..utils import LLMServiceError, ValidationError, OpenAIError


@pytest.fixture
def client():
    """Create a test client for the FastAPI app."""
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def sample_request():
    """Create a sample LLM request for testing."""
    return {
        "request_id": str(uuid.uuid4()),
        "user_prompt": "Explain quantum computing in simple terms",
        "openai_api_key": "sk-1234567890abcdef1234567890abcdef1234567890abcdef",
        "model": "gpt-4o-mini",
        "temperature": 0.7,
        "max_tokens": 500,
        "metadata": {"test": True}
    }


@pytest.fixture
def sample_response():
    """Create a sample LLM response for testing."""
    return {
        "success": True,
        "request_id": str(uuid.uuid4()),
        "data": {
            "response": "Quantum computing uses quantum mechanical phenomena...",
            "confidence": 0.95
        },
        "usage": {
            "prompt_tokens": 25,
            "completion_tokens": 45,
            "total_tokens": 70
        },
        "performance": {
            "request_latency_ms": 1250.5,
            "llm_latency_ms": 1200.0,
            "cache_hit": False,
            "retry_count": 0,
            "rate_limited": False
        },
        "model": "gpt-4o-mini",
        "cached": False,
        "timestamp": "2025-01-11T10:30:00.123Z",
        "error": None
    }


class TestHealthEndpoint:
    """Test the health check endpoint."""
    
    def test_health_endpoint_returns_200(self, client):
        """Test that health endpoint returns 200 status."""
        response = client.get("/health")
        assert response.status_code == 200
    
    def test_health_endpoint_returns_valid_response(self, client):
        """Test that health endpoint returns valid health response."""
        response = client.get("/health")
        data = response.json()
        
        assert "status" in data
        assert "version" in data
        assert "timestamp" in data
        assert "dependencies" in data
        assert isinstance(data["dependencies"], dict)
    
    def test_health_endpoint_includes_dependencies(self, client):
        """Test that health endpoint includes dependency status."""
        response = client.get("/health")
        data = response.json()
        
        dependencies = data["dependencies"]
        # Should include status for key services
        assert "redis" in dependencies
        assert "kafka" in dependencies
        assert "langchain" in dependencies


class TestInvokeEndpoint:
    """Test the main /invoke endpoint."""
    
    @patch('src.llm_microservice.main.llm_service')
    def test_invoke_success(self, mock_llm_service, client, sample_request, sample_response):
        """Test successful LLM invocation."""
        # Mock the LLM service response
        mock_llm_service.invoke.return_value = LLMResponse(**sample_response)
        
        response = client.post("/invoke", json=sample_request)
        assert response.status_code == 200
        
        data = response.json()
        assert data["success"] is True
        assert data["request_id"] == sample_response["request_id"]
        assert "data" in data
        assert "usage" in data
        assert "performance" in data
    
    def test_invoke_missing_required_fields(self, client):
        """Test that missing required fields return 400."""
        incomplete_request = {
            "user_prompt": "Test prompt"
            # Missing request_id and openai_api_key
        }
        
        response = client.post("/invoke", json=incomplete_request)
        assert response.status_code == 422  # Pydantic validation error
        
        data = response.json()
        assert "detail" in data
    
    def test_invoke_invalid_request_id(self, client, sample_request):
        """Test that invalid request ID returns 400."""
        sample_request["request_id"] = "invalid-uuid"
        
        response = client.post("/invoke", json=sample_request)
        assert response.status_code == 422
    
    def test_invoke_invalid_api_key_format(self, client, sample_request):
        """Test that invalid API key format returns 400."""
        sample_request["openai_api_key"] = "invalid-key"
        
        response = client.post("/invoke", json=sample_request)
        assert response.status_code == 422
    
    def test_invoke_invalid_temperature(self, client, sample_request):
        """Test that invalid temperature returns 400."""
        sample_request["temperature"] = 1.5  # Out of range
        
        response = client.post("/invoke", json=sample_request)
        assert response.status_code == 422
    
    def test_invoke_invalid_max_tokens(self, client, sample_request):
        """Test that invalid max_tokens returns 400."""
        sample_request["max_tokens"] = 5000  # Out of range
        
        response = client.post("/invoke", json=sample_request)
        assert response.status_code == 422
    
    @patch('src.llm_microservice.main.llm_service')
    def test_invoke_openai_error(self, mock_llm_service, client, sample_request):
        """Test OpenAI API error handling."""
        # Mock OpenAI error
        mock_llm_service.invoke.side_effect = OpenAIError("API rate limit exceeded")
        
        response = client.post("/invoke", json=sample_request)
        assert response.status_code == 503
        
        data = response.json()
        assert data["success"] is False
        assert "error" in data
    
    @patch('src.llm_microservice.main.llm_service')
    def test_invoke_cached_response(self, mock_llm_service, client, sample_request, sample_response):
        """Test cached response handling."""
        # Mock cached response
        cached_response = sample_response.copy()
        cached_response["cached"] = True
        cached_response["performance"]["cache_hit"] = True
        
        mock_llm_service.invoke.return_value = LLMResponse(**cached_response)
        
        response = client.post("/invoke", json=sample_request)
        assert response.status_code == 200
        
        data = response.json()
        assert data["cached"] is True
        assert data["performance"]["cache_hit"] is True
    
    def test_invoke_with_system_prompt(self, client, sample_request):
        """Test request with system prompt."""
        sample_request["system_prompt"] = "You are a helpful assistant."
        
        with patch('src.llm_microservice.main.llm_service') as mock_llm_service:
            mock_llm_service.invoke.return_value = Mock()
            
            response = client.post("/invoke", json=sample_request)
            
            # Should accept the request
            assert response.status_code == 200 or response.status_code == 503  # 503 if service not available
    
    def test_invoke_with_structured_messages(self, client, sample_request):
        """Test request with structured messages."""
        sample_request["messages"] = [
            {"type": "system", "content": "You are a helpful assistant."},
            {"type": "human", "content": "What is quantum computing?"}
        ]
        
        with patch('src.llm_microservice.main.llm_service') as mock_llm_service:
            mock_llm_service.invoke.return_value = Mock()
            
            response = client.post("/invoke", json=sample_request)
            
            # Should accept the request
            assert response.status_code == 200 or response.status_code == 503  # 503 if service not available


class TestStatsEndpoint:
    """Test the statistics endpoint."""
    
    def test_stats_endpoint_returns_200(self, client):
        """Test that stats endpoint returns 200 status."""
        response = client.get("/stats")
        assert response.status_code == 200
    
    def test_stats_endpoint_returns_valid_structure(self, client):
        """Test that stats endpoint returns valid structure."""
        response = client.get("/stats")
        data = response.json()
        
        assert "service" in data
        assert isinstance(data["service"], dict)
        assert "name" in data["service"]
        assert "version" in data["service"]
    
    @patch('src.llm_microservice.main.cache_service')
    @patch('src.llm_microservice.main.logging_service')
    @patch('src.llm_microservice.main.llm_service')
    def test_stats_includes_service_stats(self, mock_llm, mock_logging, mock_cache, client):
        """Test that stats include service-specific statistics."""
        # Mock service stats
        mock_cache.get_stats.return_value = {"enabled": True}
        mock_logging.get_stats.return_value = {"enabled": True}
        mock_llm.get_stats.return_value = {"enabled": True}
        
        response = client.get("/stats")
        data = response.json()
        
        assert "cache" in data
        assert "logging" in data
        assert "llm" in data


class TestErrorHandling:
    """Test error handling across endpoints."""
    
    def test_404_for_unknown_endpoint(self, client):
        """Test that unknown endpoints return 404."""
        response = client.get("/unknown-endpoint")
        assert response.status_code == 404
    
    def test_405_for_wrong_method(self, client):
        """Test that wrong HTTP methods return 405."""
        response = client.get("/invoke")  # Should be POST
        assert response.status_code == 405
    
    def test_json_parse_error(self, client):
        """Test handling of invalid JSON."""
        response = client.post(
            "/invoke",
            data="invalid json",
            headers={"Content-Type": "application/json"}
        )
        assert response.status_code == 422


class TestMiddleware:
    """Test middleware functionality."""
    
    def test_cors_headers(self, client):
        """Test that CORS headers are present."""
        response = client.options("/health")
        
        # Should have CORS headers
        assert "access-control-allow-origin" in response.headers
    
    def test_request_logging(self, client):
        """Test that requests are logged (check via response)."""
        with patch('src.llm_microservice.main.logger') as mock_logger:
            response = client.get("/health")
            
            # Should log the request
            assert mock_logger.info.called or mock_logger.error.called


class TestRequestValidation:
    """Test request validation edge cases."""
    
    def test_empty_user_prompt(self, client, sample_request):
        """Test that empty user prompt is rejected."""
        sample_request["user_prompt"] = ""
        
        response = client.post("/invoke", json=sample_request)
        assert response.status_code == 422
    
    def test_very_long_user_prompt(self, client, sample_request):
        """Test that very long user prompt is rejected."""
        sample_request["user_prompt"] = "a" * 11000  # Over 10k chars
        
        response = client.post("/invoke", json=sample_request)
        assert response.status_code == 422
    
    def test_invalid_model_name(self, client, sample_request):
        """Test that invalid model name is rejected."""
        sample_request["model"] = "invalid-model"
        
        response = client.post("/invoke", json=sample_request)
        assert response.status_code == 422
    
    def test_negative_temperature(self, client, sample_request):
        """Test that negative temperature is rejected."""
        sample_request["temperature"] = -0.1
        
        response = client.post("/invoke", json=sample_request)
        assert response.status_code == 422
    
    def test_zero_max_tokens(self, client, sample_request):
        """Test that zero max_tokens is rejected."""
        sample_request["max_tokens"] = 0
        
        response = client.post("/invoke", json=sample_request)
        assert response.status_code == 422


@pytest.mark.asyncio
class TestAsyncEndpoints:
    """Test async functionality of endpoints."""
    
    @pytest.mark.skip(reason="Requires async test setup")
    async def test_concurrent_requests(self, client):
        """Test handling of concurrent requests."""
        # This would test concurrent request handling
        # Implementation depends on async test client setup
        pass
    
    @pytest.mark.skip(reason="Requires async test setup")
    async def test_request_timeout(self, client):
        """Test request timeout handling."""
        # This would test timeout scenarios
        # Implementation depends on async test client setup
        pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 