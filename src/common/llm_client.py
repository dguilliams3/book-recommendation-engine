"""
LLM Client Library for Book Recommendation System

This module provides a standardized interface for calling the LLM microservice
with comprehensive error handling, circuit breaker pattern, and fallback mechanisms.
"""

import asyncio
import json
import time
import uuid
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum

import httpx
from pydantic import BaseModel, Field

from .settings import settings
from .structured_logging import get_logger

logger = get_logger(__name__)


class CircuitBreakerState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5
    recovery_timeout: int = 60
    success_threshold: int = 3


class CircuitBreaker:
    """Circuit breaker implementation for LLM service calls."""
    
    def __init__(self, config: CircuitBreakerConfig = None):
        self.config = config or CircuitBreakerConfig()
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[datetime] = None
        
    def can_execute(self) -> bool:
        """Check if the circuit breaker allows execution."""
        if self.state == CircuitBreakerState.CLOSED:
            return True
        elif self.state == CircuitBreakerState.OPEN:
            if self.last_failure_time and \
               datetime.now() - self.last_failure_time > timedelta(seconds=self.config.recovery_timeout):
                self.state = CircuitBreakerState.HALF_OPEN
                self.success_count = 0
                logger.info("Circuit breaker moving to HALF_OPEN state")
                return True
            return False
        elif self.state == CircuitBreakerState.HALF_OPEN:
            return True
        return False
    
    def record_success(self):
        """Record a successful execution."""
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
                logger.info("Circuit breaker moving to CLOSED state")
        elif self.state == CircuitBreakerState.CLOSED:
            self.failure_count = 0
    
    def record_failure(self):
        """Record a failed execution."""
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        if self.state == CircuitBreakerState.CLOSED:
            if self.failure_count >= self.config.failure_threshold:
                self.state = CircuitBreakerState.OPEN
                logger.warning(f"Circuit breaker moving to OPEN state after {self.failure_count} failures")
        elif self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.OPEN
            logger.warning("Circuit breaker moving back to OPEN state")


class LLMRequest(BaseModel):
    """Request model for LLM service calls."""
    request_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_prompt: str
    system_prompt: Optional[str] = None
    messages: Optional[List[Dict[str, str]]] = None
    model: str = "gpt-4o-mini"
    temperature: float = 0.7
    max_tokens: int = 1000
    openai_api_key: Optional[str] = None


class LLMResponse(BaseModel):
    """Response model for LLM service calls."""
    success: bool
    request_id: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, Any]] = None
    usage: Optional[Dict[str, Any]] = None
    performance: Optional[Dict[str, Any]] = None
    model: str
    cached: bool = False
    timestamp: str


class LLMServiceError(Exception):
    """Base exception for LLM service errors."""
    pass


class LLMServiceUnavailableError(LLMServiceError):
    """Raised when LLM service is unavailable."""
    pass


class LLMServiceTimeoutError(LLMServiceError):
    """Raised when LLM service times out."""
    pass


class LLMServiceRateLimitError(LLMServiceError):
    """Raised when rate limit is exceeded."""
    pass


class LLMClient:
    """Client for interacting with the LLM microservice."""
    
    def __init__(self):
        self.base_url = getattr(settings, 'llm_service_url', 'http://llm_microservice:8000')
        self.timeout = getattr(settings, 'llm_request_timeout', 30)
        self.max_retries = getattr(settings, 'llm_max_retries', 3)
        self.circuit_breaker = CircuitBreaker()
        self.fallback_enabled = getattr(settings, 'llm_fallback_enabled', True)
        
        # Metrics
        self.request_count = 0
        self.success_count = 0
        self.failure_count = 0
        self.fallback_count = 0
        
    async def invoke(self, request: LLMRequest) -> LLMResponse:
        """
        Invoke the LLM with circuit breaker and fallback.
        
        Args:
            request: LLM request object
            
        Returns:
            LLM response object
            
        Raises:
            LLMServiceError: If service fails and fallback is disabled
        """
        self.request_count += 1
        
        # Check circuit breaker
        if not self.circuit_breaker.can_execute():
            logger.warning("Circuit breaker is OPEN, attempting fallback")
            if self.fallback_enabled:
                return await self._fallback_openai(request)
            else:
                raise LLMServiceUnavailableError("LLM service is unavailable and fallback is disabled")
        
        # Try LLM microservice
        try:
            response = await self._call_service(request)
            self.circuit_breaker.record_success()
            self.success_count += 1
            return response
            
        except Exception as e:
            self.circuit_breaker.record_failure()
            self.failure_count += 1
            
            logger.error(f"LLM service call failed: {e}", extra={"request_id": request.request_id})
            
            # Try fallback if enabled
            if self.fallback_enabled:
                logger.info("Attempting fallback to direct OpenAI", extra={"request_id": request.request_id})
                try:
                    response = await self._fallback_openai(request)
                    self.fallback_count += 1
                    return response
                except Exception as fallback_error:
                    logger.error(f"Fallback also failed: {fallback_error}", extra={"request_id": request.request_id})
                    raise LLMServiceError(f"Both LLM service and fallback failed: {e}, {fallback_error}")
            else:
                raise LLMServiceError(f"LLM service failed: {e}")
    
    async def _call_service(self, request: LLMRequest) -> LLMResponse:
        """Call the LLM microservice."""
        # Use OpenAI API key from request or settings
        if not request.openai_api_key:
            request.openai_api_key = getattr(settings, 'openai_api_key', None)
        
        if not request.openai_api_key:
            raise LLMServiceError("OpenAI API key not provided")
        
        request_data = request.dict()
        
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                response = await client.post(
                    f"{self.base_url}/invoke",
                    json=request_data,
                    headers={"Content-Type": "application/json"}
                )
                
                if response.status_code == 429:
                    raise LLMServiceRateLimitError("Rate limit exceeded")
                elif response.status_code >= 500:
                    raise LLMServiceUnavailableError(f"Service error: {response.status_code}")
                elif response.status_code >= 400:
                    error_detail = response.text
                    raise LLMServiceError(f"Client error {response.status_code}: {error_detail}")
                
                response.raise_for_status()
                response_data = response.json()
                
                return LLMResponse(**response_data)
                
            except httpx.TimeoutException:
                raise LLMServiceTimeoutError("Request timed out")
            except httpx.ConnectError:
                raise LLMServiceUnavailableError("Cannot connect to LLM service")
            except httpx.HTTPError as e:
                raise LLMServiceError(f"HTTP error: {e}")
    
    async def _fallback_openai(self, request: LLMRequest) -> LLMResponse:
        """Fallback to direct OpenAI API call."""
        logger.info("Using direct OpenAI fallback", extra={"request_id": request.request_id})
        
        try:
            from langchain_openai import ChatOpenAI
            from langchain_core.messages import HumanMessage, SystemMessage
            
            # Initialize OpenAI client
            llm = ChatOpenAI(
                model=request.model,
                temperature=request.temperature,
                max_tokens=request.max_tokens,
                openai_api_key=request.openai_api_key or settings.openai_api_key
            )
            
            # Build messages
            messages = []
            if request.system_prompt:
                messages.append(SystemMessage(content=request.system_prompt))
            
            if request.messages:
                # Convert message format
                for msg in request.messages:
                    if msg.get("type") == "system":
                        messages.append(SystemMessage(content=msg["content"]))
                    else:
                        messages.append(HumanMessage(content=msg["content"]))
            
            messages.append(HumanMessage(content=request.user_prompt))
            
            # Make the call
            start_time = time.time()
            result = await llm.ainvoke(messages)
            end_time = time.time()
            
            # Format response to match microservice format
            return LLMResponse(
                success=True,
                request_id=request.request_id,
                data={
                    "response": result.content,
                    "confidence": 0.8  # Default confidence for fallback
                },
                usage={
                    "prompt_tokens": 0,  # Not available in fallback
                    "completion_tokens": 0,
                    "total_tokens": 0
                },
                performance={
                    "request_latency_ms": (end_time - start_time) * 1000,
                    "llm_latency_ms": (end_time - start_time) * 1000,
                    "cache_hit": False,
                    "retry_count": 0,
                    "rate_limited": False
                },
                model=request.model,
                cached=False,
                timestamp=datetime.now().isoformat() + "Z"
            )
            
        except Exception as e:
            logger.error(f"Direct OpenAI fallback failed: {e}", extra={"request_id": request.request_id})
            raise LLMServiceError(f"Fallback failed: {e}")
    
    async def health_check(self) -> Dict[str, Any]:
        """Check the health of the LLM service."""
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                response = await client.get(f"{self.base_url}/health")
                response.raise_for_status()
                return {
                    "status": "healthy",
                    "service_response": response.json(),
                    "circuit_breaker_state": self.circuit_breaker.state.value
                }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "circuit_breaker_state": self.circuit_breaker.state.value
            }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics."""
        return {
            "request_count": self.request_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "fallback_count": self.fallback_count,
            "success_rate": self.success_count / max(self.request_count, 1),
            "fallback_rate": self.fallback_count / max(self.request_count, 1),
            "circuit_breaker_state": self.circuit_breaker.state.value
        }


# Global client instance
_llm_client: Optional[LLMClient] = None


def get_llm_client() -> LLMClient:
    """Get the global LLM client instance."""
    global _llm_client
    if _llm_client is None:
        _llm_client = LLMClient()
    return _llm_client


# Convenience functions
async def invoke_llm(
    user_prompt: str,
    system_prompt: Optional[str] = None,
    model: str = "gpt-4o-mini",
    temperature: float = 0.7,
    max_tokens: int = 1000,
    request_id: Optional[str] = None
) -> LLMResponse:
    """
    Convenience function to invoke LLM.
    
    Args:
        user_prompt: The user's prompt
        system_prompt: Optional system prompt
        model: Model to use
        temperature: Temperature for generation
        max_tokens: Maximum tokens to generate
        request_id: Optional request ID
        
    Returns:
        LLM response
    """
    client = get_llm_client()
    request = LLMRequest(
        request_id=request_id or str(uuid.uuid4()),
        user_prompt=user_prompt,
        system_prompt=system_prompt,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens
    )
    return await client.invoke(request)


async def enrich_recommendations_with_llm(
    recommendations: List[Dict[str, Any]],
    user_context: Dict[str, Any],
    query: str,
    request_id: str
) -> List[Dict[str, Any]]:
    """
    Enrich recommendations with LLM-generated explanations.
    
    Args:
        recommendations: List of recommendation dictionaries
        user_context: User context (user_hash_id, uploaded_books, etc.)
        query: User's search query
        request_id: Request ID for tracking
        
    Returns:
        Enhanced recommendations with better justifications
    """
    if not recommendations:
        return recommendations
    
    # Build context for LLM
    context_str = f"User Query: {query}\n"
    
    if user_context.get('uploaded_books'):
        books_str = "\n".join([
            f"- {book.get('title', 'Unknown')} by {book.get('author', 'Unknown')}"
            for book in user_context['uploaded_books'][:5]
        ])
        context_str += f"\nUser's Reading History:\n{books_str}\n"
    
    # Format recommendations for LLM
    recs_str = "\n".join([
        f"{i+1}. {rec.get('title', 'Unknown')} by {rec.get('author', 'Unknown')} "
        f"(Level: {rec.get('reading_level', 'Unknown')}, Genre: {rec.get('genre', 'Unknown')})\n"
        f"   Current justification: {rec.get('justification', 'No justification')}"
        for i, rec in enumerate(recommendations)
    ])
    
    user_prompt = f"""
    {context_str}
    
    Current Recommendations:
    {recs_str}
    
    Please enhance the justifications for these recommendations. For each book, provide:
    1. Why it matches the user's interests based on their query and reading history
    2. What makes this book unique and appealing
    3. How it connects to their preferences or reading level
    
    Keep each justification under 100 words and make them personal and engaging.
    Return the enhanced justifications in the same order, numbered 1-{len(recommendations)}.
    """
    
    system_prompt = """You are an expert librarian helping to enhance book recommendations. 
    Provide thoughtful, personalized justifications that help readers understand why 
    each book is a good match for them. Focus on connecting the book to their interests,
    reading history, and personal preferences."""
    
    try:
        response = await invoke_llm(
            user_prompt=user_prompt,
            system_prompt=system_prompt,
            model="gpt-4o-mini",
            temperature=0.7,
            max_tokens=1500,
            request_id=request_id
        )
        
        if response.success and response.data:
            enhanced_content = response.data.get("response", "")
            
            # Parse enhanced justifications
            enhanced_recs = []
            lines = enhanced_content.split('\n')
            current_justification = ""
            rec_index = 0
            
            for line in lines:
                line = line.strip()
                if line.startswith(f"{rec_index + 1}."):
                    # Save previous justification
                    if current_justification and rec_index > 0:
                        if rec_index - 1 < len(recommendations):
                            enhanced_rec = recommendations[rec_index - 1].copy()
                            enhanced_rec['justification'] = current_justification.strip()
                            enhanced_rec['llm_enhanced'] = True
                            enhanced_recs.append(enhanced_rec)
                    
                    # Start new justification
                    current_justification = line[line.find('.') + 1:].strip()
                    rec_index += 1
                elif line and rec_index > 0:
                    current_justification += " " + line
            
            # Handle last justification
            if current_justification and rec_index > 0:
                if rec_index - 1 < len(recommendations):
                    enhanced_rec = recommendations[rec_index - 1].copy()
                    enhanced_rec['justification'] = current_justification.strip()
                    enhanced_rec['llm_enhanced'] = True
                    enhanced_recs.append(enhanced_rec)
            
            # Fill in any missing recommendations with originals
            while len(enhanced_recs) < len(recommendations):
                original_rec = recommendations[len(enhanced_recs)].copy()
                original_rec['llm_enhanced'] = False
                enhanced_recs.append(original_rec)
            
            logger.info(f"Enhanced {len(enhanced_recs)} recommendations with LLM", 
                       extra={"request_id": request_id})
            return enhanced_recs
            
    except Exception as e:
        logger.error(f"Failed to enhance recommendations with LLM: {e}", 
                    extra={"request_id": request_id})
    
    # Return original recommendations if enhancement fails
    for rec in recommendations:
        rec['llm_enhanced'] = False
    return recommendations


async def enrich_book_metadata(
    book_data: Dict[str, Any],
    request_id: str
) -> Dict[str, Any]:
    """
    Enrich user-uploaded book data with missing metadata.
    
    Args:
        book_data: Book data dictionary
        request_id: Request ID for tracking
        
    Returns:
        Enhanced book data with additional metadata
    """
    user_prompt = f"""
    Extract and enrich the following book information:
    
    Title: {book_data.get('title', 'Unknown')}
    Author: {book_data.get('author', 'Unknown')}
    Genre: {book_data.get('genre', 'Unknown')}
    Reading Level: {book_data.get('reading_level', 'Unknown')}
    
    Raw Data: {json.dumps(book_data, indent=2)}
    
    Please provide the following information in JSON format:
    {{
        "title": "Corrected/standardized title",
        "author": "Corrected/standardized author name",
        "reading_level": "Estimated grade level as a number (e.g., 4.5)",
        "genre": "Primary genre classification",
        "isbn": "ISBN if you can identify it, otherwise null",
        "confidence": "Confidence score 0-1 for the enrichment",
        "notes": "Any relevant notes about the enrichment process"
    }}
    
    Focus on:
    1. Correcting any obvious typos in title or author
    2. Estimating appropriate reading level if missing
    3. Classifying the genre if not provided
    4. Being conservative with confidence scores
    """
    
    system_prompt = """You are an expert librarian helping to standardize and enrich book metadata. 
    Use your knowledge to correct typos, standardize author names, estimate reading levels, 
    and classify genres. Be conservative with confidence scores and note any uncertainties."""
    
    try:
        response = await invoke_llm(
            user_prompt=user_prompt,
            system_prompt=system_prompt,
            model="gpt-4o-mini",
            temperature=0.3,  # Lower temperature for more consistent metadata
            max_tokens=500,
            request_id=request_id
        )
        
        if response.success and response.data:
            enhanced_content = response.data.get("response", "")
            
            # Try to parse JSON response
            try:
                import re
                # Extract JSON from response
                json_match = re.search(r'\{[^}]+\}', enhanced_content, re.DOTALL)
                if json_match:
                    enhanced_data = json.loads(json_match.group())
                    
                    # Merge with original data
                    result = book_data.copy()
                    result.update(enhanced_data)
                    result['llm_enriched'] = True
                    result['enrichment_timestamp'] = datetime.now().isoformat()
                    
                    logger.info(f"Enhanced book metadata for: {result.get('title', 'Unknown')}", 
                               extra={"request_id": request_id})
                    return result
                    
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse LLM JSON response: {e}", 
                              extra={"request_id": request_id})
                
    except Exception as e:
        logger.error(f"Failed to enrich book metadata: {e}", 
                    extra={"request_id": request_id})
    
    # Return original data if enrichment fails
    result = book_data.copy()
    result['llm_enriched'] = False
    return result 