"""
LLM service for the LLM microservice.

This module provides the core LLM functionality with LangChain integration,
smart message array building, and comprehensive error handling.
"""

import time
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
import logging

try:
    from langchain_openai import ChatOpenAI
    from langchain.prompts import (
        ChatPromptTemplate,
        SystemMessagePromptTemplate,
        HumanMessagePromptTemplate,
        AIMessagePromptTemplate,
    )
    from langchain_core.messages import (
        BaseMessage,
        SystemMessage,
        HumanMessage,
        AIMessage,
    )

    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False

from ..models.requests import LLMRequest, LangChainMessage, MessageType
from ..models.responses import LLMResponse, TokenUsage, PerformanceMetrics
from ..models.config import LLMConfig
from ..utils.errors import (
    LLMServiceError,
    OpenAIError,
    NetworkError,
    ValidationError,
    map_openai_error,
)
from ..utils.retry import retry_with_exponential_backoff, OPENAI_RETRY_CONFIG
from .cache_service import CacheService
from .logging_service import LoggingService

logger = logging.getLogger(__name__)


class LLMService:
    """Core LLM service with LangChain integration."""

    def __init__(
        self,
        config: LLMConfig,
        cache_service: Optional[CacheService] = None,
        logging_service: Optional[LoggingService] = None,
    ):
        self.config = config
        self.cache_service = cache_service
        self.logging_service = logging_service
        self.llm_client = None
        self.enabled = LANGCHAIN_AVAILABLE
        self.stats = {
            "requests_processed": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "total_tokens_used": 0,
            "total_cost_usd": 0.0,
            "errors": 0,
            "retries": 0,
        }

        if not self.enabled:
            logger.error("LangChain not available, LLM service disabled")
            raise LLMServiceError("LangChain not available")

    def _initialize_llm_client(self, api_key: str, model_name: str) -> ChatOpenAI:
        """Initialize LangChain ChatOpenAI client."""
        try:
            return ChatOpenAI(
                openai_api_key=api_key,
                model_name=model_name,
                temperature=self.config.temperature,
                max_tokens=self.config.max_tokens,
                top_p=self.config.top_p,
                frequency_penalty=self.config.frequency_penalty,
                presence_penalty=self.config.presence_penalty,
                request_timeout=self.config.request_timeout,
                max_retries=0,  # We handle retries ourselves
            )
        except Exception as e:
            logger.error(f"Failed to initialize LangChain client: {e}")
            raise LLMServiceError(f"Failed to initialize LLM client: {e}")

    def _build_messages_array(self, request: LLMRequest) -> List[BaseMessage]:
        """
        Smartly build messages array from various optional prompt inputs.

        This implements the priority system from the API specification:
        1. Use structured messages if provided
        2. Use system_prompt + user_prompt if system_prompt provided
        3. Always add user_prompt (required field)

        Args:
            request: The LLM request

        Returns:
            List[BaseMessage]: Array of LangChain messages
        """
        messages = []

        # Priority 1: Use structured messages if provided
        if request.messages:
            for message in request.messages:
                if message.type == MessageType.SYSTEM:
                    messages.append(SystemMessage(content=message.content))
                elif message.type == MessageType.HUMAN:
                    messages.append(HumanMessage(content=message.content))
                elif message.type == MessageType.AI:
                    messages.append(AIMessage(content=message.content))
            return messages

        # Priority 2: Use system_prompt + user_prompt if system_prompt provided
        if request.system_prompt:
            messages.append(SystemMessage(content=request.system_prompt))

        # Priority 3: Always add user_prompt (required field)
        messages.append(HumanMessage(content=request.user_prompt))

        return messages

    def _calculate_cost(self, usage: TokenUsage, model_name: str) -> float:
        """
        Calculate approximate cost based on token usage.

        Args:
            usage: Token usage information
            model_name: The model used

        Returns:
            float: Approximate cost in USD
        """
        # Approximate pricing per 1K tokens (as of 2024)
        pricing = {
            "gpt-4o": {"input": 0.005, "output": 0.015},
            "gpt-4o-mini": {"input": 0.00015, "output": 0.0006},
            "gpt-3.5-turbo": {"input": 0.0015, "output": 0.002},
            "gpt-3.5-turbo-16k": {"input": 0.003, "output": 0.004},
        }

        model_pricing = pricing.get(model_name, {"input": 0.001, "output": 0.002})

        input_cost = (usage.prompt_tokens / 1000) * model_pricing["input"]
        output_cost = (usage.completion_tokens / 1000) * model_pricing["output"]

        return input_cost + output_cost

    def _extract_usage_from_response(self, response: Any) -> TokenUsage:
        """
        Extract token usage from LangChain response.

        Args:
            response: LangChain response object

        Returns:
            TokenUsage: Token usage information
        """
        try:
            # Try to get usage from response metadata
            if hasattr(response, "response_metadata"):
                usage_data = response.response_metadata.get("token_usage", {})
                return TokenUsage(
                    prompt_tokens=usage_data.get("prompt_tokens", 0),
                    completion_tokens=usage_data.get("completion_tokens", 0),
                    total_tokens=usage_data.get("total_tokens", 0),
                )
            else:
                # Fallback: estimate tokens (very rough)
                content = (
                    str(response.content)
                    if hasattr(response, "content")
                    else str(response)
                )
                estimated_completion = len(content.split()) * 1.3  # Rough estimate
                estimated_prompt = 50  # Default estimate

                return TokenUsage(
                    prompt_tokens=int(estimated_prompt),
                    completion_tokens=int(estimated_completion),
                    total_tokens=int(estimated_prompt + estimated_completion),
                )
        except Exception as e:
            logger.warning(f"Failed to extract usage information: {e}")
            return TokenUsage(
                prompt_tokens=0,
                completion_tokens=0,
                total_tokens=0,
            )

    @retry_with_exponential_backoff(
        max_retries=OPENAI_RETRY_CONFIG.max_retries,
        base_delay=OPENAI_RETRY_CONFIG.base_delay,
        max_delay=OPENAI_RETRY_CONFIG.max_delay,
        retry_exceptions=(Exception,),  # We'll handle specific exceptions in the method
        stop_exceptions=(),
    )
    def _call_llm_with_retry(
        self,
        llm_client: ChatOpenAI,
        messages: List[BaseMessage],
        request_id: str,
    ) -> Any:
        """
        Call LLM with retry logic and error handling.

        Args:
            llm_client: The LangChain ChatOpenAI client
            messages: The messages to send
            request_id: The request ID for logging

        Returns:
            Any: LLM response
        """
        try:
            response = llm_client.invoke(messages)
            return response
        except Exception as e:
            # Map OpenAI errors to our error types
            mapped_error = map_openai_error(e)
            logger.error(f"LLM call failed for request {request_id}: {mapped_error}")
            self.stats["retries"] += 1
            raise mapped_error

    def invoke(self, request: LLMRequest) -> LLMResponse:
        """
        Process an LLM request with caching, retries, and logging.

        Args:
            request: The LLM request

        Returns:
            LLMResponse: The response
        """
        start_time = time.time()
        request_id = request.request_id
        cached_response = None
        retry_count = 0
        rate_limited = False

        try:
            # Check cache first
            if self.cache_service:
                cached_response = self.cache_service.get(request_id)
                if cached_response:
                    self.stats["cache_hits"] += 1
                    logger.debug(f"Cache hit for request {request_id}")

                    # Return cached response with updated metadata
                    cached_response["cached"] = True
                    cached_response["performance"]["cache_hit"] = True
                    cached_response["timestamp"] = datetime.utcnow().isoformat() + "Z"

                    return LLMResponse(**cached_response)

            self.stats["cache_misses"] += 1

            # Initialize LLM client
            llm_client = self._initialize_llm_client(
                api_key=request.openai_api_key, model_name=request.model.value
            )

            # Build messages array
            messages = self._build_messages_array(request)

            # Call LLM with retry logic
            llm_start_time = time.time()
            try:
                response = self._call_llm_with_retry(llm_client, messages, request_id)
            except Exception as e:
                if "rate limit" in str(e).lower():
                    rate_limited = True
                raise

            llm_end_time = time.time()
            llm_latency_ms = (llm_end_time - llm_start_time) * 1000

            # Extract usage information
            usage = self._extract_usage_from_response(response)

            # Calculate cost
            cost = self._calculate_cost(usage, request.model.value)

            # Update stats
            self.stats["requests_processed"] += 1
            self.stats["total_tokens_used"] += usage.total_tokens
            self.stats["total_cost_usd"] += cost

            # Build response
            end_time = time.time()
            request_latency_ms = (end_time - start_time) * 1000

            response_data = {
                "success": True,
                "request_id": request_id,
                "data": {
                    "response": (
                        response.content
                        if hasattr(response, "content")
                        else str(response)
                    ),
                    "confidence": 0.95,  # Default confidence
                },
                "usage": usage,
                "performance": PerformanceMetrics(
                    request_latency_ms=request_latency_ms,
                    llm_latency_ms=llm_latency_ms,
                    cache_hit=False,
                    retry_count=retry_count,
                    rate_limited=rate_limited,
                ),
                "model": request.model.value,
                "cached": False,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "error": None,
            }

            llm_response = LLMResponse(**response_data)

            # Cache the response
            if self.cache_service:
                self.cache_service.set(request_id, response_data)

            # Log the request
            if self.logging_service:
                self.logging_service.log_request(
                    request_id=request_id,
                    request_data=request.dict(),
                    response_data=response_data,
                    duration_ms=request_latency_ms,
                    metadata={"model": request.model.value, "cost_usd": cost},
                )

                # Log metrics
                self.logging_service.log_metrics(
                    request_id=request_id,
                    metrics={
                        "request_latency_ms": request_latency_ms,
                        "llm_latency_ms": llm_latency_ms,
                        "tokens_used": usage.total_tokens,
                        "cost_usd": cost,
                        "cache_hit": False,
                        "retry_count": retry_count,
                        "rate_limited": rate_limited,
                    },
                )

            return llm_response

        except Exception as e:
            # Update error stats
            self.stats["errors"] += 1

            # Log error
            if self.logging_service:
                self.logging_service.log_error(
                    request_id=request_id,
                    error=e,
                    context={"model": request.model.value, "retry_count": retry_count},
                )

            # Re-raise the error
            raise

    def get_stats(self) -> Dict[str, Any]:
        """
        Get LLM service statistics.

        Returns:
            Dict[str, Any]: Service statistics
        """
        return {
            "enabled": self.enabled,
            "langchain_available": LANGCHAIN_AVAILABLE,
            "stats": self.stats.copy(),
            "config": {
                "model_name": self.config.model_name,
                "temperature": self.config.temperature,
                "max_tokens": self.config.max_tokens,
                "max_retries": self.config.max_retries,
                "request_timeout": self.config.request_timeout,
            },
        }

    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on LLM service.

        Returns:
            Dict[str, Any]: Health check results
        """
        health = {
            "status": "healthy",
            "langchain_available": LANGCHAIN_AVAILABLE,
            "enabled": self.enabled,
        }

        if not self.enabled:
            health["status"] = "disabled"
            health["error"] = "LangChain not available"

        return health
