"""
FastAPI application for the LLM microservice.

This is the main entry point for the containerized LLM microservice with
comprehensive error handling, logging, and monitoring.
"""

import logging
import traceback
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse
from fastapi.exception_handlers import http_exception_handler
from pydantic import ValidationError
import uvicorn

from .models import (
    LLMRequest,
    LLMResponse,
    ErrorResponse,
    HealthResponse,
    ServiceConfig,
    LLMConfig,
)
from .services import LLMService, CacheService, LoggingService
from .utils import (
    LLMServiceError,
    create_error_response,
    get_http_status_from_error,
    ValidationError as ServiceValidationError,
    AuthenticationError,
    DuplicateRequestError,
    RateLimitError,
    OpenAIError,
    NetworkError,
    CacheError,
    LoggingError,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# Global services
cache_service: Optional[CacheService] = None
logging_service: Optional[LoggingService] = None
llm_service: Optional[LLMService] = None
config: Optional[ServiceConfig] = None


async def startup_event():
    """Initialize services on startup."""
    global cache_service, logging_service, llm_service, config

    try:
        # Load configuration
        config = ServiceConfig.from_env()

        # Set up logging level
        logging.getLogger().setLevel(config.log_level)

        logger.info(f"Starting {config.app_name} v{config.version}")
        logger.info(f"Debug mode: {config.debug}")

        # Initialize cache service
        cache_service = CacheService(config.redis)
        logger.info("Cache service initialized")

        # Initialize logging service
        logging_service = LoggingService(config.kafka, config.log_file)
        logger.info("Logging service initialized")

        # Initialize LLM service
        llm_config = LLMConfig(
            model_name=config.default_model,
            temperature=config.default_temperature,
            max_tokens=config.default_max_tokens,
        )

        llm_service = LLMService(
            config=llm_config,
            cache_service=cache_service,
            logging_service=logging_service,
        )
        logger.info("LLM service initialized")

        logger.info("All services initialized successfully")

    except Exception as e:
        logger.error(f"Failed to initialize services: {e}")
        logger.error(traceback.format_exc())
        raise


async def shutdown_event():
    """Clean up services on shutdown."""
    global cache_service, logging_service, llm_service

    logger.info("Shutting down services...")

    try:
        if logging_service:
            logging_service.close()
            logger.info("Logging service closed")

        if cache_service:
            cache_service.clear_expired()
            logger.info("Cache service cleaned up")

        logger.info("All services shut down successfully")

    except Exception as e:
        logger.error(f"Error during shutdown: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan."""
    await startup_event()
    yield
    await shutdown_event()


# Create FastAPI app
app = FastAPI(
    title="LLM Microservice",
    description="Production-ready FastAPI service for OpenAI LLM calls via LangChain",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure as needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=["*"],  # Configure as needed
)


@app.middleware("http")
async def logging_middleware(request: Request, call_next):
    """Log all requests and responses."""
    start_time = datetime.utcnow()

    try:
        response = await call_next(request)

        # Log successful requests
        duration = (datetime.utcnow() - start_time).total_seconds() * 1000
        logger.info(
            f"{request.method} {request.url.path} - "
            f"Status: {response.status_code} - "
            f"Duration: {duration:.2f}ms"
        )

        return response

    except Exception as e:
        # Log failed requests
        duration = (datetime.utcnow() - start_time).total_seconds() * 1000
        logger.error(
            f"{request.method} {request.url.path} - "
            f"Error: {str(e)} - "
            f"Duration: {duration:.2f}ms"
        )
        raise


@app.exception_handler(LLMServiceError)
async def service_error_handler(request: Request, exc: LLMServiceError):
    """Handle service-specific errors."""
    request_id = getattr(request.state, "request_id", "unknown")
    error_response = create_error_response(request_id, exc)
    status_code = get_http_status_from_error(exc)

    return JSONResponse(
        status_code=status_code,
        content=error_response.dict(),
    )


@app.exception_handler(ValidationError)
async def validation_error_handler(request: Request, exc: ValidationError):
    """Handle Pydantic validation errors."""
    request_id = getattr(request.state, "request_id", "unknown")

    # Convert Pydantic validation error to our format
    service_error = ServiceValidationError(
        message=str(exc),
        details={"validation_errors": exc.errors()},
    )

    error_response = create_error_response(request_id, service_error)

    return JSONResponse(
        status_code=400,
        content=error_response.dict(),
    )


@app.exception_handler(Exception)
async def general_error_handler(request: Request, exc: Exception):
    """Handle all other exceptions."""
    request_id = getattr(request.state, "request_id", "unknown")

    logger.error(f"Unhandled exception: {exc}")
    logger.error(traceback.format_exc())

    error_response = create_error_response(request_id, exc)

    return JSONResponse(
        status_code=500,
        content=error_response.dict(),
    )


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    try:
        dependencies = {}

        # Check cache service
        if cache_service:
            cache_health = cache_service.health_check()
            dependencies["redis"] = cache_health["status"]
        else:
            dependencies["redis"] = "disabled"

        # Check logging service
        if logging_service:
            logging_health = logging_service.health_check()
            dependencies["kafka"] = logging_health["status"]
        else:
            dependencies["kafka"] = "disabled"

        # Check LLM service
        if llm_service:
            llm_health = llm_service.health_check()
            dependencies["langchain"] = llm_health["status"]
        else:
            dependencies["langchain"] = "disabled"

        # Determine overall status
        statuses = list(dependencies.values())
        if all(status == "healthy" for status in statuses):
            overall_status = "healthy"
        elif any(status == "disabled" for status in statuses):
            overall_status = "degraded"
        else:
            overall_status = "unhealthy"

        return HealthResponse(
            status=overall_status,
            version=config.version if config else "unknown",
            timestamp=datetime.utcnow().isoformat() + "Z",
            dependencies=dependencies,
        )

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail="Health check failed")


@app.post("/invoke", response_model=LLMResponse)
async def invoke_llm(request: LLMRequest):
    """
    Invoke the LLM with the provided request.

    This endpoint handles:
    - Request validation
    - Caching and idempotency
    - LLM calls with retry logic
    - Error handling and logging
    - Response formatting
    """
    if not llm_service:
        raise HTTPException(status_code=503, detail="LLM service not available")

    try:
        # Check for duplicate request (idempotency)
        if cache_service:
            cached_response = cache_service.get(request.request_id)
            if cached_response:
                logger.info(
                    f"Returning cached response for request {request.request_id}"
                )
                return LLMResponse(**cached_response)

        # Validate authentication if enabled
        if config and config.enable_auth:
            # Here you would add your authentication logic
            # For now, we trust the OpenAI API key in the request
            pass

        # Process the request
        response = llm_service.invoke(request)

        logger.info(f"Successfully processed request {request.request_id}")
        return response

    except DuplicateRequestError as e:
        logger.warning(f"Duplicate request detected: {request.request_id}")
        raise HTTPException(status_code=409, detail=str(e))

    except AuthenticationError as e:
        logger.warning(f"Authentication failed for request {request.request_id}")
        raise HTTPException(status_code=401, detail=str(e))

    except RateLimitError as e:
        logger.warning(f"Rate limit exceeded for request {request.request_id}")
        raise HTTPException(status_code=429, detail=str(e))

    except OpenAIError as e:
        logger.error(f"OpenAI API error for request {request.request_id}: {e}")
        raise HTTPException(status_code=503, detail=str(e))

    except Exception as e:
        logger.error(f"Unexpected error for request {request.request_id}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/stats")
async def get_stats():
    """Get service statistics."""
    stats = {
        "service": {
            "name": config.app_name if config else "llm-microservice",
            "version": config.version if config else "unknown",
            "uptime": "unknown",  # You could track this
        }
    }

    if cache_service:
        stats["cache"] = cache_service.get_stats()

    if logging_service:
        stats["logging"] = logging_service.get_stats()

    if llm_service:
        stats["llm"] = llm_service.get_stats()

    return stats


def main():
    """Run the FastAPI application."""
    # Load configuration
    service_config = ServiceConfig.from_env()

    # Configure logging
    logging.getLogger().setLevel(service_config.log_level)

    # Run the application
    uvicorn.run(
        "src.llm_microservice.main:app",
        host=service_config.host,
        port=service_config.port,
        log_level=service_config.log_level.lower(),
        reload=service_config.debug,
        access_log=True,
    )


if __name__ == "__main__":
    main()
