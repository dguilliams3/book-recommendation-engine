"""
LLM Microservice - Production-ready FastAPI service for OpenAI LLM calls via LangChain.

This microservice provides a containerized, reusable service for LLM interactions with:
- FastAPI HTTP API layer
- OpenAI integration via LangChain
- Redis caching and idempotency
- Kafka logging with file fallback
- Comprehensive error handling
- Production-ready monitoring
"""

__version__ = "1.0.0" 