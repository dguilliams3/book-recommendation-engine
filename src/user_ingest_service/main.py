"""
User Ingest Service - FastAPI Application

Provides REST endpoints for Reader Mode users to upload their book lists.
Handles CSV/JSON parsing, validation, metadata enrichment, and event publishing.
"""

import hashlib
import uuid
import json
import csv
import io
from typing import List, Optional, Dict, Any
from datetime import datetime
from contextlib import asynccontextmanager
import time
from fastapi.responses import JSONResponse, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select, insert

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from common.settings import SettingsInstance as S
from common.structured_logging import get_logger
from common.kafka_utils import publish_event
from common.events import UserUploadedEvent, USER_UPLOADED_TOPIC
from common.metrics import REQUEST_COUNTER, REQUEST_LATENCY
from common.models import PublicUser, UploadedBook
from common.llm_client import enrich_book_metadata, LLMServiceError

logger = get_logger(__name__)

# Database setup
engine = create_async_engine(
    str(S.db_url).replace("postgresql://", "postgresql+asyncpg://"), echo=False
)
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage startup and shutdown tasks."""
    logger.info("Starting User Ingest Service")
    yield
    logger.info("Shutting down User Ingest Service")


app = FastAPI(
    title="User Ingest Service",
    description="Reader Mode book upload and processing service",
    version="1.0.0",
    lifespan=lifespan,
)

# ====================================================================
# MODELS
# ====================================================================


class BookUpload(BaseModel):
    """Individual book upload model"""

    title: str = Field(..., min_length=1, max_length=500)
    author: Optional[str] = Field(None, max_length=200)
    rating: Optional[int] = Field(None, ge=1, le=5)
    notes: Optional[str] = Field(None, max_length=1000)


class BooksUploadRequest(BaseModel):
    """JSON upload request model"""

    user_identifier: str = Field(..., description="Email or other identifier")
    books: List[BookUpload] = Field(..., min_length=1, max_length=100)


class UploadResponse(BaseModel):
    """Response model for successful uploads"""

    message: str
    user_hash_id: str
    books_processed: int
    upload_id: str


# ====================================================================
# UTILITIES
# ====================================================================


def hash_user_identifier(identifier: str) -> str:
    """Hash user identifier for privacy"""
    return hashlib.sha256(identifier.encode()).hexdigest()


def validate_csv_content(content: str) -> List[Dict[str, Any]]:
    """Validate and parse CSV content"""
    if len(content.encode("utf-8")) > 1024 * 1024:  # 1MB limit
        raise HTTPException(status_code=413, detail="File too large (max 1MB)")

    reader = csv.DictReader(io.StringIO(content))
    rows = list(reader)

    if len(rows) > 100:
        raise HTTPException(status_code=413, detail="Too many rows (max 100)")

    if len(rows) == 0:
        raise HTTPException(status_code=400, detail="No data rows found")

    # Validate required columns
    required_columns = {"title"}
    if not required_columns.issubset(set(reader.fieldnames or [])):
        raise HTTPException(
            status_code=400,
            detail=f"Missing required columns: {required_columns - set(reader.fieldnames or [])}",
        )

    return rows


async def upsert_user(user_hash_id: str) -> str:
    """Upsert user and return user ID"""
    async with async_session() as session:
        # Check if user exists
        result = await session.execute(
            select(PublicUser).where(PublicUser.hash_id == user_hash_id)
        )
        user = result.scalar_one_or_none()

        if user is None:
            # Create new user
            user = PublicUser(hash_id=user_hash_id)
            session.add(user)
            await session.commit()
            await session.refresh(user)
            logger.info("Created new public user", extra={"user_hash_id": user_hash_id})

        return str(user.id)


async def store_books(user_id: str, books: List[Dict[str, Any]], request_id: str = None) -> List[str]:
    """Store books in database with optional LLM enrichment and return book IDs"""
    book_ids = []
    enriched_count = 0

    async with async_session() as session:
        for book_data in books:
            # Try to enrich book metadata with LLM if enabled
            enriched_data = book_data.copy()
            
            if S.llm_service_enabled:
                try:
                    enriched_data = await enrich_book_metadata(
                        book_data, 
                        request_id or str(uuid.uuid4())
                    )
                    enriched_count += 1
                    logger.info(
                        f"Enriched book metadata: {enriched_data.get('title', 'Unknown')}",
                        extra={"user_id": user_id, "enriched": True}
                    )
                except LLMServiceError as e:
                    logger.warning(
                        f"LLM enrichment failed for book '{book_data.get('title', 'Unknown')}': {e}",
                        extra={"user_id": user_id, "book_title": book_data.get("title")}
                    )
                except Exception as e:
                    logger.error(
                        f"Unexpected error during book enrichment: {e}",
                        extra={"user_id": user_id, "book_title": book_data.get("title")},
                        exc_info=True
                    )

            # Create book record with enriched data
            book = UploadedBook(
                user_id=uuid.UUID(user_id),
                title=enriched_data.get("title") or book_data.get("title"),
                author=enriched_data.get("author") or book_data.get("author"),
                rating=enriched_data.get("rating") or book_data.get("rating"),
                notes=enriched_data.get("notes") or book_data.get("notes"),
                raw_payload=enriched_data,
                # Extract enriched metadata into individual columns
                isbn=enriched_data.get("isbn"),
                genre=enriched_data.get("genre", "General"),
                reading_level=enriched_data.get("reading_level", 5.0),
                read_date=enriched_data.get("read_date"),
                confidence=enriched_data.get("confidence", 0.0),
            )
            session.add(book)
            book_ids.append(str(book.id))

        await session.commit()
        logger.info(
            "Stored books with enrichment",
            extra={
                "user_id": user_id,
                "total_books": len(books),
                "enriched_books": enriched_count,
                "enrichment_rate": enriched_count / len(books) if books else 0
            }
        )

    return book_ids


# ====================================================================
# ENDPOINTS
# ====================================================================


@app.post("/upload_books", response_model=UploadResponse)
async def upload_books_json(request: BooksUploadRequest):
    """Upload books via JSON payload"""
    try:
        # Hash user identifier
        user_hash_id = hash_user_identifier(request.user_identifier)

        # Upsert user
        user_id = await upsert_user(user_hash_id)

        # Convert books to dict format
        books_data = [book.model_dump() for book in request.books]

        # Store books with enrichment
        upload_id = str(uuid.uuid4())
        book_ids = await store_books(user_id, books_data, upload_id)

        # Publish event
        event = UserUploadedEvent(
            user_hash_id=user_hash_id, book_count=len(books_data), book_ids=book_ids
        )
        await publish_event(USER_UPLOADED_TOPIC, event.model_dump())

        return UploadResponse(
            message="Books uploaded successfully",
            user_hash_id=user_hash_id,
            books_processed=len(books_data),
            upload_id=upload_id,
        )

    except Exception as e:
        logger.error("Failed to process JSON upload", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/upload_books_csv", response_model=UploadResponse)
async def upload_books_csv(
    file: UploadFile = File(...), user_identifier: str = Form(...)
):
    """Upload books via CSV file"""
    try:
        # Validate file type
        if not file.filename or not file.filename.endswith(".csv"):
            raise HTTPException(status_code=400, detail="Only CSV files are allowed")

        # Read and validate content
        content = await file.read()
        try:
            content_str = content.decode("utf-8")
        except UnicodeDecodeError:
            raise HTTPException(status_code=400, detail="File must be UTF-8 encoded")

        # Parse and validate CSV
        books_data = validate_csv_content(content_str)

        # Hash user identifier
        user_hash_id = hash_user_identifier(user_identifier)

        # Upsert user
        user_id = await upsert_user(user_hash_id)

        # Store books with enrichment
        upload_id = str(uuid.uuid4())
        book_ids = await store_books(user_id, books_data, upload_id)

        # Publish event
        event = UserUploadedEvent(
            user_hash_id=user_hash_id, book_count=len(books_data), book_ids=book_ids
        )
        await publish_event(USER_UPLOADED_TOPIC, event.dict())

        return UploadResponse(
            message="Books uploaded successfully",
            user_hash_id=user_hash_id,
            books_processed=len(books_data),
            upload_id=upload_id,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to process CSV upload", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "user_ingest_service"}


@app.get("/metrics")
async def metrics_endpoint():
    """Prometheus metrics endpoint for scraping."""
    from common.metrics import _PROM

    if not _PROM:
        return JSONResponse(
            {"detail": "prometheus_client not installed"}, status_code=501
        )
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/live")
async def live():
    """Liveness probe."""
    return {"status": "alive", "timestamp": time.time()}


@app.get("/ready")
async def ready():
    """Readiness probe (delegates to health check)."""
    return await health_check()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=S.user_ingest_port)
