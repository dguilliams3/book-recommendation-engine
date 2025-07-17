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
import asyncio
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
from sqlalchemy import select, insert, text, func

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
    if len(content.encode("utf-8")) > 100 * 1024:  # 100KB limit
        raise HTTPException(status_code=413, detail="File too large (max 100KB)")

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

    # Enforce field length limits for each row
    for i, row in enumerate(rows):
        if len(row.get("title", "")) > 500:
            raise HTTPException(status_code=400, detail=f"Row {i+1}: Title too long (max 500 chars)")
        if row.get("author") and len(row["author"]) > 200:
            raise HTTPException(status_code=400, detail=f"Row {i+1}: Author too long (max 200 chars)")
        if row.get("notes") and len(row["notes"]) > 1000:
            raise HTTPException(status_code=400, detail=f"Row {i+1}: Notes too long (max 1000 chars)")

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


async def check_exact_duplicate(user_id: str, title: str, author: str = None) -> bool:
    """
    Check if a book with the same title and author already exists for this user.
    
    Args:
        user_id: User ID
        title: Book title
        author: Book author (optional)
        
    Returns:
        True if exact duplicate exists, False otherwise
    """
    async with async_session() as session:
        # Normalize title and author for comparison
        normalized_title = title.strip().lower() if title else ""
        normalized_author = author.strip().lower() if author else ""
        
        # Build query based on whether author is provided
        if normalized_author:
            # Check for exact title + author match
            query = select(UploadedBook).where(
                UploadedBook.user_id == uuid.UUID(user_id),
                func.lower(UploadedBook.title) == normalized_title,
                func.lower(UploadedBook.author) == normalized_author
            )
        else:
            # Check for exact title match only (when no author provided)
            query = select(UploadedBook).where(
                UploadedBook.user_id == uuid.UUID(user_id),
                func.lower(UploadedBook.title) == normalized_title
            )
        
        result = await session.execute(query)
        existing_book = result.scalar_one_or_none()
        
        if existing_book:
            logger.info(
                f"Exact duplicate found: {title} by {author}",
                extra={
                    "user_id": user_id,
                    "existing_book_id": str(existing_book.id),
                    "duplicate_type": "exact_match"
                }
            )
            return True
        
        return False


async def check_enriched_duplicate(user_id: str, enriched_data: Dict[str, Any]) -> Optional[str]:
    """
    Check if a book with the same enriched metadata already exists for this user.
    
    Args:
        user_id: User ID
        enriched_data: Enriched book data from LLM
        
    Returns:
        Book ID if duplicate found, None otherwise
    """
    async with async_session() as session:
        # Get key fields for comparison
        title = enriched_data.get("title", "").strip().lower()
        author = enriched_data.get("author", "").strip().lower()
        isbn = enriched_data.get("isbn", "").strip()
        
        # Build query to find potential duplicates
        conditions = [
            UploadedBook.user_id == uuid.UUID(user_id)
        ]
        
        # Add title condition if available
        if title:
            conditions.append(func.lower(UploadedBook.title) == title)
        
        # Add author condition if available
        if author:
            conditions.append(func.lower(UploadedBook.author) == author)
        
        # Add ISBN condition if available
        if isbn:
            conditions.append(UploadedBook.isbn == isbn)
        
        # Must have at least title or author to check
        if not (title or author):
            return None
        
        query = select(UploadedBook).where(*conditions)
        result = await session.execute(query)
        existing_books = result.scalars().all()
        
        if existing_books:
            # Found potential duplicates - check if they're actually the same book
            for existing_book in existing_books:
                if is_same_book(enriched_data, existing_book):
                    logger.info(
                        f"Enriched duplicate found: {enriched_data.get('title')} by {enriched_data.get('author')}",
                        extra={
                            "user_id": user_id,
                            "existing_book_id": str(existing_book.id),
                            "duplicate_type": "enriched_match"
                        }
                    )
                    return str(existing_book.id)
        
        return None


def is_same_book(enriched_data: Dict[str, Any], existing_book: UploadedBook) -> bool:
    """
    Determine if two books are the same based on enriched metadata.
    
    Args:
        enriched_data: Enriched book data from LLM
        existing_book: Existing book record
        
    Returns:
        True if books are the same, False otherwise
    """
    # Normalize data for comparison
    enriched_title = enriched_data.get("title", "").strip().lower()
    enriched_author = enriched_data.get("author", "").strip().lower()
    enriched_isbn = enriched_data.get("isbn", "").strip()
    
    existing_title = existing_book.title.strip().lower() if existing_book.title else ""
    existing_author = existing_book.author.strip().lower() if existing_book.author else ""
    existing_isbn = existing_book.isbn.strip() if existing_book.isbn else ""
    
    # Check ISBN first (most reliable)
    if enriched_isbn and existing_isbn and enriched_isbn == existing_isbn:
        return True
    
    # Check title + author combination
    if (enriched_title and existing_title and enriched_title == existing_title and
        enriched_author and existing_author and enriched_author == existing_author):
        return True
    
    # Check if titles are very similar (handle typos)
    if enriched_title and existing_title:
        title_similarity = calculate_similarity(enriched_title, existing_title)
        if title_similarity > 0.9:  # 90% similarity threshold
            # If titles are very similar, check authors
            if (enriched_author and existing_author and 
                calculate_similarity(enriched_author, existing_author) > 0.8):
                return True
    
    return False


def calculate_similarity(str1: str, str2: str) -> float:
    """
    Calculate similarity between two strings using simple character-based comparison.
    
    Args:
        str1: First string
        str2: Second string
        
    Returns:
        Similarity score between 0 and 1
    """
    if not str1 or not str2:
        return 0.0
    
    # Convert to sets of characters for simple similarity
    set1 = set(str1.lower())
    set2 = set(str2.lower())
    
    if not set1 or not set2:
        return 0.0
    
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    
    return len(intersection) / len(union) if union else 0.0


async def store_books(user_id: str, books: List[Dict[str, Any]], request_id: str = None) -> List[str]:
    """Store books in database immediately and trigger async enrichment"""
    book_ids = []
    enrichment_requests = []

    async with async_session() as session:
        for book_data in books:
            # Check for exact duplicates
            is_exact_duplicate = await check_exact_duplicate(user_id, book_data.get("title"), book_data.get("author"))
            if is_exact_duplicate:
                logger.info(
                    f"Skipping exact duplicate book: {book_data.get('title')} by {book_data.get('author')}",
                    extra={
                        "user_id": user_id,
                        "book_title": book_data.get("title"),
                        "book_author": book_data.get("author")
                    }
                )
                continue

            # Check for enriched duplicates
            enriched_duplicate_id = await check_enriched_duplicate(user_id, book_data)
            if enriched_duplicate_id:
                logger.info(
                    f"Skipping enriched duplicate book: {book_data.get('title')} by {book_data.get('author')}",
                    extra={
                        "user_id": user_id,
                        "book_title": book_data.get("title"),
                        "book_author": book_data.get("author"),
                        "existing_book_id": enriched_duplicate_id
                    }
                )
                continue

            # Store book immediately with basic data
            book = UploadedBook(
                user_id=uuid.UUID(user_id),
                title=book_data.get("title"),
                author=book_data.get("author"),
                rating=book_data.get("rating"),
                notes=book_data.get("notes"),
                raw_payload=book_data,
                # Set default values - will be updated by async enrichment
                isbn=book_data.get("isbn"),
                genre=book_data.get("genre", "General"),
                reading_level=book_data.get("reading_level", 5.0),
                read_date=book_data.get("read_date"),
                confidence=0.0,  # Will be updated by enrichment
            )
            session.add(book)
            book_ids.append(str(book.id))
            
            # Prepare enrichment request for background processing
            if S.llm_service_enabled:
                enrichment_requests.append({
                    "book_id": str(book.id),
                    "book_data": book_data,
                    "request_id": request_id or str(uuid.uuid4())
                })

        await session.commit()
        logger.info(
            "Stored books for immediate access",
            extra={
                "user_id": user_id,
                "total_books": len(books),
                "enrichment_queued": len(enrichment_requests)
            }
        )

    # Trigger async enrichment for all books
    if enrichment_requests:
        try:
            # Create the async task but don't await it
            task = asyncio.create_task(process_enrichment_requests(enrichment_requests, user_id))
            
            # Add error handling for the task itself
            def handle_task_exception(task):
                try:
                    task.result()
                except Exception as e:
                    logger.error(
                        "Async enrichment task failed",
                        extra={
                            "user_id": user_id,
                            "total_requests": len(enrichment_requests),
                            "error": str(e)
                        },
                        exc_info=True
                    )
            
            task.add_done_callback(handle_task_exception)
            
        except Exception as e:
            logger.error(
                "Failed to start async enrichment task",
                extra={
                    "user_id": user_id,
                    "error": str(e)
                },
                exc_info=True
            )
            # Don't fail the upload if we can't start enrichment

    return book_ids


async def process_enrichment_requests(enrichment_requests: List[Dict], user_id: str):
    """Process enrichment requests asynchronously in the background with retry logic"""
    enriched_count = 0
    failed_count = 0
    no_change_count = 0
    
    logger.info(
        "Starting async enrichment processing",
        extra={
            "user_id": user_id,
            "total_requests": len(enrichment_requests),
            "max_attempts": S.max_enrichment_attempts
        }
    )
    
    for i, request in enumerate(enrichment_requests, 1):
        try:
            logger.debug(
                f"Processing enrichment request {i}/{len(enrichment_requests)}",
                extra={
                    "user_id": user_id,
                    "book_id": request["book_id"],
                    "book_title": request["book_data"].get("title", "Unknown")
                }
            )
            
            # Process enrichment with retry logic
            result = await process_single_book_enrichment(
                request["book_id"], 
                request["book_data"], 
                request["request_id"],
                user_id
            )
            
            if result == "enriched":
                enriched_count += 1
            elif result == "failed":
                failed_count += 1
            elif result == "no_change":
                no_change_count += 1
            elif result == "duplicate":
                no_change_count += 1 # Treat duplicate as a type of no_change for stats
                
        except Exception as e:
            logger.error(
                f"Unexpected error during async book enrichment: {e}",
                extra={
                    "user_id": user_id, 
                    "book_title": request["book_data"].get("title"),
                    "progress": f"{i}/{len(enrichment_requests)}"
                },
                exc_info=True
            )
            failed_count += 1
    
    logger.info(
        "Async enrichment processing completed",
        extra={
            "user_id": user_id,
            "total_requests": len(enrichment_requests),
            "enriched_count": enriched_count,
            "failed_count": failed_count,
            "no_change_count": no_change_count,
            "success_rate": enriched_count / len(enrichment_requests) if enrichment_requests else 0,
            "failure_rate": failed_count / len(enrichment_requests) if enrichment_requests else 0
        }
    )


async def process_single_book_enrichment(
    book_id: str, 
    book_data: Dict[str, Any], 
    request_id: str,
    user_id: str
) -> str:
    """
    Process enrichment for a single book with retry logic.
    
    Returns:
        "enriched" - if key fields were updated
        "failed" - if enrichment failed after max attempts
        "no_change" - if no key fields were updated but max attempts reached
    """
    async with async_session() as session:
        try:
            book = await session.get(UploadedBook, uuid.UUID(book_id))
            if not book:
                logger.warning(f"Book not found for enrichment: {book_id}")
                return "failed"
            
            # Check if we should attempt enrichment
            if book.enrichment_status == "enriched":
                logger.debug(f"Book already enriched: {book_id}")
                return "enriched"
            
            if book.enrichment_attempts >= S.max_enrichment_attempts:
                logger.debug(f"Book at max attempts ({book.enrichment_attempts}): {book_id}")
                return "no_change"
            
            # Mark as in progress
            book.enrichment_status = "in_progress"
            await session.commit()
            
            # Attempt enrichment with exponential backoff
            for attempt in range(book.enrichment_attempts + 1, S.max_enrichment_attempts + 1):
                try:
                    logger.debug(
                        f"Enrichment attempt {attempt}/{S.max_enrichment_attempts} for book {book_id}",
                        extra={
                            "user_id": user_id,
                            "book_id": book_id,
                            "attempt": attempt,
                            "max_attempts": S.max_enrichment_attempts
                        }
                    )
                    
                    # Add timeout to prevent hanging enrichment calls
                    try:
                        enriched_data = await asyncio.wait_for(
                            enrich_book_metadata(book_data, request_id),
                            timeout=S.enrichment_timeout  # Configurable timeout
                        )
                    except asyncio.TimeoutError:
                        logger.warning(
                            f"Enrichment timeout for book '{book_data.get('title', 'Unknown')}' (attempt {attempt})",
                            extra={
                                "user_id": user_id,
                                "book_id": book_id,
                                "attempt": attempt
                            }
                        )
                        # Continue to next attempt
                        continue
                    
                    # Update book with enriched data
                    key_field_updated = await update_book_with_enrichment(book, enriched_data, attempt)
                    
                    # After enrichment, check if this book is a duplicate of an existing enriched book
                    duplicate_book_id = await check_enriched_duplicate(user_id, enriched_data)
                    if duplicate_book_id and duplicate_book_id != str(book.id):
                        # This book is a duplicate of another book - mark it as duplicate and skip
                        book.enrichment_status = "duplicate"
                        book.enrichment_notes = f"Duplicate of book {duplicate_book_id}"
                        await session.commit()
                        
                        logger.info(
                            f"Book marked as duplicate after enrichment: {enriched_data.get('title', 'Unknown')}",
                            extra={
                                "user_id": user_id,
                                "book_id": book_id,
                                "duplicate_of": duplicate_book_id,
                                "attempt": attempt
                            }
                        )
                        return "duplicate"
                    
                    if key_field_updated:
                        # Success - key field was updated
                        book.enrichment_status = "enriched"
                        book.enrichment_attempts = attempt
                        await session.commit()
                        
                        logger.info(
                            f"Book enriched successfully on attempt {attempt}: {enriched_data.get('title', 'Unknown')}",
                            extra={
                                "user_id": user_id,
                                "book_id": book_id,
                                "attempt": attempt,
                                "enriched_fields": get_enriched_fields(book, enriched_data)
                            }
                        )
                        return "enriched"
                    else:
                        # No key fields updated, but enrichment completed
                        logger.info(
                            f"Enrichment completed but no key fields updated (attempt {attempt}): {book_data.get('title', 'Unknown')}",
                            extra={
                                "user_id": user_id,
                                "book_id": book_id,
                                "attempt": attempt
                            }
                        )
                        
                        # If this was the last attempt, mark as no change
                        if attempt >= S.max_enrichment_attempts:
                            book.enrichment_status = "max_attempts_reached"
                            book.enrichment_attempts = attempt
                            await session.commit()
                            return "no_change"
                        
                        # Otherwise, continue to next attempt after delay
                        delay = min(
                            S.enrichment_retry_delay_base * (2 ** (attempt - 1)),
                            S.enrichment_retry_delay_max
                        )
                        await asyncio.sleep(delay)
                        
                except LLMServiceError as e:
                    logger.warning(
                        f"LLM service error on attempt {attempt}: {e}",
                        extra={
                            "user_id": user_id,
                            "book_id": book_id,
                            "attempt": attempt
                        }
                    )
                    # Continue to next attempt
                    continue
                except Exception as e:
                    logger.error(
                        f"Unexpected error on attempt {attempt}: {e}",
                        extra={
                            "user_id": user_id,
                            "book_id": book_id,
                            "attempt": attempt
                        },
                        exc_info=True
                    )
                    # Continue to next attempt
                    continue
            
            # If we get here, all attempts failed
            book.enrichment_status = "failed"
            book.enrichment_attempts = S.max_enrichment_attempts
            await session.commit()
            
            logger.error(
                f"All enrichment attempts failed for book: {book_data.get('title', 'Unknown')}",
                extra={
                    "user_id": user_id,
                    "book_id": book_id,
                    "total_attempts": S.max_enrichment_attempts
                }
            )
            return "failed"
            
        except Exception as e:
            logger.error(
                f"Database error during enrichment: {e}",
                extra={
                    "user_id": user_id,
                    "book_id": book_id
                },
                exc_info=True
            )
            return "failed"


async def update_book_with_enrichment(book: UploadedBook, enriched_data: Dict[str, Any], attempt: int) -> bool:
    """
    Update book with enriched data and return True if any key field was updated.
    
    Args:
        book: The book record to update
        enriched_data: Enriched data from LLM
        attempt: Current attempt number
        
    Returns:
        True if any key field (isbn, genre, reading_level, page_count) was updated
    """
    key_field_updated = False
    
    # Update key fields only if they changed
    if enriched_data.get("isbn") and enriched_data.get("isbn") != book.isbn:
        book.isbn = enriched_data.get("isbn")
        key_field_updated = True
        
    if enriched_data.get("genre") and enriched_data.get("genre") != book.genre:
        book.genre = enriched_data.get("genre")
        key_field_updated = True
        
    if enriched_data.get("reading_level") and enriched_data.get("reading_level") != book.reading_level:
        book.reading_level = enriched_data.get("reading_level")
        key_field_updated = True
        
    if enriched_data.get("page_count") and enriched_data.get("page_count") != getattr(book, "page_count", None):
        book.page_count = enriched_data.get("page_count")
        key_field_updated = True
    
    # Update non-key fields
    book.read_date = enriched_data.get("read_date") or book.read_date
    book.confidence = enriched_data.get("confidence", 0.0)
    book.enrichment_notes = enriched_data.get("notes")
    book.raw_payload = enriched_data
    
    # Update enrichment metadata
    book.enrichment_attempts = attempt
    if key_field_updated:
        book.llm_enriched = True
        book.enrichment_timestamp = enriched_data.get("enrichment_timestamp")
    
    return key_field_updated


def get_enriched_fields(book: UploadedBook, enriched_data: Dict[str, Any]) -> List[str]:
    """Get list of fields that were actually enriched."""
    enriched_fields = []
    
    if enriched_data.get("isbn") and enriched_data.get("isbn") != book.isbn:
        enriched_fields.append("isbn")
    if enriched_data.get("genre") and enriched_data.get("genre") != book.genre:
        enriched_fields.append("genre")
    if enriched_data.get("reading_level") and enriched_data.get("reading_level") != book.reading_level:
        enriched_fields.append("reading_level")
    if enriched_data.get("page_count") and enriched_data.get("page_count") != getattr(book, "page_count", None):
        enriched_fields.append("page_count")
    
    return enriched_fields


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

        if len(request.books) > 20:
            raise HTTPException(status_code=400, detail="You can only upload up to 20 books at a time.")

        # Store books with enrichment
        upload_id = str(uuid.uuid4())
        book_ids = await store_books(user_id, books_data, upload_id)

        # Publish event
        event = UserUploadedEvent(
            user_hash_id=user_hash_id, book_count=len(books_data), book_ids=book_ids
        )
        await publish_event(USER_UPLOADED_TOPIC, event.model_dump())

        return UploadResponse(
            message="Books uploaded successfully! Metadata enrichment is happening in the background.",
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
        if len(content) > 100 * 1024:
            raise HTTPException(status_code=413, detail="File too large (max 100KB)")
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
            message="Books uploaded successfully! Metadata enrichment is happening in the background.",
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


@app.get("/enrichment/status")
async def get_enrichment_status():
    """Get enrichment status statistics"""
    try:
        async with async_session() as session:
            # Get enrichment statistics
            stats_query = """
            SELECT 
                enrichment_status,
                COUNT(*) as count,
                AVG(enrichment_attempts) as avg_attempts,
                MAX(enrichment_attempts) as max_attempts
            FROM uploaded_books 
            GROUP BY enrichment_status
            """
            
            result = await session.execute(text(stats_query))
            stats = result.fetchall()
            
            # Get books that could be retried (below current max)
            retryable_query = """
            SELECT COUNT(*) as count
            FROM uploaded_books 
            WHERE enrichment_status IN ('failed', 'max_attempts_reached')
            AND enrichment_attempts < :max_attempts
            """
            
            result = await session.execute(text(retryable_query), {"max_attempts": S.max_enrichment_attempts})
            retryable_count = result.scalar()
            
            return {
                "enrichment_stats": [
                    {
                        "status": row.enrichment_status,
                        "count": row.count,
                        "avg_attempts": float(row.avg_attempts) if row.avg_attempts else 0,
                        "max_attempts": row.max_attempts
                    }
                    for row in stats
                ],
                "retryable_books": retryable_count,
                "current_max_attempts": S.max_enrichment_attempts,
                "retry_delay_base": S.enrichment_retry_delay_base,
                "retry_delay_max": S.enrichment_retry_delay_max
            }
            
    except Exception as e:
        logger.error("Failed to get enrichment status", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/enrichment/retry")
async def retry_failed_enrichments():
    """Retry enrichment for books that are below the current max attempts limit"""
    try:
        async with async_session() as session:
            # Find books that could be retried
            retryable_query = """
            SELECT id, title, author, enrichment_attempts, enrichment_status
            FROM uploaded_books 
            WHERE enrichment_status IN ('failed', 'max_attempts_reached')
            AND enrichment_attempts < :max_attempts
            ORDER BY enrichment_attempts DESC
            LIMIT 100
            """
            
            result = await session.execute(text(retryable_query), {"max_attempts": S.max_enrichment_attempts})
            retryable_books = result.fetchall()
            
            if not retryable_books:
                return {
                    "message": "No books available for retry",
                    "retried_count": 0
                }
            
            # Reset status for retry
            book_ids = [str(book.id) for book in retryable_books]
            reset_query = """
            UPDATE uploaded_books 
            SET enrichment_status = 'pending'
            WHERE id = ANY(:book_ids)
            """
            
            await session.execute(text(reset_query), {"book_ids": book_ids})
            await session.commit()
            
            logger.info(
                f"Reset enrichment status for {len(retryable_books)} books for retry",
                extra={
                    "book_count": len(retryable_books),
                    "book_ids": book_ids[:5]  # Log first 5 for debugging
                }
            )
            
            return {
                "message": f"Reset enrichment status for {len(retryable_books)} books",
                "retried_count": len(retryable_books),
                "book_ids": book_ids
            }
            
    except Exception as e:
        logger.error("Failed to retry failed enrichments", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/enrichment/cleanup-duplicates")
async def cleanup_duplicate_books():
    """Remove books marked as duplicates"""
    try:
        async with async_session() as session:
            # Find books marked as duplicates
            duplicate_query = """
            SELECT id, title, author, enrichment_notes
            FROM uploaded_books 
            WHERE enrichment_status = 'duplicate'
            ORDER BY created_at DESC
            """
            
            result = await session.execute(text(duplicate_query))
            duplicate_books = result.fetchall()
            
            if not duplicate_books:
                return {
                    "message": "No duplicate books found",
                    "removed_count": 0
                }
            
            # Delete duplicate books
            book_ids = [str(book.id) for book in duplicate_books]
            delete_query = """
            DELETE FROM uploaded_books 
            WHERE enrichment_status = 'duplicate'
            """
            
            await session.execute(text(delete_query))
            await session.commit()
            
            logger.info(
                f"Removed {len(duplicate_books)} duplicate books",
                extra={
                    "removed_count": len(duplicate_books),
                    "book_ids": book_ids[:5]  # Log first 5 for debugging
                }
            )
            
            return {
                "message": f"Removed {len(duplicate_books)} duplicate books",
                "removed_count": len(duplicate_books),
                "book_ids": book_ids
            }
            
    except Exception as e:
        logger.error("Failed to cleanup duplicate books", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=S.user_ingest_port)
