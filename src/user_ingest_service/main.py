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

logger = get_logger(__name__)

# Database setup
engine = create_async_engine(
    str(S.db_url).replace("postgresql://", "postgresql+asyncpg://"),
    echo=False
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
    lifespan=lifespan
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
    if len(content.encode('utf-8')) > 1024 * 1024:  # 1MB limit
        raise HTTPException(status_code=413, detail="File too large (max 1MB)")
    
    reader = csv.DictReader(io.StringIO(content))
    rows = list(reader)
    
    if len(rows) > 100:
        raise HTTPException(status_code=413, detail="Too many rows (max 100)")
    
    if len(rows) == 0:
        raise HTTPException(status_code=400, detail="No data rows found")
    
    # Validate required columns
    required_columns = {'title'}
    if not required_columns.issubset(set(reader.fieldnames or [])):
        raise HTTPException(
            status_code=400, 
            detail=f"Missing required columns: {required_columns - set(reader.fieldnames or [])}"
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

async def store_books(user_id: str, books: List[Dict[str, Any]]) -> List[str]:
    """Store books in database and return book IDs"""
    book_ids = []
    
    async with async_session() as session:
        for book_data in books:
            # Create book record
            book = UploadedBook(
                user_id=uuid.UUID(user_id),
                title=book_data.get('title'),
                author=book_data.get('author'),
                rating=book_data.get('rating'),
                notes=book_data.get('notes'),
                raw_payload=book_data
            )
            session.add(book)
            book_ids.append(str(book.id))
        
        await session.commit()
        logger.info("Stored books", extra={"user_id": user_id, "count": len(books)})
    
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
        
        # Store books
        book_ids = await store_books(user_id, books_data)
        
        # Publish event
        event = UserUploadedEvent(
            user_hash_id=user_hash_id,
            book_count=len(books_data),
            book_ids=book_ids
        )
        await publish_event(USER_UPLOADED_TOPIC, event.model_dump())
        
        upload_id = str(uuid.uuid4())
        
        return UploadResponse(
            message="Books uploaded successfully",
            user_hash_id=user_hash_id,
            books_processed=len(books_data),
            upload_id=upload_id
        )
        
    except Exception as e:
        logger.error("Failed to process JSON upload", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/upload_books_csv", response_model=UploadResponse)
async def upload_books_csv(
    file: UploadFile = File(...),
    user_identifier: str = Form(...)
):
    """Upload books via CSV file"""
    try:
        # Validate file type
        if not file.filename or not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="Only CSV files are allowed")
        
        # Read and validate content
        content = await file.read()
        try:
            content_str = content.decode('utf-8')
        except UnicodeDecodeError:
            raise HTTPException(status_code=400, detail="File must be UTF-8 encoded")
        
        # Parse and validate CSV
        books_data = validate_csv_content(content_str)
        
        # Hash user identifier
        user_hash_id = hash_user_identifier(user_identifier)
        
        # Upsert user
        user_id = await upsert_user(user_hash_id)
        
        # Store books
        book_ids = await store_books(user_id, books_data)
        
        # Publish event
        event = UserUploadedEvent(
            user_hash_id=user_hash_id,
            book_count=len(books_data),
            book_ids=book_ids
        )
        await publish_event(USER_UPLOADED_TOPIC, event.dict())
        
        upload_id = str(uuid.uuid4())
        
        return UploadResponse(
            message="Books uploaded successfully",
            user_hash_id=user_hash_id,
            books_processed=len(books_data),
            upload_id=upload_id
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004) 