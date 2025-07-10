from typing import List, Optional
from datetime import date
from pydantic import BaseModel, Field, conint, field_validator, ConfigDict
from sqlalchemy import Column, String, Integer, Float, Date, Text, JSON, TIMESTAMP, UUID, SmallInteger, ForeignKey, Index
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import text
import uuid

Base = declarative_base()

# ====================================================================
# EXISTING SCHEMA MODELS (from 00_init_schema.sql)
# ====================================================================

class Student(Base):
    """Core student demographics and academic data"""
    __tablename__ = "students"
    
    student_id = Column(String, primary_key=True)
    grade_level = Column(Integer)
    age = Column(Integer)
    homeroom_teacher = Column(String)
    prior_year_reading_score = Column(Integer)
    lunch_period = Column(String)

class Catalog(Base):
    """Book catalog with metadata and reading difficulty metrics"""
    __tablename__ = "catalog"
    
    book_id = Column(String, primary_key=True)
    isbn = Column(String)
    title = Column(String)
    author = Column(String)
    genre = Column(String)
    keywords = Column(String)
    description = Column(String)
    page_count = Column(Integer)
    publication_year = Column(Integer)
    difficulty_band = Column(String)
    reading_level = Column(Float)  # NUMERIC(4,2)
    average_student_rating = Column(Float)  # NUMERIC(3,2)

class Checkout(Base):
    """Student book checkout/return history with ratings"""
    __tablename__ = "checkout"
    
    student_id = Column(String, ForeignKey('students.student_id'), primary_key=True)
    book_id = Column(String, ForeignKey('catalog.book_id'), primary_key=True)
    checkout_date = Column(Date, primary_key=True)
    return_date = Column(Date)
    student_rating = Column(Integer)
    checkout_id = Column(String)

class StudentEmbedding(Base):
    """ML embeddings for student preference modeling"""
    __tablename__ = "student_embeddings"
    
    student_id = Column(String, ForeignKey('students.student_id'), primary_key=True)
    vec = Column(Text)  # VECTOR(1536) - will be handled by raw SQL
    last_event = Column(UUID(as_uuid=True))

class BookEmbedding(Base):
    """ML embeddings for semantic book similarity"""
    __tablename__ = "book_embeddings"
    
    book_id = Column(String, ForeignKey('catalog.book_id'), primary_key=True)
    vec = Column(Text)  # VECTOR(1536) - will be handled by raw SQL
    last_event = Column(UUID(as_uuid=True))

class StudentSimilarity(Base):
    """Precomputed student similarity matrix for collaborative filtering"""
    __tablename__ = "student_similarity"
    
    a = Column(String, ForeignKey('students.student_id'), primary_key=True)
    b = Column(String, ForeignKey('students.student_id'), primary_key=True)
    sim = Column(Float)
    last_event = Column(UUID(as_uuid=True))

class StudentProfileCache(Base):
    """Cached student reading profiles for performance"""
    __tablename__ = "student_profile_cache"
    
    student_id = Column(String, ForeignKey('students.student_id'), primary_key=True)
    histogram = Column(JSONB)
    last_event = Column(UUID(as_uuid=True))

class RecommendationHistory(Base):
    """Recommendation history for deduplication and tracking"""
    __tablename__ = "recommendation_history"
    
    student_id = Column(String, ForeignKey('students.student_id'), primary_key=True)
    book_id = Column(String, ForeignKey('catalog.book_id'), primary_key=True)
    recommended_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    justification = Column(Text)

# ====================================================================
# READER MODE MODELS (NEW)
# ====================================================================

class PublicUser(Base):
    """Reader Mode users - anonymous public users who upload book lists"""
    __tablename__ = "public_users"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    hash_id = Column(String, unique=True, nullable=False)  # SHA256 hash of identifier
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

class UploadedBook(Base):
    """Books uploaded by Reader Mode users"""
    __tablename__ = "uploaded_books"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey('public_users.id'), nullable=False)
    title = Column(Text)
    author = Column(Text)
    rating = Column(SmallInteger)  # 1-5 user rating
    notes = Column(Text)
    raw_payload = Column(JSON)  # Original upload data
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

class Feedback(Base):
    """Reader Mode feedback on recommendations"""
    __tablename__ = "feedback"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey('public_users.id'), nullable=False)
    book_id = Column(String, ForeignKey('catalog.book_id'), nullable=False)
    score = Column(SmallInteger, nullable=False)  # +1 (thumbs up) or -1 (thumbs down)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

# ====================================================================
# INDEXES (defined declaratively)
# ====================================================================

# Performance indexes
Index('idx_checkout_student_id', Checkout.student_id)
Index('idx_checkout_book_id', Checkout.book_id)
Index('idx_checkout_checkout_id', Checkout.checkout_id)
Index('idx_catalog_reading_level', Catalog.reading_level)
Index('idx_catalog_genre', Catalog.genre)
Index('idx_catalog_rating', Catalog.average_student_rating)
Index('idx_students_grade', Student.grade_level)
Index('idx_students_teacher', Student.homeroom_teacher)
Index('idx_similarity_score', StudentSimilarity.sim.desc())

# Vector indexes will be created manually as they require special syntax 

# --------------------------------------------------------------------
# DATA INGESTION / VALIDATION MODELS (Pydantic)
# --------------------------------------------------------------------

# NOTE: These lightweight models were removed during the ORM refactor but are
# still consumed by the ingestion pipeline, test factories, and several worker
# services. Re-introducing them here maintains backward-compat without touching
# call-sites. They intentionally avoid any ORMs or heavy dependencies.

import json as _json  # stdlib – keep alias to avoid clashing with sqlalchemy.JSON

class _RecordModel(BaseModel):
    """Shared config for record models."""

    model_config = ConfigDict(str_strip_whitespace=True, populate_by_name=True)

    @staticmethod
    def _ensure_list(value):
        """Coerce *value* into a list[str].

        Accepts:
        * list[str]               – returned as-is
        * JSON-encoded string     – decoded via json.loads
        * plain string            – wrapped in single-element list
        * None / empty string     – returns []
        """
        if value in (None, ""):
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            try:
                parsed = _json.loads(value)
                if isinstance(parsed, list):
                    return parsed
            except Exception:
                pass
            return [value]
        # Fallback – best-effort cast
        return [str(value)]


class BookCatalogItem(_RecordModel):
    book_id: str
    isbn: str
    title: str
    author: Optional[str] = None
    genre: list[str] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    description: Optional[str] = None
    page_count: Optional[int] = None
    publication_year: Optional[int] = None
    difficulty_band: Optional[str] = None
    reading_level: Optional[float] = None
    average_student_rating: Optional[float] = None

    # ----------------------------- validators ----------------------------

    @field_validator("genre", mode="before")
    @classmethod
    def _validate_genre(cls, v):
        return cls._ensure_list(v)

    @field_validator("keywords", mode="before")
    @classmethod
    def _validate_keywords(cls, v):
        return cls._ensure_list(v)


class StudentRecord(_RecordModel):
    student_id: str
    grade_level: int
    age: int
    homeroom_teacher: str
    prior_year_reading_score: Optional[float] = None
    lunch_period: int | str  # persisted as string in CSV – accept either

    @field_validator("lunch_period", mode="before")
    @classmethod
    def _coerce_lunch(cls, v):
        # Accept ints, numeric strings, or enum-like strings – return as int
        try:
            return int(v)
        except (TypeError, ValueError):
            # Leave as-is to surface validation error later if truly invalid
            return v

    @field_validator("prior_year_reading_score", mode="before")
    @classmethod
    def _coerce_prior_score(cls, v):
        if v in (None, "", "null", "NaN"):
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return v


class CheckoutRecord(_RecordModel):
    student_id: str
    book_id: str
    checkout_date: date
    return_date: Optional[date] = None
    student_rating: Optional[int] = Field(None, ge=1, le=5)
    checkout_id: str | None = None

    # ----------------------------- validators ----------------------------

    @field_validator("student_rating", mode="before")
    @classmethod
    def _coerce_rating(cls, v):
        """Accept int, float, str, or missing. Return int or None."""
        if v in (None, "", "null", "NaN"):
            return None
        try:
            return int(float(v))
        except (TypeError, ValueError):
            # Let Pydantic raise proper ValidationError later
            return v

    @field_validator("checkout_id", mode="after")
    @classmethod
    def _default_checkout_id(cls, v):
        return v or str(uuid.uuid4()) 