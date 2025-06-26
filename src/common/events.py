"""
Shared event schemas for Kafka messaging between services.
"""
from datetime import datetime, UTC
from typing import List, Optional, Literal
from pydantic import BaseModel, Field


# Base model with ISO datetime serialization
class _BaseEvent(BaseModel):
    model_config = {
        "ser_json_timedelta": "iso8601",
        "ser_json_bytes": "utf8",
    }
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


class BookAddedEvent(_BaseEvent):
    """Event published when new books are added to the catalog."""
    event_type: Literal["books_added"] = "books_added"
    count: int = Field(..., description="Number of books added")
    book_ids: Optional[List[str]] = Field(None, description="List of book IDs added")
    source: str = Field("ingestion_service", description="Service that triggered the event")


class GraphRefreshEvent(_BaseEvent):
    """Event published when graph refresh is triggered."""
    event_type: Literal["graph_refresh_triggered"] = "graph_refresh_triggered"
    reason: str = Field(..., description="Reason for refresh (e.g., 'books_added', 'manual')")
    trigger_count: Optional[int] = Field(None, description="Number of events that triggered this refresh")


class StudentAddedEvent(_BaseEvent):
    event_type: Literal["student_added"] = "student_added"
    student_id: str
    payload: dict | None = None
    source: str = Field("ingestion_service")


class StudentUpdatedEvent(_BaseEvent):
    event_type: Literal["student_updated"] = "student_updated"
    student_id: str
    payload: dict | None = None
    source: str = Field("ingestion_service")


class CheckoutAddedEvent(_BaseEvent):
    event_type: Literal["checkout_added"] = "checkout_added"
    student_id: str
    book_id: str
    checkout_date: str
    source: str = Field("ingestion_service")


class StudentProfileChangedEvent(_BaseEvent):
    event_type: Literal["student_profile_changed"] = "student_profile_changed"
    student_id: str
    source: str = Field("student_profile_worker")


class StudentEmbeddingChangedEvent(_BaseEvent):
    event_type: Literal["student_embedding_changed"] = "student_embedding_changed"
    student_id: str
    source: str = Field("student_embedding_worker")


class BookUpdatedEvent(_BaseEvent):
    event_type: Literal["book_updated"] = "book_updated"
    book_id: str
    payload: dict | None = None
    source: str = Field("book_enrichment_worker")


class BookDeletedEvent(_BaseEvent):
    event_type: Literal["book_deleted"] = "book_deleted"
    book_id: str
    source: str = Field("ingestion_service")


# Batch variant for initial ingest
class StudentsAddedEvent(_BaseEvent):
    """Event published when multiple students are added in bulk (e.g., initial ingestion)."""

    event_type: Literal["students_added"] = "students_added"
    count: int
    source: str = Field("ingestion_service")


# Topic names
BOOK_EVENTS_TOPIC = "book_events"
GRAPH_EVENTS_TOPIC = "graph_events"
STUDENT_EVENTS_TOPIC = "student_events"
CHECKOUT_EVENTS_TOPIC = "checkout_events"
STUDENT_PROFILE_TOPIC = "student_profile_events"
STUDENT_EMBEDDING_TOPIC = "student_embedding_events" 