"""
Test suite for Feedback Worker.
"""
import pytest
import json
from unittest.mock import Mock, patch
from datetime import datetime

from src.common.events import FeedbackEvent


@pytest.fixture
def sample_feedback_event():
    """Sample feedback event for testing."""
    return FeedbackEvent(
        user_hash_id="test_hash_123",
        book_id="1",
        score=1
    )


def test_feedback_event_creation(sample_feedback_event):
    """Test that FeedbackEvent can be created properly."""
    assert sample_feedback_event.user_hash_id == "test_hash_123"
    assert sample_feedback_event.book_id == "1"
    assert sample_feedback_event.score == 1
    assert isinstance(sample_feedback_event.timestamp, datetime)


def test_feedback_event_serialization():
    """Test that FeedbackEvent can be properly serialized/deserialized."""
    event = FeedbackEvent(
        user_hash_id="test_hash_123",
        book_id="1",
        score=1
    )
    
    # Test serialization
    serialized = json.dumps(event.model_dump(), default=str)
    assert isinstance(serialized, str)
    
    # Test deserialization
    data = json.loads(serialized)
    assert data["user_hash_id"] == "test_hash_123"
    assert data["book_id"] == "1"
    assert data["score"] == 1


def test_feedback_event_validation():
    """Test that FeedbackEvent validates inputs properly."""
    # Valid positive feedback
    event1 = FeedbackEvent(
        user_hash_id="test_hash_123",
        book_id="1",
        score=1
    )
    assert event1.score == 1
    
    # Valid negative feedback
    event2 = FeedbackEvent(
        user_hash_id="test_hash_123",
        book_id="1",
        score=-1
    )
    assert event2.score == -1


def test_feedback_event_defaults():
    """Test that FeedbackEvent has proper default values."""
    event = FeedbackEvent(
        user_hash_id="test_hash_123",
        book_id="1",
        score=1
    )
    
    assert event.event_type == "feedback_received"
    assert event.source == "feedback_worker"
    assert event.timestamp is not None


def test_feedback_integration_concept():
    """Test the concept of feedback processing without actual implementation."""
    # This test verifies the event structure is correct for integration
    event = FeedbackEvent(
        user_hash_id="test_hash_123",
        book_id="1",
        score=1
    )
    
    # Verify the event has all required fields for database storage
    assert hasattr(event, 'user_hash_id')
    assert hasattr(event, 'book_id')
    assert hasattr(event, 'score')
    assert hasattr(event, 'timestamp')
    
    # Verify the event can be converted to dict for processing
    event_dict = event.model_dump()
    assert 'user_hash_id' in event_dict
    assert 'book_id' in event_dict
    assert 'score' in event_dict 