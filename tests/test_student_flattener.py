"""Tests for StudentFlattener Phase 2 enhancements."""
import pytest
from embedding.student import StudentFlattener


def test_student_flattener_basic():
    """Test basic student flattening functionality."""
    row = {
        "student_id": "S001",
        "grade_level": 4,
        "prior_year_reading_score": 3.5
    }
    text, meta = StudentFlattener()(row)
    
    assert "Grade 4 student with id S001" in text
    assert "reading-level-3.5" in text
    assert meta["student_id"] == "S001"


def test_student_flattener_phase2_homeroom_lunch():
    """Test Phase 2 enhancement: homeroom_teacher and lunch_period tokens."""
    row = {
        "student_id": "S001", 
        "grade_level": 4,
        "homeroom_teacher": "Ms. Patel",
        "lunch_period": 1,
        "prior_year_reading_score": 3.4
    }
    text, meta = StudentFlattener()(row)
    
    # Check core functionality
    assert "Grade 4 student with id S001" in text
    
    # Check Phase 2 enhancements
    assert "teacher-patel" in text, f"Expected homeroom token in: {text}"
    assert "lunch-1" in text, f"Expected lunch token in: {text}"
    assert "reading-level-3.4" in text
    
    # Check metadata
    assert meta["student_id"] == "S001"
    # The metadata contains the original fields, not separate token fields
    assert "homeroom_teacher" in meta
    assert "lunch_period" in meta


def test_student_flattener_missing_homeroom_lunch():
    """Test behavior when homeroom/lunch data is missing."""
    row = {
        "student_id": "S002",
        "grade_level": 3,
        "prior_year_reading_score": 2.8
    }
    text, meta = StudentFlattener()(row)
    
    assert "Grade 3 student with id S002" in text
    assert "reading-level-2.8" in text
    # Should not contain tokens for missing data
    assert "teacher-" not in text
    assert "lunch-" not in text


def test_student_flattener_special_characters_in_homeroom():
    """Test homeroom teacher name normalization."""
    row = {
        "student_id": "S003",
        "grade_level": 5,
        "homeroom_teacher": "Mrs. O'Brien-Smith",
        "lunch_period": 2
    }
    text, meta = StudentFlattener()(row)
    
    # Should normalize special characters (preserves apostrophes and periods)
    assert "teacher-mrs.-o'brien-smith" in text.lower()
    assert "lunch-2" in text


def test_student_flattener_edge_cases():
    """Test edge cases like missing reading score."""
    row = {
        "student_id": "S004",
        "grade_level": 4,
        "homeroom_teacher": "Mr. Johnson",
        "lunch_period": 3
        # No prior_year_reading_score
    }
    text, meta = StudentFlattener()(row)
    
    assert "Grade 4 student with id S004" in text
    assert "teacher-johnson" in text
    assert "lunch-3" in text
    # Should handle missing reading score gracefully


def test_student_flattener_preserves_existing_functionality():
    """Ensure Phase 2 doesn't break existing functionality."""
    # Test with minimal data (original functionality)
    row = {"student_id": "S005", "grade_level": 2}
    text, meta = StudentFlattener()(row)
    
    assert "Grade 2 student with id S005" in text
    assert meta["student_id"] == "S005" 