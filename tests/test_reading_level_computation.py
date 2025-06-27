#!/usr/bin/env python3
"""
Tests for reading level computation functionality.

Tests the core business logic for computing student reading levels,
which was refactored from the MCP server into common/reading_level_utils.py.
"""
import pytest
from unittest.mock import AsyncMock, patch

from src.common.reading_level_utils import (
    compute_student_reading_level,
    get_student_reading_level_from_db
)

class TestReadingLevelComputation:
    """Test suite for reading level computation logic."""
    
    def test_compute_reading_level_with_checkout_history(self):
        """Test primary method: compute from recent checkout history."""
        # Mock checkout history data
        recent_checkouts = [
            {"reading_level": 4.2},
            {"reading_level": 4.0}, 
            {"reading_level": 4.5},
            {"reading_level": 3.8},
            {"reading_level": 4.1}
        ]
        
        result = compute_student_reading_level(
            checkout_rows=recent_checkouts,
            student_grade=4,
            eog_score=3
        )
        
        # Should average the reading levels
        expected_avg = (4.2 + 4.0 + 4.5 + 3.8 + 4.1) / 5  # 4.12
        assert abs(result["avg_reading_level"] - expected_avg) < 0.1
        assert result["confidence"] == 1.0  # 5 books = max confidence
        assert result["method"] == "checkout_history"
        assert result["books_used"] == 5

    def test_compute_reading_level_eog_fallback_score_3(self):
        """Test EOG fallback method with score 3 (at grade level)."""
        recent_checkouts = []  # No checkout history
        
        result = compute_student_reading_level(
            checkout_rows=recent_checkouts,
            student_grade=4,
            eog_score=3
        )
        
        # EOG score 3 = at grade level (no adjustment)
        assert result["avg_reading_level"] == 4.0
        assert result["confidence"] == 0.3
        assert result["method"] == "eog_fallback"
        assert result["grade_adjustment"] == 0

    def test_compute_reading_level_eog_fallback_score_1(self):
        """Test EOG fallback method with score 1 (below grade level)."""
        recent_checkouts = []
        
        result = compute_student_reading_level(
            checkout_rows=recent_checkouts,
            student_grade=5,
            eog_score=1
        )
        
        # EOG score 1 = grade level - 2
        assert result["avg_reading_level"] == 3.0  # 5 - 2
        assert result["confidence"] == 0.3
        assert result["grade_adjustment"] == -2

    def test_compute_reading_level_eog_fallback_score_5(self):
        """Test EOG fallback method with score 5 (above grade level)."""
        recent_checkouts = []
        
        result = compute_student_reading_level(
            checkout_rows=recent_checkouts,
            student_grade=3,
            eog_score=5
        )
        
        # EOG score 5 = grade level + 2
        assert result["avg_reading_level"] == 5.0  # 3 + 2
        assert result["confidence"] == 0.3
        assert result["grade_adjustment"] == 2

    def test_compute_reading_level_ultimate_fallback(self):
        """Test ultimate fallback when no data is available."""
        recent_checkouts = []
        
        result = compute_student_reading_level(
            checkout_rows=recent_checkouts,
            student_grade=4,
            eog_score=None
        )
        
        # Should use grade 4 default
        assert result["avg_reading_level"] == 4.0
        assert result["confidence"] == 0.3  # EOG fallback with default score 3
        assert result["method"] == "eog_fallback"

    def test_compute_reading_level_safety_check_negative(self):
        """Test that negative reading levels are prevented."""
        recent_checkouts = []
        
        result = compute_student_reading_level(
            checkout_rows=recent_checkouts,
            student_grade=1,
            eog_score=1
        )
        
        # Would be 1 - 2 = -1, but should be clamped to 0.5
        assert result["avg_reading_level"] == 0.5
        assert result["confidence"] == 0.3
        assert result["method"] == "eog_fallback"

    def test_compute_reading_level_mixed_confidence_levels(self):
        """Test confidence scaling based on book count."""
        test_cases = [
            ([{"reading_level": 3.0}], 0.2),  # 1 book = 0.2 confidence  
            ([{"reading_level": 3.0}] * 2, 0.4),  # 2 books = 0.4 confidence
            ([{"reading_level": 3.0}] * 3, 0.6),  # 3 books = 0.6 confidence
            ([{"reading_level": 3.0}] * 5, 1.0),  # 5+ books = 1.0 confidence
            ([{"reading_level": 3.0}] * 10, 1.0), # 10 books = still 1.0 confidence
        ]
        
        for checkouts, expected_confidence in test_cases:
            result = compute_student_reading_level(
                checkout_rows=checkouts,
                student_grade=3,
                eog_score=3
            )
            assert result["confidence"] == expected_confidence
            assert result["method"] == "checkout_history"

    @pytest.mark.asyncio
    async def test_get_student_reading_level_from_db_success(self):
        """Test database wrapper function with successful query."""
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        # Mock recent checkouts query
        mock_conn.fetch.return_value = [
            {"reading_level": 4.0},
            {"reading_level": 4.2}
        ]
        
        # Mock student data query  
        mock_conn.fetchrow.return_value = {
            "student_id": "S001",
            "grade_level": 4,
            "prior_year_reading_score": 3
        }
        
        result = await get_student_reading_level_from_db("S001", mock_pool)
            
        assert result["avg_reading_level"] == 4.1  # Average of 4.0 and 4.2
        assert result["confidence"] == 0.4  # 2 books
        assert result["method"] == "checkout_history"
        assert "error" not in result

    @pytest.mark.asyncio
    async def test_get_student_reading_level_from_db_student_not_found(self):
        """Test database wrapper when student doesn't exist."""
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        # Mock student not found
        mock_conn.fetchrow.return_value = None
        
        result = await get_student_reading_level_from_db("NONEXISTENT", mock_pool)
            
        assert result["avg_reading_level"] == 4.0  # Default fallback
        assert result["confidence"] == 0.0
        assert result["method"] == "default_fallback"
        assert "student_not_found" in result["error"]

    @pytest.mark.asyncio 
    async def test_get_student_reading_level_from_db_database_error(self):
        """Test database wrapper with connection error."""
        mock_pool = AsyncMock()
        mock_pool.acquire.side_effect = Exception("Connection failed")
        
        result = await get_student_reading_level_from_db("S001", mock_pool)
            
        assert result["avg_reading_level"] == 4.0  # Safe fallback
        assert result["confidence"] == 0.0
        assert result["method"] == "error_fallback"
        assert "Connection failed" in result["error_details"]

    def test_reading_level_data_types(self):
        """Test that function handles various data types correctly."""
        # Test with float reading levels
        recent_checkouts = [
            {"reading_level": 3.5},
            {"reading_level": 4.0},
            {"reading_level": None},  # Missing reading level
            {"reading_level": 3.8}
        ]
        
        result = compute_student_reading_level(
            checkout_rows=recent_checkouts,
            student_grade=4,
            eog_score=3
        )
        
        # Should skip None values and average the rest
        expected_avg = (3.5 + 4.0 + 3.8) / 3  # 3.77
        assert abs(result["avg_reading_level"] - expected_avg) < 0.1
        assert result["books_used"] == 3  # Should exclude None values

if __name__ == "__main__":
    pytest.main([__file__]) 