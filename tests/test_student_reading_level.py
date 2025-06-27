#!/usr/bin/env python3
"""
Test Dan's practical student reading level computation.

This validates the new logic that:
1. Averages reading levels from recent checkouts  
2. Falls back to grade + EOG adjustment
3. Handles edge cases safely (no negative levels)
"""
import pytest
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from common.reading_level_utils import compute_student_reading_level


def test_checkout_history_primary_method():
    """Test primary method: averaging recent checkout reading levels."""
    checkout_rows = [
        {"reading_level": 3.5, "student_rating": 4},
        {"reading_level": 4.0, "student_rating": 5}, 
        {"reading_level": 3.8, "student_rating": 3},
        {"reading_level": 4.2, "student_rating": 4},
    ]
    
    result = compute_student_reading_level(checkout_rows, student_grade=4, eog_score=3)
    
    expected_avg = (3.5 + 4.0 + 3.8 + 4.2) / 4  # 3.875
    
    assert result["avg_reading_level"] == 3.9  # Rounded to 1 decimal
    assert result["method"] == "checkout_history"
    assert result["books_used"] == 4
    assert result["confidence"] == 0.8  # 4/5 = 0.8


def test_eog_fallback_method():
    """Test EOG fallback when no checkout history available."""
    # No checkout data
    checkout_rows = []
    
    # Test various EOG scores
    test_cases = [
        (4, 5, 6.0),  # Grade 4, EOG 5 -> 4 + 2 = 6.0
        (4, 4, 5.0),  # Grade 4, EOG 4 -> 4 + 1 = 5.0  
        (4, 3, 4.0),  # Grade 4, EOG 3 -> 4 + 0 = 4.0
        (4, 2, 3.0),  # Grade 4, EOG 2 -> 4 - 1 = 3.0
        (4, 1, 2.0),  # Grade 4, EOG 1 -> 4 - 2 = 2.0
    ]
    
    for grade, eog, expected_level in test_cases:
        result = compute_student_reading_level(checkout_rows, student_grade=grade, eog_score=eog)
        
        assert result["avg_reading_level"] == expected_level
        assert result["method"] == "eog_fallback"
        assert result["confidence"] == 0.3
        assert result["eog_score"] == eog


def test_safety_no_negative_levels():
    """Test safety check: never return negative reading levels."""
    checkout_rows = []
    
    # Grade 1 student with EOG score 1 -> 1 + (-2) = -1, should become 0.5
    result = compute_student_reading_level(checkout_rows, student_grade=1, eog_score=1)
    
    assert result["avg_reading_level"] == 0.5  # Minimum allowed level
    assert result["method"] == "eog_fallback"
    

def test_invalid_data_handling():
    """Test handling of invalid/missing data."""
    # Invalid reading levels in checkout data
    checkout_rows = [
        {"reading_level": None, "student_rating": 4},
        {"reading_level": "invalid", "student_rating": 5},
        {"reading_level": -1.0, "student_rating": 3},  # Negative - should be skipped
        {"reading_level": 4.0, "student_rating": 4},  # Only this should count
    ]
    
    result = compute_student_reading_level(checkout_rows, student_grade=4, eog_score=3)
    
    assert result["avg_reading_level"] == 4.0  # Only one valid book
    assert result["books_used"] == 1
    assert result["method"] == "checkout_history"


def test_ultimate_fallback():
    """Test ultimate fallback to grade level when everything else fails."""
    checkout_rows = []
    
    # Invalid EOG data
    result = compute_student_reading_level(checkout_rows, student_grade=3, eog_score="invalid")
    
    assert result["avg_reading_level"] == 3.0  # Uses grade level
    assert result["method"] == "grade_fallback"
    assert result["confidence"] == 0.1
    

def test_recent_limit():
    """Test that only recent books are considered for averaging."""
    # 12 books, but only last 10 should be used (recent_limit=10)
    checkout_rows = [
        {"reading_level": 1.0, "student_rating": 3},  # Old book - should be ignored
        {"reading_level": 2.0, "student_rating": 3},  # Old book - should be ignored  
    ] + [
        {"reading_level": 4.0, "student_rating": 4} for _ in range(10)  # 10 recent books at level 4.0
    ]
    
    result = compute_student_reading_level(checkout_rows, student_grade=4, eog_score=3, recent_limit=10)
    
    assert result["avg_reading_level"] == 4.0  # Should average the 10 recent books only
    assert result["books_used"] == 10
    assert result["method"] == "checkout_history"


def test_confidence_scaling():
    """Test that confidence scales with number of books."""
    # Test with different numbers of books
    test_cases = [
        (1, 0.2),   # 1 book -> 1/5 = 0.2 confidence
        (3, 0.6),   # 3 books -> 3/5 = 0.6 confidence
        (5, 1.0),   # 5+ books -> 1.0 confidence (max)
        (10, 1.0),  # 10 books -> still 1.0 confidence (capped)
    ]
    
    for num_books, expected_confidence in test_cases:
        checkout_rows = [{"reading_level": 4.0, "student_rating": 4} for _ in range(num_books)]
        result = compute_student_reading_level(checkout_rows, student_grade=4, eog_score=3)
        
        assert result["confidence"] == expected_confidence
        assert result["books_used"] == num_books


if __name__ == "__main__":
    print("🧪 Testing Dan's Student Reading Level Logic")
    print("=" * 50)
    
    # Run all tests
    test_functions = [
        test_checkout_history_primary_method,
        test_eog_fallback_method, 
        test_safety_no_negative_levels,
        test_invalid_data_handling,
        test_ultimate_fallback,
        test_recent_limit,
        test_confidence_scaling,
    ]
    
    passed = 0
    for test_func in test_functions:
        try:
            test_func()
            print(f"✅ {test_func.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"❌ {test_func.__name__}: {e}")
        except Exception as e:
            print(f"💥 {test_func.__name__}: {e}")
    
    print(f"\n🎯 Results: {passed}/{len(test_functions)} tests passed")
    
    if passed == len(test_functions):
        print("🎉 ALL TESTS PASSED - Dan's logic is solid!")
    else:
        print("⚠️  Some tests failed - needs fixes") 