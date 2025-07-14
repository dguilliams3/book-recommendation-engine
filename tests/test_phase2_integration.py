"""Integration tests for Phase 2 enhancements."""
import pytest
import asyncio
from unittest.mock import patch
import os

pytestmark = pytest.mark.integration

skip_integration = pytest.mark.skipif(
    os.name == 'nt' and not os.environ.get('RUN_INTEGRATION'),
    reason="Integration tests require Docker and are skipped on Windows unless RUN_INTEGRATION=1"
)

from common.weights import get as get_weights


def test_phase2_weights_configuration():
    """Test that Phase 2 weights are properly configured."""
    weights = get_weights()
    
    # Verify all Phase 2 parameters are present
    phase2_params = [
        "reading_match_weight",
        "rating_boost_weight", 
        "social_boost_weight",
        "recency_weight",
        "semantic_history_count"
    ]
    
    for param in phase2_params:
        assert param in weights, f"Missing Phase 2 parameter: {param}"
    
    # Verify sensible defaults
    assert 0 <= weights["reading_match_weight"] <= 1.0
    assert 0 <= weights["rating_boost_weight"] <= 1.0
    assert 0 <= weights["social_boost_weight"] <= 1.0
    assert 0 <= weights["recency_weight"] <= 1.0
    assert weights["semantic_history_count"] > 0


def test_phase2_rating_weight_mapping():
    """Test the Phase 2 rating-to-weight mapping."""
    from recommendation_api.candidate_builder import RATING_WEIGHTS
    
    expected_mapping = {
        5: 1.0,  # 5 stars = full weight
        4: 0.7,  # 4 stars = 70% weight
        3: 0.4,  # 3 stars = 40% weight
        2: 0.2,  # 2 stars = 20% weight
        1: 0.1   # 1 star = 10% weight
    }
    
    assert RATING_WEIGHTS == expected_mapping
    
    # Verify all weights are in valid range
    for rating, weight in RATING_WEIGHTS.items():
        assert 1 <= rating <= 5, f"Invalid rating: {rating}"
        assert 0 <= weight <= 1.0, f"Invalid weight for rating {rating}: {weight}"


def test_phase2_candidate_book_structure():
    """Test that CandidateBook supports Phase 2 fields."""
    from recommendation_api.scoring import CandidateBook
    
    # Test Phase 2 CandidateBook with new fields
    candidate = CandidateBook(
        book_id="B001",
        level=4.0,
        days_since_checkout=5,
        neighbour_recent=3,
        staff_pick=False,
        rating_boost=0.7,  # Phase 2: rating boost from semantic similarity
        semantic_candidate=True  # Phase 2: flag for semantic candidates
    )
    
    # Verify all fields are accessible
    assert candidate["book_id"] == "B001"
    assert candidate["rating_boost"] == 0.7
    assert candidate["semantic_candidate"] == True


def test_phase2_student_flattener_homeroom_lunch():
    """Test Phase 2 StudentFlattener integration."""
    from embedding.student import StudentFlattener
    
    student_data = {
        "student_id": "S001",
        "grade_level": 4,
        "homeroom_teacher": "Ms. Johnson",
        "lunch_period": 2,
        "prior_year_reading_score": 3.5
    }
    
    flattener = StudentFlattener()
    text, metadata = flattener(student_data)
    
    # Verify Phase 2 tokens are included
    assert "teacher-johnson" in text.lower()
    assert "lunch-2" in text
    assert "Grade 4 student with id S001" in text
    
    # Verify metadata preservation
    assert metadata["student_id"] == "S001"
    assert "homeroom_teacher" in metadata
    assert "lunch_period" in metadata


def test_phase2_weights_consistency():
    """Test that weights files are consistent between common and api modules."""
    from common.weights import get as get_common_weights
    
    # Load weights from common module
    common_weights = get_common_weights()
    
    # Check that API weights file exists and can be loaded
    import json
    from pathlib import Path
    
    api_weights_path = Path("src/recommendation_api/weights.json")
    assert api_weights_path.exists(), "API weights.json file should exist"
    
    with open(api_weights_path) as f:
        api_weights = json.load(f)
    
    # Verify key Phase 2 parameters match
    phase2_keys = ["reading_match_weight", "rating_boost_weight", "social_boost_weight"]
    for key in phase2_keys:
        assert key in common_weights, f"Missing {key} in common weights"
        assert key in api_weights, f"Missing {key} in API weights"
        assert common_weights[key] == api_weights[key], f"Mismatch for {key}: common={common_weights[key]}, api={api_weights[key]}"


@skip_integration
@pytest.mark.asyncio
async def test_phase2_faiss_integration_graceful_failure():
    """Test that FAISS integration fails gracefully when unavailable."""
    from recommendation_api.candidate_builder import _load_faiss_store
    
    # Test FAISS loading (should handle missing files gracefully)
    store = _load_faiss_store()
    
    # Should either load successfully or return None (no crashes)
    assert store is None or hasattr(store, 'similarity_search')


def test_phase2_backward_compatibility():
    """Test that Phase 2 changes don't break existing functionality.""" 
    from recommendation_api.scoring import score_candidates
    
    # Old-style candidate without Phase 2 fields
    old_candidate = {
        "book_id": "B001",
        "level": 4,
        "days_since_checkout": 10,
        "neighbour_recent": 2,
        "staff_pick": False,
        "student_level": 4
        # No rating_boost or semantic_candidate fields
    }
    
    # Should not crash with old-style data
    ranked = score_candidates([old_candidate])
    
    assert len(ranked) == 1
    assert ranked[0][1]["book_id"] == "B001"
    assert isinstance(ranked[0][0], (int, float))


def test_phase2_comprehensive_scoring():
    """Test Phase 2 comprehensive scoring with all components."""
    from recommendation_api.scoring import score_candidates
    
    # Phase 2 candidate with all new fields
    phase2_candidate = {
        "book_id": "B002",
        "level": 4,
        "days_since_checkout": 5,
        "neighbour_recent": 3,
        "staff_pick": True,
        "student_level": 4,
        "rating_boost": 0.8,  # High rating boost
        "semantic_candidate": True
    }
    
    # Basic candidate for comparison
    basic_candidate = {
        "book_id": "B003",
        "level": 4,
        "days_since_checkout": 5,
        "neighbour_recent": 3,
        "staff_pick": False,
        "student_level": 4,
        "rating_boost": 0.0,
        "semantic_candidate": False
    }
    
    candidates = [phase2_candidate, basic_candidate]
    ranked = score_candidates(candidates)
    
    # Phase 2 candidate should score higher due to rating boost and staff pick
    assert ranked[0][1]["book_id"] == "B002"
    assert ranked[0][0] > ranked[1][0]
    
    # Verify Phase 2 fields are preserved
    assert ranked[0][1]["semantic_candidate"] == True
    assert ranked[0][1]["rating_boost"] == 0.8 