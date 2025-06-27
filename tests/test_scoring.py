import pytest

from recommendation_api.scoring import score_candidates
import common.weights as W


def _fixed_weights():
    return {
        "reading_match": 1.0,
        "recency_half_life_days": 30,
        "social_boost": 1.0,
        "staff_pick_bonus": 0.5,
        "cold_start_k": 20,
    }


def _phase2_weights():
    """Phase 2 weights with new components."""
    return {
        "reading_match": 1.0,
        "reading_match_weight": 0.4,
        "rating_boost_weight": 0.3,
        "social_boost": 0.1,
        "social_boost_weight": 0.2,
        "recency_weight": 0.1,
        "recency_half_life_days": 30,
        "staff_pick_bonus": 0.05,
        "cold_start_k": 20,
        "semantic_history_count": 10
    }


def _mk(level, days, neigh, pick=False, rating_boost=0.0, semantic_candidate=False):
    return {
        "book_id": f"b_{level}_{days}_{neigh}_{pick}",
        "level": level,
        "days_since_checkout": days,
        "neighbour_recent": neigh,
        "staff_pick": pick,
        "student_level": 4,
        "rating_boost": rating_boost,
        "semantic_candidate": semantic_candidate,
    }


def test_scoring_prefers_level_and_recency(monkeypatch):
    monkeypatch.setattr(W, "get", _fixed_weights)
    better = _mk(4, 1, 0)
    worse = _mk(1, 60, 0)
    ranked = score_candidates([better, worse])
    assert ranked[0][1]["book_id"] == better["book_id"]


def test_staff_pick_bonus(monkeypatch):
    monkeypatch.setattr(W, "get", _fixed_weights)
    sp = _mk(4, 100, 0, pick=True)
    normal = _mk(4, 100, 0, pick=False)
    ranked = score_candidates([sp, normal])
    assert ranked[0][1]["book_id"] == sp["book_id"]


# ===== PHASE 2 TESTS =====

def test_phase2_rating_boost_component(monkeypatch):
    """Test Phase 2: rating_boost component in scoring formula."""
    monkeypatch.setattr(W, "get", _phase2_weights)
    
    # Two similar books, one with higher rating boost
    high_rated = _mk(4, 10, 1, rating_boost=0.8)  # From 5-star books
    low_rated = _mk(4, 10, 1, rating_boost=0.2)   # From 2-star books
    
    ranked = score_candidates([high_rated, low_rated])
    
    # High-rated semantic candidate should score higher
    assert ranked[0][1]["book_id"] == high_rated["book_id"]
    assert ranked[0][0] > ranked[1][0], "High rating boost should increase score"


def test_phase2_semantic_candidate_flag(monkeypatch):
    """Test that semantic_candidate flag is preserved."""
    monkeypatch.setattr(W, "get", _phase2_weights)
    
    semantic = _mk(4, 10, 1, semantic_candidate=True, rating_boost=0.6)
    regular = _mk(4, 10, 1, semantic_candidate=False, rating_boost=0.0)
    
    ranked = score_candidates([semantic, regular])
    
    # Semantic candidate should score higher due to rating boost
    assert ranked[0][1]["book_id"] == semantic["book_id"]
    assert ranked[0][1]["semantic_candidate"] == True
    assert ranked[1][1]["semantic_candidate"] == False


def test_phase2_rating_weight_graduation(monkeypatch):
    """Test Phase 2: rating weights (5★=1.0, 4★=0.7, 3★=0.4, 2★=0.2, 1★=0.1)."""
    monkeypatch.setattr(W, "get", _phase2_weights)
    
    candidates = [
        _mk(4, 10, 1, rating_boost=1.0),   # 5-star books
        _mk(4, 10, 1, rating_boost=0.7),   # 4-star books  
        _mk(4, 10, 1, rating_boost=0.4),   # 3-star books
        _mk(4, 10, 1, rating_boost=0.2),   # 2-star books
        _mk(4, 10, 1, rating_boost=0.1),   # 1-star books
    ]
    
    ranked = score_candidates(candidates)
    
    # Should be ranked by rating boost (highest first)
    scores = [score for score, _ in ranked]
    assert scores == sorted(scores, reverse=True), "Scores should be descending"
    
    # Verify rating boost order
    rating_boosts = [cand["rating_boost"] for _, cand in ranked]
    assert rating_boosts == [1.0, 0.7, 0.4, 0.2, 0.1]


def test_phase2_combined_scoring_formula(monkeypatch):
    """Test Phase 2: combined formula with all components."""
    monkeypatch.setattr(W, "get", _phase2_weights)
    
    # Perfect match: good reading level + high rating + social signal + recent
    perfect = _mk(4, 1, 5, pick=True, rating_boost=1.0, semantic_candidate=True)
    
    # Poor match: wrong level + low rating + no social + old
    poor = _mk(1, 90, 0, pick=False, rating_boost=0.1, semantic_candidate=False)
    
    ranked = score_candidates([perfect, poor])
    
    assert ranked[0][1]["book_id"] == perfect["book_id"]
    assert ranked[0][0] > ranked[1][0] * 2, "Perfect match should significantly outscore poor match"


def test_phase2_weights_loaded_correctly(monkeypatch):
    """Test that Phase 2 weights are loaded and used correctly."""
    monkeypatch.setattr(W, "get", _phase2_weights)
    
    # Mock weights should include all Phase 2 components
    weights = W.get()
    
    assert "reading_match_weight" in weights
    assert "rating_boost_weight" in weights  
    assert "social_boost_weight" in weights
    assert "recency_weight" in weights
    assert "semantic_history_count" in weights
    
    assert weights["rating_boost_weight"] == 0.3
    assert weights["semantic_history_count"] == 10


def test_phase2_backwards_compatibility(monkeypatch):
    """Test that Phase 2 doesn't break existing functionality."""
    monkeypatch.setattr(W, "get", _fixed_weights)  # Old weights
    
    # Old-style candidates without rating_boost
    old_style = {
        "book_id": "old_book",
        "level": 4,
        "days_since_checkout": 10,
        "neighbour_recent": 2,
        "staff_pick": False,
        "student_level": 4,
        # No rating_boost or semantic_candidate
    }
    
    ranked = score_candidates([old_style])
    
    # Should not crash and should return valid results
    assert len(ranked) == 1
    assert ranked[0][1]["book_id"] == "old_book"
    assert isinstance(ranked[0][0], (int, float)) 