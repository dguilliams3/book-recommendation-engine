"""Lightweight, deterministic *pre*-ranking of candidate books.

We'd rather not ask the LLM to evaluate fifty books—it's slow and expensive.
Instead we whittle the list down to a given set (~8 is a good choice) items using a simple formula that is:

• Fast (pure Python, no network)
• Transparent (weights in ``weights.json``)
• Tunable (PMs can edit numbers, no redeploy)

The LLM then has a high-quality shortlist to choose from, and any cost/latency
concerns are mitigated.

Phase 2 Enhancement: Adds rating_boost component based on semantic similarity
and social signals from similar students' reading patterns.
"""

from __future__ import annotations

import math
from typing import Dict, Iterable, Tuple

from common.weights import get as get_weights


class CandidateBook(dict):
    """Typed alias so mypy/IDE knows expected keys.

    Phase 2 additions:
    - semantic_candidate: bool - flagged by FAISS semantic search
    - rating_boost: float - calculated from student's rating history patterns

    Phase 3 additions:
    - query_match: bool - flagged by direct query semantic search
    """

    book_id: str
    title: str | None
    author: str | None
    level: float | None
    days_since_checkout: int | None
    neighbour_recent: int  # how many neighbours checked out in last X days
    staff_pick: bool
    semantic_candidate: bool  # Phase 2: from FAISS search
    rating_boost: float | None  # Phase 2: calculated rating boost
    query_match: bool | None  # Phase 3: direct query semantic match


def score_candidates(
    cands: Iterable[CandidateBook],
) -> list[Tuple[float, CandidateBook]]:
    """Return list sorted descending by score.

    This is where we turn a pile of candidate books into a ranked shortlist
    for the LLM. We use a weighted formula that balances multiple signals:

    Phase 2 Formula:
    final_score = (reading_match × α + rating_boost × β + social_boost × γ + recency_decay × δ)

    Where:
    - α (reading_match_weight): How well book level matches student level
    - β (rating_boost_weight): Boost for books similar to highly-rated books
    - γ (social_boost_weight): Boost from neighbor checkout patterns
    - δ (recency_weight): Decay factor for book age/checkout recency

    The beauty is that PMs can tune these weights without touching code.
    """
    from common.structured_logging import get_logger

    logger = get_logger(__name__)

    w = get_weights()  # hot-reloaded mapping
    logger.debug(
        "Scoring candidates", extra={"candidate_count": len(list(cands)), "weights": w}
    )
    out: list[Tuple[float, CandidateBook]] = []
    half_life = w.get("recency_half_life_days", 30)

    for c in cands:
        s = 0.0

        # --- Reading level proximity (α) ----------------------------------
        # Smaller difference between student and book → bigger score boost.
        # We normalise the diff by /5 so the term stays in 0-1 range.
        if c.get("level") is not None and c.get("student_level") is not None:
            diff = abs(
                float(c["level"]) - float(c.get("student_level", c["level"]))
            )  # Convert to float
            reading_match = max(0, 1 - diff / 5)
            s += (
                w.get("reading_match_weight", w.get("reading_match", 0.4))
                * reading_match
            )
        elif c.get("level") is not None:
            # Default reading match when student level unavailable
            s += w.get("reading_match_weight", w.get("reading_match", 0.4)) * 0.5

        # --- Phase 2: Rating boost (β) ------------------------------------
        # Boost for books found via semantic similarity to highly-rated books
        rating_boost = 0.0

        # --- Phase 3: Query match boost (highest priority) ----------------
        if c.get("query_match", False):
            # Query matches get the highest boost - user explicitly searched for this
            rating_boost += 1.0  # Maximum boost for direct query matches
        elif c.get("semantic_candidate", False):
            # Semantic candidates get a base boost
            rating_boost += 0.6  # Base semantic similarity boost

        # Additional boost can be calculated from student's rating patterns
        # (This would require additional DB queries in a full implementation)
        if c.get("rating_boost") is not None:
            rating_boost += c["rating_boost"]

        s += w.get("rating_boost_weight", 0.3) * rating_boost

        # --- Social boost (γ) ---------------------------------------------
        # How many similar students checked out this book recently
        social_boost = c.get("neighbour_recent", 0)
        s += w.get("social_boost_weight", w.get("social_boost", 0.2)) * social_boost

        # --- Recency decay (δ) --------------------------------------------
        recency_decay = 0.0
        if c.get("days_since_checkout") is not None:
            recency_decay = math.exp(-c["days_since_checkout"] / half_life)
        s += w.get("recency_weight", 0.1) * recency_decay

        # --- Librarian staff-pick bonus -----------------------------------
        if c.get("staff_pick"):
            s += w.get("staff_pick_bonus", 0.05)

        out.append((s, c))

    out.sort(key=lambda t: t[0], reverse=True)
    return out
