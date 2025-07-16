"""Utilities to derive numeric reading-level estimates for a book.

Decision tree implemented here mirrors the roadmap discussed in chat:
1. Google Books preview text (highest quality)
2. Open Library description
3. Local blurb FKGL (caller supplies blurb)
4. GPT fallback (title-only estimate)

All public helpers are async so they can be called from workers without
blocking.  For synchronous contexts, wrap with `asyncio.run()`.
"""

from __future__ import annotations

import asyncio
import os
from typing import Optional, Tuple

import textstat
import httpx

try:
    import openai
except ImportError:  # optional dependency
    openai = None  # type: ignore

__all__ = [
    "get_reading_level",
    "numeric_to_grade_text",
]

_GOOGLE_API_KEY = os.getenv("GOOGLE_BOOKS_KEY")
_OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #


async def _fetch_google_preview(isbn: str) -> Optional[str]:
    """Return preview text ≥150 words or None."""
    if not _GOOGLE_API_KEY:
        return None
    url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}&key={_GOOGLE_API_KEY}"
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url)
    if resp.status_code != 200:
        return None
    data = resp.json()
    if not data.get("items"):
        return None
    info = data["items"][0]
    if not info["accessInfo"].get("readingModes", {}).get("text"):
        return None
    # Try to fetch text preview link if present
    epub = info["accessInfo"].get("epub", {})
    pdf = info["accessInfo"].get("pdf", {})
    txt_url = epub.get("downloadLink") or pdf.get("downloadLink")
    if not txt_url:
        return None
    async with httpx.AsyncClient(timeout=15) as client:
        preview_resp = await client.get(txt_url)
    if preview_resp.status_code != 200:
        return None
    text = preview_resp.text
    if len(text.split()) < 150:
        return None
    return text


async def _fetch_openlib_description(isbn: str) -> Optional[str]:
    url = f"https://openlibrary.org/isbn/{isbn}.json"
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url)
    if resp.status_code != 200:
        return None
    data = resp.json()
    desc = data.get("description")
    if isinstance(desc, dict):
        desc = desc.get("value")
    if isinstance(desc, str) and len(desc.split()) >= 100:
        return desc
    return None


def _fkgl(text: str) -> float:
    """Return Flesch-Kincaid Grade Level rounded to 2 decimals."""
    score = textstat.flesch_kincaid_grade(text)
    return round(float(score), 2)


async def _llm_estimate(
    title: str, *, model: str = "gpt-4o-mini"
) -> Optional[Tuple[float, float]]:
    """Return (grade_estimate, confidence) using OpenAI chat completion."""
    if openai is None or not _OPENAI_API_KEY:
        return None
    openai.api_key = _OPENAI_API_KEY
    prompt = (
        "You are a children-literature expert. Given ONLY the book title, "
        "estimate its typical grade reading level on the US scale (K=0) with one decimal. "
        'Also provide a confidence 0-1. Output JSON: {"grade_estimate":?,"confidence":?}. '
        "If unsure, set confidence below 0.4.\n\n"
        f'Title: "{title}"'
    )
    try:
        resp = openai.ChatCompletion.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=60,
        )
        import json

        raw = resp.choices[0].message.content.strip()
        data = json.loads(raw)
        return float(data.get("grade_estimate")), float(data.get("confidence", 0))
    except Exception:
        return None


# --------------------------------------------------------------------------- #
# Public orchestrator                                                          #
# --------------------------------------------------------------------------- #


async def get_reading_level(*_args, **_kwargs):
    """Deprecated stub: returns (None, 'deprecated').

    The system now expects `reading_level` to be provided in the source CSV
    instead of calling external services.  This stub avoids import-time
    explosions in any legacy code paths that might still import it.
    """
    return None, "deprecated"


# --------------------------------------------------------------------------- #
# Grade label mapping                                                          #
# --------------------------------------------------------------------------- #


def numeric_to_grade_text(level: float | int | None) -> str | None:
    """Convert a numeric grade level (e.g., 4.0) into human label ("4th grade").

    Kindergarten is returned for values <1.  Returns ``None`` if *level* is
    ``None`` or negative.
    """
    if level is None or level < 0:
        return None

    if level < 1:
        return "Kindergarten"
    # Round to nearest whole grade for label purposes
    grade = int(round(float(level)))
    if grade <= 0:
        return "Kindergarten"
    if grade == 1:
        suffix = "st"
    elif grade == 2:
        suffix = "nd"
    elif grade == 3:
        suffix = "rd"
    else:
        suffix = "th"
    return f"{grade}{suffix} grade"


#!/usr/bin/env python3
"""
Student Reading Level Computation - Dan's Practical Approach

This module handles computing student reading levels using:
1. PRIMARY: Average reading levels from recent checkout history
2. FALLBACK: Grade level ± EOG score adjustment  
3. SAFETY: Never return negative reading levels

Separated from MCP server concerns for better testability and reuse.
"""
import asyncpg
from typing import List, Dict, Optional, Any
from .structured_logging import get_logger

logger = get_logger(__name__)


def compute_student_reading_level(
    checkout_rows: List[Dict[str, Any]],
    student_grade: Optional[int] = 4,
    eog_score: Optional[float] = 3,
    recent_limit: int = 10,
) -> Dict[str, Any]:
    """
    Compute student reading level using Dan's practical logic.

    This is the core business logic separated from database/MCP concerns.

    Algorithm:
    1. PRIMARY: Average the reading_level of recent checked-out books
    2. FALLBACK: Grade level ± EOG adjustment (3=grade, 4=+1, 5=+2, 2=-1, 1=-2)
    3. SAFETY: Never return negative reading levels (minimum 0.5)

    Args:
        checkout_rows: List of checkout records with reading_level data
        student_grade: Current grade level (default 4th grade)
        eog_score: Last year's EOG reading score (1-5 scale)
        recent_limit: How many recent books to consider for averaging

    Returns:
        dict with avg_reading_level, confidence, method, and metadata

    Examples:
        >>> # Student with good checkout history
        >>> checkouts = [{"reading_level": 4.2}, {"reading_level": 3.8}]
        >>> result = compute_student_reading_level(checkouts, 4, 3)
        >>> result["method"]
        'checkout_history'

        >>> # Student with no checkouts, using EOG fallback
        >>> result = compute_student_reading_level([], 4, 5)  # Grade 4, EOG 5
        >>> result["avg_reading_level"]
        6.0  # 4 + 2
    """
    logger.debug(
        "Computing student reading level",
        extra={
            "checkout_count": len(checkout_rows),
            "student_grade": student_grade,
            "eog_score": eog_score,
            "recent_limit": recent_limit,
        },
    )

    # Extract reading levels from recent checkouts (most recent first)
    reading_levels = []
    for row in checkout_rows[-recent_limit:]:  # Take last N checkouts
        if row.get("reading_level") is not None:
            try:
                level = float(row["reading_level"])
                if level > 0:  # Sanity check - reading levels should be positive
                    reading_levels.append(level)
            except (ValueError, TypeError):
                logger.debug(
                    "Skipping invalid reading level",
                    extra={"value": row.get("reading_level")},
                )
                continue  # Skip invalid reading level data

    # PRIMARY METHOD: Average recent checkout reading levels
    if reading_levels:
        avg_reading_level = sum(reading_levels) / len(reading_levels)
        confidence = min(
            len(reading_levels) / 5.0, 1.0
        )  # More books = higher confidence

        result = {
            "avg_reading_level": round(avg_reading_level, 1),
            "confidence": round(confidence, 2),
            "method": "checkout_history",
            "books_used": len(reading_levels),
            "recent_limit": recent_limit,
        }

        logger.info("Reading level computed from checkout history", extra=result)
        return result

    # FALLBACK METHOD: EOG score adjustment to grade level
    try:
        eog_score = int(eog_score) if eog_score is not None else 3
        student_grade = int(student_grade) if student_grade is not None else 4

        # Dan's EOG adjustment mapping: 3=grade level, 4=+1, 5=+2, 2=-1, 1=-2
        eog_adjustments = {1: -2, 2: -1, 3: 0, 4: 1, 5: 2}
        adjustment = eog_adjustments.get(eog_score, 0)

        estimated_level = student_grade + adjustment

        # SAFETY: Never return negative reading levels
        estimated_level = max(estimated_level, 0.5)  # Minimum reading level is 0.5

        result = {
            "avg_reading_level": round(estimated_level, 1),
            "confidence": 0.3,  # Lower confidence for fallback method
            "method": "eog_fallback",
            "eog_score": eog_score,
            "grade_adjustment": adjustment,
            "grade_level": student_grade,
        }

        logger.info("Reading level computed from EOG fallback", extra=result)
        return result

    except (ValueError, TypeError) as e:
        logger.warning(
            "Invalid EOG/grade data, using grade fallback",
            extra={
                "student_grade": student_grade,
                "eog_score": eog_score,
                "error": str(e),
            },
        )

        # ULTIMATE FALLBACK: Just use grade level
        safe_grade = max(float(student_grade) if student_grade else 4.0, 0.5)
        result = {
            "avg_reading_level": round(safe_grade, 1),
            "confidence": 0.1,  # Very low confidence
            "method": "grade_fallback",
            "note": "Used grade level due to missing/invalid EOG data",
        }

        logger.info("Reading level computed from grade fallback", extra=result)
        return result


async def get_student_reading_level_from_db(
    student_id: str, db_pool: asyncpg.Pool, recent_limit: int = 10
) -> Dict[str, Any]:
    """
    Database wrapper for student reading level computation.

    This handles all the database queries and error handling, then delegates
    to the pure business logic function above.

    Args:
        student_id: Student identifier
        db_pool: AsyncPG connection pool
        recent_limit: How many recent books to consider

    Returns:
        Reading level computation result or error fallback

    Error Handling:
        - Student not found: Returns default grade 4 level with error note
        - Database errors: Returns safe fallback with error details
        - Invalid data: Gracefully handled by compute_student_reading_level()
    """
    logger.info(
        "Fetching student reading level from database",
        extra={"student_id": student_id, "recent_limit": recent_limit},
    )

    try:
        async with db_pool.acquire() as con:
            # Fetch student data for fallback calculation
            student_data = await con.fetchrow(
                """SELECT grade_level, prior_year_reading_score 
                   FROM students WHERE student_id=$1""",
                student_id,
            )

            if not student_data:
                logger.warning(
                    "Student not found in database", extra={"student_id": student_id}
                )
                return {
                    "avg_reading_level": 4.0,
                    "confidence": 0.0,
                    "method": "default_fallback",
                    "error": "student_not_found",
                    "student_id": student_id,
                }

            # Fetch checkout history with reading levels (ordered by checkout date)
            checkout_rows = await con.fetch(
                """SELECT c.checkout_date, cat.reading_level, c.student_rating
                   FROM checkout c
                   JOIN catalog cat USING(book_id)
                   WHERE c.student_id=$1 AND c.return_date IS NOT NULL
                   ORDER BY c.checkout_date DESC""",
                student_id,
            )

            logger.debug(
                "Fetched student data and checkout history",
                extra={
                    "student_id": student_id,
                    "grade_level": student_data["grade_level"],
                    "eog_score": student_data["prior_year_reading_score"],
                    "checkout_count": len(checkout_rows),
                },
            )

            # Convert to list of dicts for processing
            checkout_data = [dict(row) for row in checkout_rows]

            # Call the core business logic
            result = compute_student_reading_level(
                checkout_rows=checkout_data,
                student_grade=student_data["grade_level"],
                eog_score=student_data["prior_year_reading_score"],
                recent_limit=recent_limit,
            )

            # Add metadata for traceability
            result["student_id"] = student_id
            result["total_checkouts"] = len(checkout_rows)

            logger.info("Student reading level computed successfully", extra=result)
            return result

    except Exception as e:
        logger.error(
            "Database error computing student reading level",
            exc_info=True,
            extra={"student_id": student_id},
        )

        # Return safe fallback even on database errors
        return {
            "avg_reading_level": 4.0,
            "confidence": 0.0,
            "method": "error_fallback",
            "error": "database_error",
            "error_details": str(e),
            "student_id": student_id,
        }
