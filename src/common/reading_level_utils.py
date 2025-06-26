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


async def _llm_estimate(title: str, *, model: str = "gpt-4o-mini") -> Optional[Tuple[float, float]]:
    """Return (grade_estimate, confidence) using OpenAI chat completion."""
    if openai is None or not _OPENAI_API_KEY:
        return None
    openai.api_key = _OPENAI_API_KEY
    prompt = (
        "You are a children-literature expert. Given ONLY the book title, "
        "estimate its typical grade reading level on the US scale (K=0) with one decimal. "
        "Also provide a confidence 0-1. Output JSON: {\"grade_estimate\":?,\"confidence\":?}. "
        "If unsure, set confidence below 0.4.\n\n"
        f"Title: \"{title}\""
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