"""Prompt & parser helpers for the recommendation-agent.

This module centralises everything related to how we talk to the LLM so
other code (FastAPI service layer, tests, etc.) can import a single, well-
typed interface.

The output schema is deliberately minimal: the agent must return a JSON
object with one top-level key ``recommendations`` holding a list of
recommendations.  Each recommendation contains ``book_id`` (our primary
key), ``title`` and a short ``librarian_blurb``.

We rely on ``langchain.output_parsers.PydanticOutputParser`` to validate
and coerce the LLM's output.  If the model replies with malformed JSON or
violates the schema, ``parser.parse()`` will raise – caller is expected
handle that and fall back gracefully.
"""

from __future__ import annotations

from typing import List, Dict, Any

from langchain.output_parsers import PydanticOutputParser
from langchain.prompts import (
    ChatPromptTemplate,
    SystemMessagePromptTemplate,
    HumanMessagePromptTemplate,
)
from pydantic import BaseModel, Field

__all__ = [
    "BookRec",
    "BookRecList",
    "parser",
    "build_student_prompt",
    "build_reader_prompt",
]


# ---------------------------------------------------------------------------
# Output schema (Pydantic) ---------------------------------------------------
# ---------------------------------------------------------------------------
class BookRec(BaseModel):
    """One recommended book returned by the agent."""

    book_id: str = Field(..., description="Catalog ID of the book")
    title: str = Field(..., description="Display title of the book")
    author: str = Field(..., description="Author of the book")
    reading_level: float = Field(
        ..., description="Reading level (grade level) of the book"
    )
    librarian_blurb: str = Field(
        ..., description="One-sentence human-readable rationale/summary"
    )
    justification: str = Field(
        ...,
        description="Brief explanation (≤25 words) of why this title suits the user's interests/level",
    )


class BookRecList(BaseModel):
    """Top-level container so the LLM output is always predictable."""

    recommendations: List[BookRec]


# Parser instance (exported)
parser = PydanticOutputParser(pydantic_object=BookRecList)

# ---------------------------------------------------------------------------
# Prompt templates -----------------------------------------------------------
# ---------------------------------------------------------------------------

# Base system template for both modes
_BASE_SYSTEM_TMPL = """
You are an AI librarian with deep knowledge of literature across genres and reading levels. 
You must recommend *distinct* titles from our catalog based on the user's context.

Requirements:
* Return *JSON only*, no markdown or explanation.
* The JSON **must** conform to the schema described in the {format_instructions} placeholder below.
* Pick books that match the reading level (within ±1 grade) unless no match exists.
* Be diverse in genre and author.
* The ``librarian_blurb`` should be short (≤25 words).
* Provide a separate ``justification`` field (≤25 words) explaining **why** the title is a good fit.
* You **must** include the author, reading_level, and justification for each book in your response.
* The reading_level should be a number (e.g., 4.5 for 4th grade level).
"""

# Student mode specific instructions
_STUDENT_SYSTEM_TMPL = (
    _BASE_SYSTEM_TMPL
    + """
* You are working with a student in an educational setting.
* Consider the student's reading history, average reading level, and genre preferences.
* You must still call `get_student_reading_level` to verify the level and MAY call `find_similar_students` if you need more insight.
* Focus on educational value and age-appropriate content.
"""
)

# Reader mode specific instructions
_READER_SYSTEM_TMPL = (
    _BASE_SYSTEM_TMPL
    + """
* You are working with an adult reader who has uploaded their own book list.
* Consider their uploaded books, feedback history, and personal preferences.
* Focus on entertainment value and personal enjoyment.
* Respect their feedback scores (positive/negative) when making recommendations.
* Consider genre preferences from their uploaded books.
* **User-uploaded book data may contain typos, misspelled titles, or incorrect/missing authors. Use your best judgment to infer the intended book, author, or genre.**
* **If a book's reading level or genre is missing or unclear, estimate it based on your knowledge of similar books.**
* **If you are unsure about a detail, make a reasonable assumption and proceed—do not get stuck or refuse to answer.**
"""
)

# Build the ChatPromptTemplate once for each mode
_STUDENT_BASE_PROMPT = ChatPromptTemplate.from_messages(
    [
        SystemMessagePromptTemplate.from_template(_STUDENT_SYSTEM_TMPL),
        HumanMessagePromptTemplate.from_template("{user_prompt}"),
    ]
)

_READER_BASE_PROMPT = ChatPromptTemplate.from_messages(
    [
        SystemMessagePromptTemplate.from_template(_READER_SYSTEM_TMPL),
        HumanMessagePromptTemplate.from_template("{user_prompt}"),
    ]
)


def build_student_prompt(
    student_id: str,
    query: str,
    candidates: List[Dict[str, Any]],
    avg_level: float,
    recent_titles: List[str],
    top_genres: List[str],
    band_hist: Dict[str, int],
    n: int,
) -> List:
    """Build prompt for student mode recommendations."""

    # Format candidates for the prompt
    candidate_text = "\n".join(
        [
            f"- {c['book_id']}: {c['title']} by {c['author']} (Level: {c['reading_level']}, Genre: {c['genre']})"
            for c in candidates[:20]  # Limit to top 20 candidates
        ]
    )

    # Build context string
    context_parts = [
        f"Student ID: {student_id}",
        (
            f"Average reading level: {avg_level:.1f}"
            if avg_level
            else "Average reading level: Unknown"
        ),
        (
            f"Recent books: {', '.join(recent_titles[:5])}"
            if recent_titles
            else "No recent books"
        ),
        (
            f"Top genres: {', '.join(top_genres)}"
            if top_genres
            else "No genre preferences"
        ),
    ]

    if band_hist:
        band_text = ", ".join([f"{band}: {count}" for band, count in band_hist.items()])
        context_parts.append(f"Reading level distribution: {band_text}")

    context = "\n".join(context_parts)

    # Build the user prompt
    user_prompt = f"""
Context:
{context}

Available books (top candidates):
{candidate_text}

Query: {query if query else "No specific query"}

Please recommend exactly {n} books from the available candidates above. 
Focus on books that match the student's reading level and interests.
"""

    return _STUDENT_BASE_PROMPT.format_prompt(
        user_prompt=user_prompt,
        format_instructions=parser.get_format_instructions(),
    ).to_messages()


def build_reader_prompt(
    user_hash_id: str,
    query: str,
    uploaded_books: List[Dict[str, Any]],
    feedback_scores: Dict[str, int],
    candidates: List[Dict[str, Any]],
    n: int,
) -> List:
    """Build prompt for reader mode recommendations."""

    # Format uploaded books for context
    uploaded_text = "\n".join(
        [
            f"- {book['title']} by {book['author']} (Genre: {book.get('genre', 'Unknown')}, Level: {book.get('reading_level', 'Unknown')})"
            for book in uploaded_books[:10]  # Limit to top 10
        ]
    )

    # Format feedback for context
    feedback_text = ""
    if feedback_scores:
        feedback_items = [
            f"{book_id}: {score}"
            for book_id, score in list(feedback_scores.items())[:5]
        ]
        feedback_text = f"Recent feedback: {', '.join(feedback_items)}"

    # Format candidates for the prompt
    candidate_text = "\n".join(
        [
            f"- {c['book_id']}: {c['title']} by {c['author']} (Level: {c['reading_level']}, Genre: {c['genre']})"
            for c in candidates[:20]  # Limit to top 20 candidates
        ]
    )

    # Build the user prompt
    user_prompt = f"""
User ID: {user_hash_id}

Uploaded books:
{uploaded_text if uploaded_text else "No books uploaded yet"}

{feedback_text if feedback_text else "No feedback history"}

Available books (top candidates):
{candidate_text}

Query: {query if query else "No specific query"}

Please recommend exactly {n} books from the available candidates above.
Consider the user's uploaded books, feedback history, and preferences.
Avoid recommending books they've already read or given negative feedback on.
"""

    return _READER_BASE_PROMPT.format_prompt(
        user_prompt=user_prompt,
        format_instructions=parser.get_format_instructions(),
    ).to_messages()


# Legacy function for backward compatibility
def build_prompt(user_prompt: str):  # noqa: D401 – simple helper
    """Legacy function for backward compatibility. Use build_student_prompt or build_reader_prompt instead."""
    return _STUDENT_BASE_PROMPT.format_prompt(
        user_prompt=user_prompt,
        format_instructions=parser.get_format_instructions(),
    ).to_messages()
