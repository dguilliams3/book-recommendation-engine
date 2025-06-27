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

from typing import List

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
    "build_prompt",
]


# ---------------------------------------------------------------------------
# Output schema (Pydantic) ---------------------------------------------------
# ---------------------------------------------------------------------------
class BookRec(BaseModel):
    """One recommended book returned by the agent."""

    book_id: str = Field(..., description="Catalog ID of the book")
    title: str = Field(..., description="Display title of the book")
    author: str = Field(..., description="Author of the book")
    reading_level: float = Field(..., description="Reading level (grade level) of the book")
    librarian_blurb: str = Field(
        ..., description="One-sentence human-readable rationale/summary"
    )
    justification: str = Field(
        ..., description="Brief explanation (≤25 words) of why this title suits the student's interests/level"
    )


class BookRecList(BaseModel):
    """Top-level container so the LLM output is always predictable."""

    recommendations: List[BookRec]


# Parser instance (exported)
parser = PydanticOutputParser(pydantic_object=BookRecList)

# ---------------------------------------------------------------------------
# Prompt template -----------------------------------------------------------
# ---------------------------------------------------------------------------
_SYSTEM_TMPL = """
You are an elementary-school librarian AI with deep knowledge of children's
literature across genres and reading levels.  Given a student's ID, their
approximate reading grade (4th grade), and a set of interest keywords, you
must recommend *distinct* titles from our catalog.

Requirements:
* Return *JSON only*, no markdown or explanation.
* The JSON **must** conform to the schema described in the
  {format_instructions} placeholder below.
* Pick books that match the reading level (within ±1 grade) unless no match
  exists.
* Be diverse in genre and author.
* The ``librarian_blurb`` should be short (≤25 words).
* Provide a separate ``justification`` field (≤25 words) explaining **why** the title is a good fit. Include specifics related to the student's history and data with your justification.
* You **must** include the author, reading_level, and justification for each book in your response.
* The reading_level should be a number (e.g., 4.5 for 4th grade level).
* A context line will be provided that lists the student's average reading level and their most recent books.
* You must still call `get_student_reading_level` to verify the level and MAY call `find_similar_students` if you need more insight.
"""

# Build the ChatPromptTemplate once so we don't pay construction costs every call
_BASE_PROMPT = ChatPromptTemplate.from_messages(
    [
        SystemMessagePromptTemplate.from_template(_SYSTEM_TMPL),
        HumanMessagePromptTemplate.from_template("{user_prompt}"),
    ]
)


def build_prompt(user_prompt: str):  # noqa: D401 – simple helper
    """Return a **list** of messages ready for LangChain/ChatOpenAI.

    The caller just passes whatever they would normally ask the assistant in
    natural language; we insert the format instructions automatically.
    """

    return _BASE_PROMPT.format_prompt(
        user_prompt=user_prompt,
        format_instructions=parser.get_format_instructions(),
    ).to_messages() 