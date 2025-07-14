from typing import Any, Dict, Tuple

from common.reading_level_utils import numeric_to_grade_text
from .base import Flattener


class BookFlattener(Flattener):
    """Flatten a catalog row into embedding text and metadata."""

    def __call__(self, row: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        # Ensure lists for genre/keywords
        genres = row.get("genre") or []
        if isinstance(genres, str):
            genres = [genres]
        keywords = row.get("keywords") or []
        if isinstance(keywords, str):
            keywords = [keywords]

        level = row.get("reading_level")
        grade_label = numeric_to_grade_text(level)

        text_parts = [
            row.get("title", ""),
            row.get("description", ""),
            *genres,
            *keywords,
        ]
        # Include author name to improve semantic clustering
        author = row.get("author")
        if author:
            text_parts.append(author)
        if grade_label:
            text_parts.append(grade_label)
        text = ". ".join(filter(None, text_parts))

        meta = {
            "book_id": row.get("book_id"),
            "reading_level": level,
            "grade_label": grade_label,
            "genre": genres,
            "keywords": keywords,
            "author": author,
        }
        return text, meta
