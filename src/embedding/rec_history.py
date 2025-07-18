from typing import Any, Dict, Tuple

from .base import Flattener


class RecommendationFlattener(Flattener):
    """Flatten historical recommendation row."""

    def __call__(self, row: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        text = (
            f"On {row.get('recommended_at')}, recommended book {row.get('book_id')} "
            f"to user {row.get('user_id')}"
        )
        meta = {"user_id": row.get("user_id"), "book_id": row.get("book_id")}
        return text, meta
