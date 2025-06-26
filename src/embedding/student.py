from typing import Any, Dict, Tuple

from .base import Flattener

class StudentFlattener(Flattener):
    """Create embedding text for a student based on profile columns."""

    def __call__(self, row: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        text = f"Grade {row.get('grade_level')} student with id {row.get('student_id')}"
        meta = {"student_id": row.get("student_id"), "grade_level": row.get("grade_level")}
        return text, meta 