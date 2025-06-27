from typing import Any, Dict, Tuple

from .base import Flattener

class StudentFlattener(Flattener):
    """Create embedding text for a student based on profile columns and reading history.
    
    Phase 2 Enhancement: Includes homeroom_teacher and lunch_period tokens for
    improved social similarity detection in the student embedding graph.
    """

    def __call__(self, row: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        # Base student profile
        parts = [f"Grade {row.get('grade_level', 4)} student with id {row.get('student_id')}"]
        
        # Add homeroom context for social clustering
        homeroom = row.get('homeroom_teacher')
        if homeroom:
            # Normalize teacher name to token format (e.g., "Ms. Patel" -> "teacher-patel")
            teacher_token = homeroom.lower().replace('ms. ', '').replace('mr. ', '').replace(' ', '-')
            parts.append(f"teacher-{teacher_token}")
        
        # Add lunch period for additional social context
        lunch = row.get('lunch_period')
        if lunch:
            parts.append(f"lunch-{lunch}")
        
        # Include prior reading performance if available
        prior_score = row.get('prior_year_reading_score')
        if prior_score:
            parts.append(f"reading-level-{round(prior_score, 1)}")
        
        text = " ".join(parts)
        
        meta = {
            "student_id": row.get("student_id"), 
            "grade_level": row.get("grade_level"),
            "homeroom_teacher": homeroom,
            "lunch_period": lunch,
            "prior_year_reading_score": prior_score
        }
        
        return text, meta 