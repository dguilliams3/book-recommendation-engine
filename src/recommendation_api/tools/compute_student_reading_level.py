import asyncpg
from typing import Dict, Optional, List, Union
from common import SettingsInstance as S

async def compute_student_reading_level(student_id_or_rows: Union[str, List[Dict]]) -> Optional[Dict]:
    """Compute student's current reading level based on checkout history and ratings."""
    
    # If we got a student_id, fetch the data
    if isinstance(student_id_or_rows, str):
        student_id = student_id_or_rows
        pg_url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
        conn = await asyncpg.connect(pg_url)
        
        try:
            # Get recent checkout history with book difficulty and student ratings
            rows = await conn.fetch("""
                SELECT c.book_id, c.student_rating, b.difficulty_band, c.checkout_date
                FROM checkout c
                JOIN catalog b ON c.book_id = b.book_id
                WHERE c.student_id = $1 
                AND c.student_rating IS NOT NULL
                AND b.difficulty_band IS NOT NULL
                ORDER BY c.checkout_date DESC
                LIMIT 20
            """, student_id)
            
            if not rows:
                return {"reading_level": "unknown", "confidence": 0.0}
            
            # Convert to list of dicts for processing
            rows_data = [dict(r) for r in rows]
            
        finally:
            await conn.close()
    else:
        # We already have the processed rows
        rows_data = student_id_or_rows
        if not rows_data:
            return {"reading_level": "unknown", "confidence": 0.0}
    
    # Analyze reading patterns
    difficulty_scores = {
        "beginner": 1.0,
        "early_elementary": 2.0,
        "late_elementary": 3.0,
        "middle_school": 4.0,
        "advanced": 5.0
    }
    
    total_score = 0
    total_weight = 0
    
    for i, row in enumerate(rows_data):
        difficulty = row.get("difficulty_band")
        rating = row.get("student_rating", row.get("rating", 3))  # Handle both field names
        
        if difficulty in difficulty_scores:
            # Weight recent books more heavily
            weight = 1.0 / (i + 1)
            # Adjust score based on student rating (1-5 scale)
            rating_factor = (rating - 3) / 2  # -1 to +1
            adjusted_score = difficulty_scores[difficulty] + rating_factor
            
            total_score += adjusted_score * weight
            total_weight += weight
    
    if total_weight == 0:
        return {"reading_level": "unknown", "confidence": 0.0}
    
    avg_score = total_score / total_weight
    
    # Map score back to difficulty band
    if avg_score <= 1.5:
        reading_level = "beginner"
    elif avg_score <= 2.5:
        reading_level = "early_elementary"
    elif avg_score <= 3.5:
        reading_level = "late_elementary"
    elif avg_score <= 4.5:
        reading_level = "middle_school"
    else:
        reading_level = "advanced"
    
    # Calculate confidence based on number of ratings
    confidence = min(1.0, len(rows_data) / 10.0)
    
    return {
        "reading_level": reading_level,
        "confidence": confidence,
        "average_score": avg_score,
        "books_analyzed": len(rows_data)
    } 