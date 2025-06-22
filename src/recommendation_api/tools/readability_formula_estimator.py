import textstat
from typing import Dict

def readability_formula_estimator(text: str) -> Dict:
    """Estimate reading difficulty using multiple readability formulas."""
    if not text or len(text.strip()) < 50:
        return {"difficulty_band": "unknown"}
    
    # Calculate various readability scores
    flesch_reading_ease = textstat.flesch_reading_ease(text)
    flesch_kincaid_grade = textstat.flesch_kincaid_grade(text)
    gunning_fog = textstat.gunning_fog(text)
    smog_index = textstat.smog_index(text)
    automated_readability_index = textstat.automated_readability_index(text)
    coleman_liau_index = textstat.coleman_liau_index(text)
    linsear_write_formula = textstat.linsear_write_formula(text)
    dale_chall_readability_score = textstat.dale_chall_readability_score(text)
    
    # Determine difficulty band based on average grade level
    avg_grade = (flesch_kincaid_grade + gunning_fog + smog_index + 
                 automated_readability_index + coleman_liau_index + 
                 linsear_write_formula) / 6
    
    if avg_grade <= 2.0:
        difficulty_band = "beginner"
    elif avg_grade <= 4.0:
        difficulty_band = "early_elementary"
    elif avg_grade <= 6.0:
        difficulty_band = "late_elementary"
    elif avg_grade <= 8.0:
        difficulty_band = "middle_school"
    else:
        difficulty_band = "advanced"
    
    return {
        "difficulty_band": difficulty_band,
        "flesch_reading_ease": flesch_reading_ease,
        "flesch_kincaid_grade": flesch_kincaid_grade,
        "gunning_fog": gunning_fog,
        "smog_index": smog_index,
        "automated_readability_index": automated_readability_index,
        "coleman_liau_index": coleman_liau_index,
        "linsear_write_formula": linsear_write_formula,
        "dale_chall_readability_score": dale_chall_readability_score,
        "average_grade_level": avg_grade
    } 