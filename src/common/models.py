from typing import List, Optional
from datetime import date
from pydantic import BaseModel, Field, conint
from pydantic import validator

class BookCatalogItem(BaseModel):
    book_id: str
    isbn: str | None = None
    title: str
    genre: List[str] | str | None = None
    keywords: List[str] | str | None = None
    description: Optional[str] = None
    page_count: Optional[int] = None
    publication_year: Optional[int] = None
    difficulty_band: Optional[str] = None
    average_student_rating: Optional[float] = Field(None, ge=0.0, le=5.0)

    # validators ------------------------------------------------------
    @validator("genre", pre=True)
    def _ensure_list_genre(cls, v):
        if v is None:
            return []
        if isinstance(v, str):
            try:
                import json
                return json.loads(v)
            except Exception:
                return [v]
        return v

    @validator("keywords", pre=True)
    def _ensure_list_keywords(cls, v):
        if v is None:
            return []
        if isinstance(v, str):
            try:
                import json
                return json.loads(v)
            except Exception:
                return [v]
        return v

class StudentRecord(BaseModel):
    student_id: str
    grade_level: conint(ge=4, le=4) = 4
    age: conint(ge=9, le=10)
    homeroom_teacher: str
    prior_year_reading_score: Optional[float] = Field(None, ge=0.0, le=5.0)
    lunch_period: conint(ge=1, le=2)

class CheckoutRecord(BaseModel):
    student_id: str
    book_id: str
    checkout_date: date
    return_date: Optional[date]
    student_rating: Optional[conint(ge=1, le=5)] 