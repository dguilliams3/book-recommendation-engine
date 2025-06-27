from typing import List, Optional
from datetime import date
from pydantic import BaseModel, Field, conint, field_validator, ConfigDict

class BookCatalogItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    book_id: str
    isbn: str | None = None
    title: str
    author: Optional[str] = None
    genre: List[str] = Field(default_factory=list)
    keywords: List[str] = Field(default_factory=list)
    description: Optional[str] = None
    page_count: Optional[int] = None
    publication_year: Optional[int] = None
    difficulty_band: Optional[str] = None
    reading_level: Optional[float] = Field(None, ge=0.0, le=12.0)
    average_student_rating: Optional[float] = Field(None, ge=0.0, le=5.0)

    # validators ------------------------------------------------------
    @field_validator("genre", mode="before")
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

    @field_validator("keywords", mode="before")
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
    checkout_id: Optional[str] = None
    student_id: str
    book_id: str
    checkout_date: date
    return_date: Optional[date]
    student_rating: Optional[conint(ge=1, le=5)] 