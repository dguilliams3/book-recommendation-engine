from typing import List, Optional
from datetime import date
from pydantic import BaseModel, Field, conint

class BookCatalogItem(BaseModel):
    book_id: str
    title: str
    genre: List[str]
    keywords: List[str]
    description: Optional[str] = None
    average_student_rating: Optional[float] = Field(None, ge=0.0, le=5.0)

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