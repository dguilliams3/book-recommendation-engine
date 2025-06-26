from __future__ import annotations

import random
import uuid
from datetime import date, timedelta
from typing import Any, Dict

from common import models


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def make_book(**overrides: Any) -> models.BookCatalogItem:
    """Return a `BookCatalogItem` with deterministic but unique defaults.

    Parameters can be overridden by keyword arguments to tailor the object for
    specific test cases.
    """
    idx = random.randint(1, 9999)
    defaults: Dict[str, Any] = {
        "book_id": overrides.get("book_id", f"B{idx:04d}"),
        "isbn": overrides.get("isbn", str(random.randint(10**9, 10**10 - 1))),
        "title": overrides.get("title", f"Sample Book {idx}"),
        "genre": overrides.get("genre", ["fiction"]),
        "keywords": overrides.get("keywords", ["sample", "test"]),
        "description": overrides.get("description", "A fascinating tale used for testing."),
        "page_count": overrides.get("page_count", 120),
        "publication_year": overrides.get("publication_year", 2020),
        "difficulty_band": overrides.get("difficulty_band", "late_elementary"),
        "reading_level": overrides.get("reading_level", 3.0),
        "average_student_rating": overrides.get("average_student_rating", 3.5),
    }
    defaults.update(overrides)
    return models.BookCatalogItem(**defaults)


def make_student(**overrides: Any) -> models.StudentRecord:
    idx = random.randint(1, 9999)
    defaults: Dict[str, Any] = {
        "student_id": overrides.get("student_id", f"S{idx:04d}"),
        "grade_level": overrides.get("grade_level", 4),
        "age": overrides.get("age", 9),
        "homeroom_teacher": overrides.get("homeroom_teacher", "Ms. Test"),
        "prior_year_reading_score": overrides.get("prior_year_reading_score", 3.0),
        "lunch_period": overrides.get("lunch_period", 1),
    }
    defaults.update(overrides)
    return models.StudentRecord(**defaults)


def make_checkout(**overrides: Any) -> models.CheckoutRecord:
    today = date.today()
    defaults: Dict[str, Any] = {
        "student_id": overrides.get("student_id", f"S{random.randint(1, 9999):04d}"),
        "book_id": overrides.get("book_id", f"B{random.randint(1, 9999):04d}"),
        "checkout_date": overrides.get("checkout_date", today - timedelta(days=7)),
        "return_date": overrides.get("return_date", today),
        "student_rating": overrides.get("student_rating", random.randint(1, 5)),
    }
    defaults.update(overrides)
    return models.CheckoutRecord(**defaults) 