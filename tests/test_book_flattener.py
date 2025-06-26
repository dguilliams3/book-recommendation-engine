from embedding.book import BookFlattener


def test_book_flattener_basic():
    row = {
        "book_id": "B1",
        "title": "Test Title",
        "description": "Great book.",
        "genre": ["fantasy"],
        "keywords": ["magic"],
        "reading_level": 4.0,
    }
    text, meta = BookFlattener()(row)
    assert "Test Title" in text
    assert "Great book." in text
    assert "fantasy" in text
    assert "magic" in text
    assert "4th grade" in text
    assert meta["grade_label"] == "4th grade"
    assert meta["book_id"] == "B1" 