import json

from common.models import BookCatalogItem


def test_genre_and_keywords_accept_string_json():
    item = BookCatalogItem(
        book_id="B1", isbn="", title="T", genre='["mystery"]', keywords='["fun"]'
    )
    assert item.genre == ["mystery"]
    assert item.keywords == ["fun"]


def test_genre_and_keywords_accept_plain_string():
    item = BookCatalogItem(
        book_id="B2", isbn="", title="T", genre="mystery", keywords="fun"
    )
    assert item.genre == ["mystery"]
    assert item.keywords == ["fun"]


def test_genre_and_keywords_none_defaults_to_empty():
    item = BookCatalogItem(book_id="B3", isbn="", title="T")
    assert item.genre == []
    assert item.keywords == [] 