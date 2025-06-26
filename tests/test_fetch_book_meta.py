import asyncio, json
from types import SimpleNamespace

import pytest

from recommendation_api.tools.fetch_google_books_meta import fetch_google_books_meta
from recommendation_api.tools.fetch_open_library_meta import fetch_open_library_meta


class _FakeResp:
    def __init__(self, status: int, payload: dict):
        self.status = status
        self._payload = payload

    async def json(self):
        return self._payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeSession:
    def __init__(self, expect_url: str, payload: dict, status: int = 200):
        self.expect_url = expect_url
        self.payload = payload
        self.status = status
        self.requested_url = None

    def get(self, url):  # noqa: D401 – mimics aiohttp API
        self.requested_url = url
        assert self.expect_url.split("?")[0] in url  # allow query params
        return _FakeResp(self.status, self.payload)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.mark.asyncio
async def test_fetch_google_books_meta_success(monkeypatch):
    isbn = "1234567890"
    fake_payload = {
        "items": [
            {
                "volumeInfo": {
                    "title": "Test Title",
                    "authors": ["A"],
                    "pageCount": 200,
                    "publishedDate": "2010-01-01",
                    "description": "A book.",
                    "categories": ["Fiction"],
                    "averageRating": 4.5,
                    "ratingsCount": 10,
                }
            }
        ]
    }

    def _fake_client_session(*args, **kwargs):
        return _FakeSession(
            expect_url=f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}",
            payload=fake_payload,
        )

    monkeypatch.setattr("aiohttp.ClientSession", _fake_client_session)

    meta = await fetch_google_books_meta(isbn)
    assert meta["title"] == "Test Title"
    assert meta["page_count"] == 200
    assert meta["publication_year"] == "2010"


@pytest.mark.asyncio
async def test_fetch_open_library_meta_success(monkeypatch):
    isbn = "1111111111"
    fake_payload = {
        "title": "OpenLib Title",
        "publish_date": "2005",
        "number_of_pages": 150,
    }

    def _fake_client_session(*args, **kwargs):
        return _FakeSession(
            expect_url=f"https://openlibrary.org/isbn/{isbn}.json",
            payload=fake_payload,
        )

    monkeypatch.setattr("aiohttp.ClientSession", _fake_client_session)

    meta = await fetch_open_library_meta(isbn)
    assert meta["title"] == "OpenLib Title"
    assert meta["publication_year"] == "2005"


@pytest.mark.asyncio
async def test_fetch_google_books_meta_not_found(monkeypatch):
    isbn = "000"
    def _fake_client_session(*args, **kwargs):
        return _FakeSession(
            expect_url="https://www.googleapis.com/books/v1/volumes?q=isbn:000",
            payload={},
            status=404,
        )

    monkeypatch.setattr("aiohttp.ClientSession", _fake_client_session)

    meta = await fetch_google_books_meta(isbn)
    assert meta is None 