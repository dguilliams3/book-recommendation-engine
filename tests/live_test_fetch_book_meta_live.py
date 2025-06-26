import os, asyncio

import pytest

from recommendation_api.tools.fetch_google_books_meta import fetch_google_books_meta
from recommendation_api.tools.fetch_open_library_meta import fetch_open_library_meta


LIVE_FLAG = os.getenv("RUN_LIVE_API_TESTS") == "1"

pytestmark = pytest.mark.skipif(not LIVE_FLAG, reason="live API tests disabled – set RUN_LIVE_API_TESTS=1 to enable")

ISBN_SAMPLE = "9780547928227"  # The Hobbit – common ISBN present in both APIs


@pytest.mark.asyncio
async def test_google_books_live():
    meta = await fetch_google_books_meta(ISBN_SAMPLE)
    assert meta and meta["title"].lower().startswith("the hobbit")
    assert meta["page_count"] and int(meta["publication_year"]) >= 1937


@pytest.mark.asyncio
async def test_open_library_live():
    meta = await fetch_open_library_meta(ISBN_SAMPLE)
    assert meta and "hobbit" in meta["title"].lower() 