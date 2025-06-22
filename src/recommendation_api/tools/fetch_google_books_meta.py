import aiohttp
import asyncio
from typing import Dict, Optional

async def fetch_google_books_meta(isbn: str) -> Optional[Dict]:
    """Fetch book metadata from Google Books API."""
    url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                if data.get("items"):
                    volume_info = data["items"][0].get("volumeInfo", {})
                    return {
                        "title": volume_info.get("title"),
                        "authors": volume_info.get("authors", []),
                        "page_count": volume_info.get("pageCount"),
                        "publication_year": volume_info.get("publishedDate", "")[:4],
                        "description": volume_info.get("description"),
                        "categories": volume_info.get("categories", []),
                        "average_rating": volume_info.get("averageRating"),
                        "ratings_count": volume_info.get("ratingsCount"),
                    }
    return None 