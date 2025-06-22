import aiohttp
import asyncio
from typing import Dict, Optional

async def fetch_open_library_meta(isbn: str) -> Optional[Dict]:
    """Fetch book metadata from Open Library API."""
    url = f"https://openlibrary.org/isbn/{isbn}.json"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return {
                    "title": data.get("title"),
                    "authors": data.get("authors", []),
                    "page_count": data.get("number_of_pages"),
                    "publication_year": data.get("publish_date", "")[-4:],
                    "description": data.get("description", {}).get("value") if isinstance(data.get("description"), dict) else data.get("description"),
                    "subjects": data.get("subjects", []),
                    "publishers": data.get("publishers", []),
                }
    return None 