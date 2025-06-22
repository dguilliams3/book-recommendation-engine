"""
Book‑level enrichment cascade.
Per manifest: Google Books (2 rpm) ➜ Open Library (4 rpm) ➜ readability fallback.
"""
import asyncio, logging, aiohttp, asyncpg, time
from common import SettingsInstance as S
from tools import readability_formula_estimator

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("book_enrichment")

GB_PER_MIN = 2
OL_PER_MIN = 4

async def google_books(isbn, session):
    key_param = f"&key={S.google_books_api_key}" if S.google_books_api_key else ""
    url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}{key_param}"
    async with session.get(url) as r:
        if r.status != 200:
            return {}
        return await r.json()

async def open_library(isbn, session):
    url = f"https://openlibrary.org/isbn/{isbn}.json"
    async with session.get(url) as r:
        if r.status != 200:
            return {}
        return await r.json()

async def main():
    pg_url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(pg_url)
    books = await conn.fetch("SELECT book_id,isbn,description FROM catalog "
                             "WHERE page_count IS NULL OR publication_year IS NULL")
    sem_gb = asyncio.Semaphore(GB_PER_MIN)
    sem_ol = asyncio.Semaphore(OL_PER_MIN)

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as ses:
        for row in books:
            meta = {}
            if row["isbn"]:
                async with sem_gb:
                    meta = await google_books(row["isbn"], ses)
                await asyncio.sleep(60/GB_PER_MIN)
            if not meta and row["isbn"]:
                async with sem_ol:
                    meta = await open_library(row["isbn"], ses)
                await asyncio.sleep(60/OL_PER_MIN)

            # extract meta fields
            page = None
            year = None
            if meta:
                vi = meta.get("items", [{}])[0].get("volumeInfo", {}) if isinstance(meta, dict) else meta
                page = vi.get("pageCount")
                pub = vi.get("publishedDate", "")
                if isinstance(pub, str):
                    year = pub[:4]
                elif "publish_date" in meta:
                    year = meta.get("publish_date", "")[ -4: ]
            if page is None or not year or not year.isdigit():
                rb = readability_formula_estimator(row["description"] or "")
            else:
                rb = {}
            await conn.execute(
                "UPDATE catalog SET page_count=$1, publication_year=$2, difficulty_band=COALESCE(difficulty_band,$3)"
                " WHERE book_id=$4",
                page, int(year) if year and year.isdigit() else None,
                rb.get("difficulty_band"), row["book_id"])
    await conn.close()
    log.info("book enrichment complete")

if __name__ == "__main__":
    asyncio.run(main()) 