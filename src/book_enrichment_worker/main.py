"""
Book‑level enrichment cascade.
Per manifest: Google Books (2 rpm) ➜ Open Library (4 rpm) ➜ readability fallback.
"""
import asyncio, aiohttp, asyncpg, time, sys
from pathlib import Path

# Add src to Python path so we can find the common module when run directly
sys.path.insert(0, str(Path(__file__).parent.parent))

from common import SettingsInstance as S
from common.structured_logging import get_logger
from recommendation_api.tools import readability_formula_estimator

logger = get_logger(__name__)

GB_PER_MIN = 2
OL_PER_MIN = 4

async def google_books(isbn, session):
    logger.debug("Fetching from Google Books", extra={"isbn": isbn})
    key_param = f"&key={S.google_books_api_key}" if S.google_books_api_key else ""
    url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}{key_param}"
    
    try:
        async with session.get(url) as r:
            if r.status != 200:
                logger.warning("Google Books API returned non-200 status", extra={
                    "isbn": isbn,
                    "status": r.status,
                    "url": url
                })
                return {}
            
            data = await r.json()
            logger.debug("Google Books API response", extra={
                "isbn": isbn,
                "has_items": bool(data.get("items")),
                "total_items": len(data.get("items", []))
            })
            return data
            
    except Exception as e:
        logger.error("Failed to fetch from Google Books", exc_info=True, extra={"isbn": isbn, "url": url})
        return {}

async def open_library(isbn, session):
    logger.debug("Fetching from Open Library", extra={"isbn": isbn})
    url = f"https://openlibrary.org/isbn/{isbn}.json"
    
    try:
        async with session.get(url) as r:
            if r.status != 200:
                logger.warning("Open Library API returned non-200 status", extra={
                    "isbn": isbn,
                    "status": r.status,
                    "url": url
                })
                return {}
            
            data = await r.json()
            logger.debug("Open Library API response", extra={
                "isbn": isbn,
                "has_title": bool(data.get("title")),
                "has_publish_date": bool(data.get("publish_date"))
            })
            return data
            
    except Exception as e:
        logger.error("Failed to fetch from Open Library", exc_info=True, extra={"isbn": isbn, "url": url})
        return {}

async def main():
    logger.info("Starting book enrichment process")
    logger.info("Rate limits", extra={"google_books_rpm": GB_PER_MIN, "open_library_rpm": OL_PER_MIN})
    
    # Connect to database
    logger.info("Connecting to database")
    pg_url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
    try:
        conn = await asyncpg.connect(pg_url)
        logger.info("Database connection established")
    except Exception as e:
        logger.error("Failed to connect to database", exc_info=True)
        raise
    
    # Fetch books needing enrichment
    logger.info("Fetching books requiring enrichment")
    try:
        books = await conn.fetch("SELECT book_id,isbn,description FROM catalog "
                                 "WHERE page_count IS NULL OR publication_year IS NULL")
        logger.info("Found books requiring enrichment", extra={"count": len(books)})
    except Exception as e:
        logger.error("Failed to fetch books from database", exc_info=True)
        await conn.close()
        raise
    
    # Setup rate limiting
    sem_gb = asyncio.Semaphore(GB_PER_MIN)
    sem_ol = asyncio.Semaphore(OL_PER_MIN)
    
    processed_count = 0
    enriched_count = 0
    google_books_success = 0
    open_library_success = 0
    readability_fallback = 0
    
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as ses:
        logger.info("Starting enrichment loop")
        
        for row in books:
            processed_count += 1
            book_id = row["book_id"]
            isbn = row["isbn"]
            description = row["description"]
            
            logger.debug("Processing book", extra={
                "book_id": book_id,
                "isbn": isbn,
                "processed_count": processed_count,
                "total_books": len(books)
            })
            
            meta = {}
            
            # Try Google Books first
            if isbn:
                logger.debug("Attempting Google Books enrichment", extra={"book_id": book_id, "isbn": isbn})
                async with sem_gb:
                    meta = await google_books(isbn, ses)
                await asyncio.sleep(60/GB_PER_MIN)
                
                if meta:
                    google_books_success += 1
                    logger.debug("Google Books enrichment successful", extra={"book_id": book_id})
            
            # Fallback to Open Library
            if not meta and isbn:
                logger.debug("Attempting Open Library enrichment", extra={"book_id": book_id, "isbn": isbn})
                async with sem_ol:
                    meta = await open_library(isbn, ses)
                await asyncio.sleep(60/OL_PER_MIN)
                
                if meta:
                    open_library_success += 1
                    logger.debug("Open Library enrichment successful", extra={"book_id": book_id})
            
            # Extract metadata
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
                
                logger.debug("Extracted metadata", extra={
                    "book_id": book_id,
                    "page_count": page,
                    "publication_year": year
                })
            
            # Fallback to readability formula
            if page is None or not year or not year.isdigit():
                logger.debug("Using readability formula fallback", extra={"book_id": book_id})
                rb = readability_formula_estimator(description or "")
                readability_fallback += 1
            else:
                rb = {}
            
            # Update database
            try:
                await conn.execute(
                    "UPDATE catalog SET page_count=$1, publication_year=$2, difficulty_band=COALESCE(difficulty_band,$3)"
                    " WHERE book_id=$4",
                    page, int(year) if year and year.isdigit() else None,
                    rb.get("difficulty_band"), book_id)
                
                enriched_count += 1
                logger.debug("Database updated successfully", extra={"book_id": book_id})
                
            except Exception as e:
                logger.error("Failed to update database", exc_info=True, extra={"book_id": book_id})
            
            # Progress logging
            if processed_count % 10 == 0:
                logger.info("Enrichment progress", extra={
                    "processed": processed_count,
                    "total": len(books),
                    "enriched": enriched_count,
                    "google_books_success": google_books_success,
                    "open_library_success": open_library_success,
                    "readability_fallback": readability_fallback
                })
    
    # Final summary
    await conn.close()
    logger.info("Book enrichment process completed", extra={
        "total_processed": processed_count,
        "total_enriched": enriched_count,
        "google_books_success": google_books_success,
        "open_library_success": open_library_success,
        "readability_fallback": readability_fallback
    })

if __name__ == "__main__":
    logger.info("Starting book enrichment worker")
    try:
        asyncio.run(main())
        logger.info("Book enrichment worker completed successfully")
    except KeyboardInterrupt:
        logger.info("Book enrichment worker interrupted by user")
    except Exception as e:
        logger.error("Book enrichment worker failed", exc_info=True)
        raise 