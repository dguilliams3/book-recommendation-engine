"""
Book‑level enrichment cascade.
Per manifest: Google Books (2 rpm) ➜ Open Library (4 rpm) ➜ readability fallback.
"""
import asyncio, aiohttp, asyncpg, time, sys, os, json
from pathlib import Path
from typing import List, Dict, Any

# Add src to Python path so we can find the common module when run directly
sys.path.insert(0, str(Path(__file__).parent.parent))

from common import SettingsInstance as S
from common.structured_logging import get_logger
from common.tools import readability_formula_estimator
from common.events import BookUpdatedEvent, BOOK_EVENTS_TOPIC
from common.kafka_utils import publish_event
from openai import AsyncOpenAI

logger = get_logger(__name__)

GB_PER_MIN = 2
OL_PER_MIN = 4

# ---------------------------------------------------------------------------
# LLM Configuration
# ---------------------------------------------------------------------------

LLM_MODEL = os.getenv("BOOK_DESC_LLM_MODEL", "gpt-4o-mini")  # Still cost-effective for bulk processing
_openai_client: AsyncOpenAI | None = None


async def _get_openai_client() -> AsyncOpenAI:
    global _openai_client
    if _openai_client is None:
        _openai_client = AsyncOpenAI(api_key=S.openai_api_key)
    return _openai_client


async def llm_description(title: str, author: str | None, genres: list[str], year: str | int | None) -> str | None:
    """Call the LLM once to generate a <=50-word spoiler-free description.

    Returns None on failure or if the model responds with UNKNOWN.
    """
    # Build prompt – keep it deterministic and safe
    genres_str = ", ".join(genres) if genres else "unknown"
    prompt = (
        "You are a school librarian. "
        "Write a spoiler-free, 40-50 word description of the book below. "
        "If you are unsure, reply exactly: UNKNOWN\n\n"
        f"Title: \"{title}\"\n"
        f"Author: \"{author or 'Unknown'}\"\n"
        f"Genres: {genres_str}\n"
        f"Published: {year if year else 'unknown'}"
    )

    try:
        oc = await _get_openai_client()
        rsp = await oc.chat.completions.create(
            model=LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=80,
            temperature=0.7,
        )
        desc = rsp.choices[0].message.content.strip()
        if desc.upper().startswith("UNKNOWN"):
            return None
        # Word count guard – naive split is fine
        if 20 <= len(desc.split()) <= 60:
            return desc
        # Otherwise truncate politely
        return " ".join(desc.split()[:55]) + "..."
    except Exception:
        logger.warning("LLM description generation failed", exc_info=True, extra={"title": title})
        return None

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

# ---------------------------------------------------------------------------
# Batch Processing Configuration
# ---------------------------------------------------------------------------

BATCH_SIZE = int(os.getenv("BOOK_DESC_BATCH_SIZE", "50"))  # Process books in chunks
MAX_CONCURRENT_REQUESTS = int(os.getenv("BOOK_DESC_MAX_CONCURRENT", "8"))  # Rate limiting


async def process_book_descriptions_batch(books: List[Dict[str, Any]]) -> Dict[str, str]:
    """Process multiple books concurrently for description generation.
    
    Returns a mapping of book_id -> description (or None if failed/unknown)
    """
    if not books:
        return {}
    
    logger.info("Starting batch description processing", extra={
        "book_count": len(books),
        "batch_size": BATCH_SIZE,
        "max_concurrent": MAX_CONCURRENT_REQUESTS
    })
    
    # Process in smaller batches to avoid overwhelming the API
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    results = {}
    
    async def process_single_book(book: Dict[str, Any]) -> tuple[str, str | None]:
        """Process a single book with rate limiting."""
        async with semaphore:
            book_id = book["book_id"]
            try:
                description = await llm_description(
                    book["title"], 
                    book["author"], 
                    book.get("genres", []), 
                    book.get("publication_year")
                )
                return book_id, description
            except Exception as e:
                logger.warning("Failed to process book description", extra={
                    "book_id": book_id,
                    "error": str(e)
                })
                return book_id, None
    
    # Process all books concurrently
    tasks = [process_single_book(book) for book in books]
    batch_results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Collect results
    for result in batch_results:
        if isinstance(result, Exception):
            logger.error("Task failed with exception", exc_info=True)
            continue
        book_id, description = result
        results[book_id] = description
    
    logger.info("Batch description processing completed", extra={
        "processed": len(results),
        "successful": len([d for d in results.values() if d is not None])
    })
    
    return results


async def enrich_all_missing_descriptions():
    """Enrich all books missing descriptions in the catalog."""
    logger.info("Starting bulk description enrichment")
    
    # Connect to database with retry logic
    pg_url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
    
    max_retries = 10
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            conn = await asyncpg.connect(pg_url)
            logger.info("Database connection established for bulk enrichment")
            break
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Bulk enrichment DB connection attempt {attempt + 1} failed, retrying in {retry_delay}s", extra={
                    "attempt": attempt + 1,
                    "max_retries": max_retries,
                    "error": str(e)
                })
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 1.5, 30)
            else:
                logger.error("Failed to connect to database for bulk enrichment after all retries", exc_info=True)
                raise
    
    try:
        # Fetch all books missing descriptions
        books = await conn.fetch("""
            SELECT book_id, title, author, genre, publication_year
            FROM catalog 
            WHERE description IS NULL
            ORDER BY book_id
        """)
        
        if not books:
            logger.info("No books missing descriptions found")
            return
        
        logger.info("Found books missing descriptions", extra={"count": len(books)})
        
        # Convert to list of dicts and parse genres
        book_list = []
        for row in books:
            try:
                genres = json.loads(row["genre"]) if row["genre"] else []
            except Exception:
                genres = []
            
            book_list.append({
                "book_id": row["book_id"],
                "title": row["title"],
                "author": row["author"],
                "genres": genres,
                "publication_year": row["publication_year"]
            })
        
        # Process in batches
        total_processed = 0
        total_updated = 0
        
        for i in range(0, len(book_list), BATCH_SIZE):
            batch = book_list[i:i + BATCH_SIZE]
            logger.info("Processing batch", extra={
                "batch_num": i // BATCH_SIZE + 1,
                "batch_size": len(batch),
                "total_batches": (len(book_list) + BATCH_SIZE - 1) // BATCH_SIZE
            })
            
            # Get descriptions for this batch
            descriptions = await process_book_descriptions_batch(batch)
            
            # Update database for this batch
            for book_id, description in descriptions.items():
                if description is not None:
                    await conn.execute(
                        "UPDATE catalog SET description = $1 WHERE book_id = $2",
                        description, book_id
                    )
                    total_updated += 1
                    
                    # Publish update event
                    try:
                        upd_evt = BookUpdatedEvent(
                            book_id=book_id, 
                            payload={"description_added": True}
                        )
                        await publish_event(BOOK_EVENTS_TOPIC, upd_evt.model_dump())
                    except Exception:
                        logger.warning("Failed to publish book updated event", extra={"book_id": book_id})
                
                total_processed += 1
            
            # Small delay between batches to be nice to the API
            if i + BATCH_SIZE < len(book_list):
                await asyncio.sleep(1)
        
        logger.info("Bulk description enrichment completed", extra={
            "total_processed": total_processed,
            "total_updated": total_updated,
            "success_rate": f"{(total_updated/total_processed)*100:.1f}%" if total_processed > 0 else "0%"
        })
        
    finally:
        await conn.close()


async def main():
    logger.info("Starting book enrichment process")
    logger.info("Rate limits", extra={"google_books_rpm": GB_PER_MIN, "open_library_rpm": OL_PER_MIN})
    
    # Check if we should do bulk description enrichment
    if os.getenv("BULK_DESCRIPTION_ENRICHMENT") == "1":
        logger.info("Running bulk description enrichment mode")
        await enrich_all_missing_descriptions()
        return
    
    # Original enrichment logic continues below...
    logger.info("Running standard enrichment mode")
    
    # Connect to database with retry logic
    logger.info("Connecting to database")
    pg_url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
    
    max_retries = 10
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            conn = await asyncpg.connect(pg_url)
            logger.info("Database connection established")
            break
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Database connection attempt {attempt + 1} failed, retrying in {retry_delay}s", extra={
                    "attempt": attempt + 1,
                    "max_retries": max_retries,
                    "error": str(e)
                })
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 1.5, 30)  # Exponential backoff, max 30s
            else:
                logger.error("Failed to connect to database after all retries", exc_info=True)
                raise
    
    # Fetch books needing enrichment
    logger.info("Fetching books requiring enrichment")
    try:
        books = await conn.fetch(
            """SELECT book_id,isbn,description,title,author,genre,publication_year,page_count
                   FROM catalog
                  WHERE page_count IS NULL
                     OR publication_year IS NULL
                     OR description IS NULL"""
        )
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
            title = row["title"]
            author = row["author"]
            genre_json = row["genre"]
            try:
                genres = json.loads(genre_json) if genre_json else []
            except Exception:
                genres = []
            pub_year = row["publication_year"]
            
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
                
                # Validate page count - reject 0 or invalid values
                if page is not None and (not isinstance(page, int) or page <= 0):
                    logger.debug("Invalid page count received, treating as None", extra={
                        "book_id": book_id,
                        "invalid_page_count": page
                    })
                    page = None
                
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
            
            # LLM description fallback --------------------------------------
            if not description or len(str(description)) > 300:
                description = await llm_description(title, author, genres, pub_year or year)

            # Fallback to readability formula (difficulty_band only)
            if page is None or not year or not year.isdigit():
                logger.debug("Using readability formula fallback", extra={"book_id": book_id})
                rb = readability_formula_estimator(description or "")
                readability_fallback += 1
            else:
                rb = {}
            
            # Update database
            try:
                await conn.execute(
                    "UPDATE catalog SET page_count=$1, publication_year=$2, description=COALESCE(description,$3), difficulty_band=COALESCE(difficulty_band,$4)"
                    " WHERE book_id=$5",
                    page, int(year) if year and year.isdigit() else None,
                    description, rb.get("difficulty_band"), book_id)
                
                enriched_count += 1
                logger.debug("Database updated successfully", extra={"book_id": book_id})
                
                # Publish BookUpdated event
                try:
                    upd_evt = BookUpdatedEvent(book_id=book_id, payload={"page_count": page, "publication_year": year, "description_added": bool(description)})
                    await publish_event(BOOK_EVENTS_TOPIC, upd_evt.model_dump())
                except Exception:
                    logger.warning("Failed to publish book updated event", extra={"book_id": book_id})
            
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