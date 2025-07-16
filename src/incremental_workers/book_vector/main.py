import asyncio, json, os, uuid, shutil, hashlib
from pathlib import Path
from filelock import FileLock
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import FAISS
from aiokafka import AIOKafkaConsumer
import asyncpg
from common.settings import settings as S
from common.kafka_utils import KafkaEventConsumer, publish_event
from common.events import BOOK_EVENTS_TOPIC, BookAddedEvent, BookUpdatedEvent
from common.structured_logging import get_logger
import time
import threading
from fastapi import FastAPI, Request, HTTPException
import uvicorn

logger = get_logger(__name__)

async def trigger_book_enrichment(book_id: str, priority: int = 2, reason: str = "embedding_generation") -> None:
    """Trigger on-demand enrichment for a book when missing metadata is needed for embeddings.
    
    Args:
        book_id: Book to enrich
        priority: Priority level (default: 2=high for worker requests)
        reason: Reason for enrichment request
    """
    try:
        import uuid
        enrichment_request = {
            'event_type': 'book_enrichment_requested',
            'request_id': str(uuid.uuid4()),
            'book_id': book_id,
            'priority': priority,
            'reason': reason,
            'requester': 'book_vector_worker',
            'timestamp': time.time()
        }
        
        await publish_event('book_enrichment_requests', enrichment_request)
        logger.info(f"Triggered enrichment for book {book_id} (priority {priority}, reason: {reason})")
        
    except Exception as e:
        logger.error(f"Failed to trigger enrichment for book {book_id}: {e}")

VECTOR_DIR = S.vector_store_dir
VECTOR_DIR.mkdir(parents=True, exist_ok=True)
LOCK_FILE = VECTOR_DIR / "index.lock"

embeddings = OpenAIEmbeddings(api_key=S.openai_api_key, model=S.embedding_model)


def compute_embedding_hash(book_data: dict) -> str:
    """Compute a hash of the book data used for embedding generation"""
    # Create a deterministic string representation of the embedding-relevant fields
    embedding_content = {
        "title": book_data.get("title", ""),
        "author": book_data.get("author", ""),
        "genre": book_data.get("genre", ""),
        "description": book_data.get("description", ""),
        "reading_level": book_data.get("reading_level"),
        "difficulty_band": book_data.get("difficulty_band", ""),
        "publication_year": book_data.get("publication_year"),
    }
    content_str = json.dumps(embedding_content, sort_keys=True, default=str)
    return hashlib.sha256(content_str.encode()).hexdigest()


async def ensure_store():
    if (VECTOR_DIR / "index.faiss").exists():
        return FAISS.load_local(
            VECTOR_DIR, embeddings, allow_dangerous_deserialization=True
        )
    return FAISS.from_texts(["dummy"], embeddings, metadatas=[{"book_id": "dummy"}])


async def update_faiss_index_atomic(
    texts: list[str], metadatas: list[dict], event_id: str
) -> int:
    """Atomically update FAISS index with backup/restore mechanism."""
    backup_path = VECTOR_DIR.parent / f"{VECTOR_DIR.name}.backup"
    temp_path = VECTOR_DIR.parent / f"{VECTOR_DIR.name}.temp"

    # Clean up any existing temp/backup directories
    if backup_path.exists():
        shutil.rmtree(backup_path)
    if temp_path.exists():
        shutil.rmtree(temp_path)

    try:
        with FileLock(str(LOCK_FILE)):
            # Create backup of current index
            if VECTOR_DIR.exists():
                shutil.copytree(VECTOR_DIR, backup_path)
                logger.debug("FAISS index backup created", extra={"event_id": event_id})

            # Load current store
            store = await ensure_store()

            # Add new texts
            store.add_texts(texts, metadatas=metadatas)

            # Save to temporary location first
            store.save_local(temp_path)

            # Atomic move: replace current index with updated one
            if VECTOR_DIR.exists():
                shutil.rmtree(VECTOR_DIR)
            shutil.move(temp_path, VECTOR_DIR)

            # Remove backup on success
            if backup_path.exists():
                shutil.rmtree(backup_path)

            index_size = store.index.ntotal
            logger.info(
                "FAISS index updated atomically",
                extra={
                    "event_id": event_id,
                    "texts_added": len(texts),
                    "index_size": index_size,
                },
            )
            
            return index_size
    except Exception as e:
        # Restore from backup on failure
        if backup_path.exists() and VECTOR_DIR.exists():
            shutil.rmtree(VECTOR_DIR)
            shutil.move(backup_path, VECTOR_DIR)
            logger.error("FAISS index restored from backup after failure", extra={"event_id": event_id})
        raise e


async def update_book_embeddings_table(book_ids: list[str], embedding_hashes: list[str], event_id: str = None):
    """Update the book_embeddings table with audit trail and content hashes."""
    if event_id is None:
        event_id = str(uuid.uuid4())

    pg_url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(pg_url)

    try:
        # Update last_event and content_hash for all processed books
        await conn.executemany(
            """INSERT INTO book_embeddings (book_id, last_event, content_hash) VALUES ($1, $2, $3)
               ON CONFLICT (book_id) DO UPDATE SET last_event = $2, content_hash = $3""",
            [(book_id, event_id, embedding_hash) for book_id, embedding_hash in zip(book_ids, embedding_hashes)],
        )

        logger.debug(
            "Book embeddings table updated",
            extra={"book_ids": book_ids, "event_id": event_id, "count": len(book_ids)},
        )

    finally:
        await conn.close()


async def check_existing_embeddings(book_ids: list[str]) -> dict[str, str]:
    """Check which books already have embeddings and their content hashes"""
    pg_url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(pg_url)
    
    try:
        # Get existing embeddings and their content hashes
        rows = await conn.fetch(
            """SELECT book_id, content_hash FROM book_embeddings WHERE book_id = ANY($1::text[])""",
            book_ids
        )
        
        existing_embeddings = {row["book_id"]: row["content_hash"] for row in rows}
        logger.debug(f"Found {len(existing_embeddings)} existing embeddings")
        return existing_embeddings
        
    finally:
        await conn.close()


async def handle_book_event(evt: dict):
    etype = evt.get("event_type")
    if etype not in {"books_added", "book_updated"}:
        return

    # Generate unique event ID for traceability
    event_id = str(uuid.uuid4())

    logger.info("Processing book event", extra={**evt, "event_id": event_id})
    ids = evt.get("book_ids") or [evt.get("book_id")]
    if not ids:
        return

    # Fetch book data from DB lazily to avoid large event payloads
    pg_url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(pg_url)
    rows = await conn.fetch(
        """SELECT book_id,title,author,genre,difficulty_band,reading_level,publication_year,description,isbn,page_count
           FROM catalog WHERE book_id = ANY($1::text[])""",
        ids,
    )
    await conn.close()
    if not rows:
        logger.warning(
            "No matching books found", extra={"ids": ids, "event_id": event_id}
        )
        return

    # Check existing embeddings to determine what needs processing
    existing_embeddings = await check_existing_embeddings(ids)
    
    texts = []
    metadatas = []
    processed_book_ids = []
    processed_embedding_hashes = []
    skipped_count = 0
    
    for r in rows:
        book_id = r["book_id"]
        
        # Check if book needs enrichment and trigger it
        needs_enrichment = (
            r["publication_year"] is None or
            r["page_count"] is None or
            not r["isbn"] or r["isbn"] == ''
        )
        
        if needs_enrichment:
            await trigger_book_enrichment(book_id)
        
        # Parse genre JSON array stored as text – fallback to empty list on error
        try:
            genre_list = json.loads(r["genre"]) if r["genre"] else []
        except Exception:
            genre_list = []
        genres_str = ", ".join(genre_list)

        desc = r["description"] or ""
        text = (
            f"{r['title']} by {r['author']}. "
            f"Genre: {genres_str}. "
            f"Reading level: {r['reading_level']} ({r['difficulty_band']}). "
            f"Published {r['publication_year']}. "
            f"{desc}"
        )
        
        # Compute embedding hash for this book
        book_data = {
            "title": r["title"],
            "author": r["author"],
            "genre": genres_str,
            "description": desc,
            "reading_level": r["reading_level"],
            "difficulty_band": r["difficulty_band"],
            "publication_year": r["publication_year"],
        }
        embedding_hash = compute_embedding_hash(book_data)
        
        # Check if embedding already exists and hasn't changed
        if book_id in existing_embeddings and existing_embeddings[book_id] == embedding_hash:
            # Embedding exists and content hasn't changed - skip processing
            skipped_count += 1
            logger.debug(f"Book {book_id} embedding unchanged, skipping", extra={"book_id": book_id, "event_id": event_id})
            continue
        
        # Book needs embedding generation or update
        texts.append(text)
        metadatas.append(
            {
                "book_id": book_id,
                "genre": genres_str,
                "level": r["reading_level"],
            }
        )
        processed_book_ids.append(book_id)
        processed_embedding_hashes.append(embedding_hash)
        
        logger.debug(f"Book {book_id} will be processed", extra={"book_id": book_id, "event_id": event_id})

    if not texts:
        logger.info(
            "No books require embedding updates",
            extra={"total_books": len(rows), "skipped": skipped_count, "event_id": event_id}
        )
        return

    # Embed and add to FAISS with atomic operations
    index_size = await update_faiss_index_atomic(texts, metadatas, event_id)

    # Update audit trail in database
    await update_book_embeddings_table(processed_book_ids, processed_embedding_hashes, event_id)

    logger.info(
        "Book vectors updated",
        extra={
            "processed": len(processed_book_ids),
            "skipped": skipped_count,
            "index_size": index_size,
            "event_id": event_id,
        },
    )

async def validate_and_sync_faiss_index():
    """Validate FAISS index and rebuild if needed from Postgres data.
    
    Handles fresh starts (no book_embeddings table) and ensures consistency
    between Postgres catalog and FAISS index.
    """
    logger.info("Starting FAISS index validation and synchronization...")
    
    pg_url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(pg_url)
    
    try:
        # Check if book_embeddings table exists
        table_exists = await conn.fetchval(
            """SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'book_embeddings'
            )"""
        )
        
        if not table_exists:
            logger.info("book_embeddings table does not exist - this is a fresh start")
            # For fresh start, we'll let the Kafka events handle embedding generation
            # Just ensure we have a clean FAISS index
            if VECTOR_DIR.exists():
                shutil.rmtree(VECTOR_DIR)
                logger.info("Removed existing FAISS index for fresh start")
            return
        
        # Get count of books with embeddings
        embedding_count = await conn.fetchval("SELECT COUNT(*) FROM book_embeddings")
        logger.info(f"Found {embedding_count} books with embeddings in database")
        
        # Check if FAISS index exists and get its size
        faiss_exists = VECTOR_DIR.exists() and (VECTOR_DIR / "index.faiss").exists()
        
        if faiss_exists:
            try:
                store = await ensure_store()
                faiss_count = store.index.ntotal
                logger.info(f"FAISS index exists with {faiss_count} embeddings")
                
                # Compare counts
                if faiss_count != embedding_count:
                    logger.warning(
                        f"FAISS index count ({faiss_count}) doesn't match database count ({embedding_count}) - rebuilding"
                    )
                    await full_faiss_rebuild()
                else:
                    logger.info("FAISS index is consistent with database")
            except Exception as e:
                logger.error(f"Error reading FAISS index: {e} - rebuilding")
                await full_faiss_rebuild()
        else:
            logger.info("FAISS index doesn't exist - rebuilding from database")
            await full_faiss_rebuild()
            
    except Exception as e:
        logger.error(f"Error during FAISS validation: {e}")
        # Don't fail startup - let Kafka events handle embedding generation
    finally:
        await conn.close()

REBUILD_TOKEN = os.getenv("REBUILD_TOKEN", "changeme")

app = FastAPI()

@app.post("/rebuild")
async def rebuild_index(request: Request):
    token = request.headers.get("x-rebuild-token")
    if token != REBUILD_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        await full_faiss_rebuild()
        return {"status": "success"}
    except Exception as e:
        logger.error(f"FAISS rebuild failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def full_faiss_rebuild():
    """Rebuild the FAISS index from all books in the catalog."""
    logger.info("Starting full FAISS index rebuild...")
    from langchain_openai import OpenAIEmbeddings
    from langchain_community.vectorstores import FAISS
    import asyncpg
    from filelock import FileLock
    # Fetch all books
    pg_url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(pg_url)
    rows = await conn.fetch(
        """SELECT book_id,title,author,genre,difficulty_band,reading_level,publication_year,description,isbn,page_count FROM catalog"""
    )
    await conn.close()
    if not rows:
        logger.warning("No books found for FAISS rebuild")
        return
    texts = []
    metadatas = []
    for r in rows:
        try:
            genre_list = json.loads(r["genre"]) if r["genre"] else []
        except Exception:
            genre_list = []
        genres_str = ", ".join(genre_list)
        desc = r["description"] or ""
        text = (
            f"{r['title']} by {r['author']}. "
            f"Genre: {genres_str}. "
            f"Reading level: {r['reading_level']} ({r['difficulty_band']}). "
            f"Published {r['publication_year']}. "
            f"{desc}"
        )
        texts.append(text)
        metadatas.append({
            "book_id": r["book_id"],
            "genre": genres_str,
            "level": r["reading_level"],
        })
    with FileLock(str(LOCK_FILE)):
        embeddings = OpenAIEmbeddings(api_key=S.openai_api_key, model=S.embedding_model)
        store = FAISS.from_texts(texts, embeddings, metadatas=metadatas)
        store.save_local(VECTOR_DIR)
    logger.info(f"FAISS index rebuilt with {len(texts)} books.")

def run_fastapi():
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="info")

async def main():
    # Validate and sync FAISS index on startup
    await validate_and_sync_faiss_index()
    
    # Start FastAPI in a background thread
    threading.Thread(target=run_fastapi, daemon=True).start()
    consumer = KafkaEventConsumer(BOOK_EVENTS_TOPIC, "book_vector_worker")
    await consumer.start(handle_book_event)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
