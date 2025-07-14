import asyncio, json, os, uuid, shutil
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

logger = get_logger(__name__)

VECTOR_DIR = S.vector_store_dir
VECTOR_DIR.mkdir(parents=True, exist_ok=True)
LOCK_FILE = VECTOR_DIR / "index.lock"

embeddings = OpenAIEmbeddings(api_key=S.openai_api_key, model=S.embedding_model)


async def ensure_store():
    if (VECTOR_DIR / "index.faiss").exists():
        return FAISS.load_local(
            VECTOR_DIR, embeddings, allow_dangerous_deserialization=True
        )
    return FAISS.from_texts(["dummy"], embeddings, metadatas=[{"book_id": "dummy"}])


async def update_faiss_index_atomic(
    texts: list[str], metadatas: list[dict], event_id: str
):
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

            logger.info(
                "FAISS index updated atomically",
                extra={
                    "event_id": event_id,
                    "texts_added": len(texts),
                    "index_size": store.index.ntotal,
                },
            )

    except Exception as e:
        logger.error(
            "FAISS index update failed", exc_info=True, extra={"event_id": event_id}
        )

        # Restore from backup if it exists
        if backup_path.exists():
            try:
                if VECTOR_DIR.exists():
                    shutil.rmtree(VECTOR_DIR)
                shutil.move(backup_path, VECTOR_DIR)
                logger.info(
                    "FAISS index restored from backup", extra={"event_id": event_id}
                )
            except Exception as restore_error:
                logger.error(
                    "Failed to restore FAISS index from backup",
                    exc_info=True,
                    extra={"event_id": event_id},
                )

        # Clean up temp directory
        if temp_path.exists():
            shutil.rmtree(temp_path)

        raise  # Re-raise original exception


async def update_book_embeddings_table(book_ids: list[str], event_id: str = None):
    """Update the book_embeddings table with audit trail."""
    if event_id is None:
        event_id = str(uuid.uuid4())

    pg_url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(pg_url)

    try:
        # Update last_event for all processed books
        await conn.executemany(
            """INSERT INTO book_embeddings (book_id, last_event) VALUES ($1, $2)
               ON CONFLICT (book_id) DO UPDATE SET last_event = $2""",
            [(book_id, event_id) for book_id in book_ids],
        )

        logger.debug(
            "Book embeddings table updated",
            extra={"book_ids": book_ids, "event_id": event_id, "count": len(book_ids)},
        )

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
        """SELECT book_id,title,author,genre,difficulty_band,reading_level,publication_year,description
           FROM catalog WHERE book_id = ANY($1::text[])""",
        ids,
    )
    await conn.close()
    if not rows:
        logger.warning(
            "No matching books found", extra={"ids": ids, "event_id": event_id}
        )
        return

    texts = []
    metadatas = []
    for r in rows:
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
        texts.append(text)

        metadatas.append(
            {
                "book_id": r["book_id"],
                "genre": genres_str,
                "level": r["reading_level"],
            }
        )

    # Embed and add to FAISS with atomic operations
    await update_faiss_index_atomic(texts, metadatas, event_id)

    # Update audit trail in database
    processed_ids = [r["book_id"] for r in rows]
    await update_book_embeddings_table(processed_ids, event_id)

    logger.info(
        "Book vectors updated",
        extra={
            "added": len(rows),
            "index_size": store.index.ntotal,
            "event_id": event_id,
        },
    )


async def main():
    consumer = KafkaEventConsumer(BOOK_EVENTS_TOPIC, "book_vector_worker")
    await consumer.start(handle_book_event)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
