"""High-level ingestion workflow extracted from `main.py`.

Keeping this logic in its own module makes `main.py` a clean entry-point while
allowing unit tests to import :func:`run_ingestion` directly.
"""

from __future__ import annotations

import asyncio, csv, json, sys, time, uuid, hashlib
from pathlib import Path
from typing import Iterable, Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import date

from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy import text

from common.settings import settings as S
from common import models
from common.structured_logging import get_logger
from common.kafka_utils import publish_event
from common.events import (
    BookAddedEvent,
    BOOK_EVENTS_TOPIC,
    StudentAddedEvent,
    CheckoutAddedEvent,
    STUDENT_EVENTS_TOPIC,
    CHECKOUT_EVENTS_TOPIC,
    StudentsAddedEvent,
)
from common.reading_level_utils import numeric_to_grade_text

from .csv_utils import _load_csv
from .db_utils import _bootstrap_schema
from embedding.book import BookFlattener

logger = get_logger(__name__)
TOPIC = "ingestion_metrics"


@dataclass
class ProcessingStats:
    """Track processing statistics for idempotency checks"""
    books_processed: int = 0
    books_skipped: int = 0
    books_updated: int = 0
    students_processed: int = 0
    students_skipped: int = 0
    students_updated: int = 0
    checkouts_processed: int = 0
    checkouts_skipped: int = 0
    checkouts_updated: int = 0
    embeddings_generated: int = 0
    embeddings_skipped: int = 0
    faiss_updated: bool = False


async def _publish(payload):  # fire-and-forget helper
    try:
        await publish_event(TOPIC, payload)
        logger.debug("Metric published", extra={"payload": payload})
    except Exception:
        logger.error("Failed to publish metric", exc_info=True)


def compute_content_hash(data: Dict) -> str:
    """Compute a hash of the content to detect changes"""
    # Create a deterministic string representation
    content_str = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(content_str.encode()).hexdigest()


async def check_existing_book(session: AsyncSession, book_id: str, content_hash: str) -> Tuple[bool, Optional[str]]:
    """Check if book exists and if content has changed"""
    result = await session.execute(
        text("SELECT book_id, content_hash FROM catalog WHERE book_id = :book_id"),
        {"book_id": book_id}
    )
    row = result.fetchone()
    
    if not row:
        return False, None  # Book doesn't exist
    
    existing_hash = row.content_hash
    if existing_hash == content_hash:
        return True, existing_hash  # Book exists and content is identical
    
    return True, existing_hash  # Book exists but content has changed


async def check_existing_student(session: AsyncSession, student_id: str, content_hash: str) -> Tuple[bool, Optional[str]]:
    """Check if student exists and if content has changed"""
    result = await session.execute(
        text("SELECT student_id, content_hash FROM students WHERE student_id = :student_id"),
        {"student_id": student_id}
    )
    row = result.fetchone()
    
    if not row:
        return False, None  # Student doesn't exist
    
    existing_hash = row.content_hash
    if existing_hash == content_hash:
        return True, existing_hash  # Student exists and content is identical
    
    return True, existing_hash  # Student exists but content has changed


async def check_existing_checkout(session: AsyncSession, student_id: str, book_id: str, checkout_date: date, content_hash: str) -> Tuple[bool, Optional[str]]:
    """Check if checkout exists and if content has changed"""
    result = await session.execute(
        text("SELECT checkout_id, content_hash FROM checkout WHERE student_id = :student_id AND book_id = :book_id AND checkout_date = :checkout_date"),
        {"student_id": student_id, "book_id": book_id, "checkout_date": checkout_date}
    )
    row = result.fetchone()
    
    if not row:
        return False, None  # Checkout doesn't exist
    
    existing_hash = row.content_hash
    if existing_hash == content_hash:
        return True, existing_hash  # Checkout exists and content is identical
    
    return True, existing_hash  # Checkout exists but content has changed


async def check_existing_embedding(session: AsyncSession, book_id: str, content_hash: str) -> Tuple[bool, Optional[str]]:
    """Check if book embedding exists and if content has changed"""
    result = await session.execute(
        text("SELECT book_id, content_hash FROM book_embeddings WHERE book_id = :book_id"),
        {"book_id": book_id}
    )
    row = result.fetchone()
    
    if not row:
        return False, None  # Embedding doesn't exist
    
    existing_hash = row.content_hash
    if existing_hash == content_hash:
        return True, existing_hash  # Embedding exists and content is identical
    
    return True, existing_hash  # Embedding exists but content has changed


async def ensure_content_hash_columns(session: AsyncSession):
    """Ensure content_hash columns exist for idempotency checks"""
    try:
        # Add content_hash columns if they don't exist
        await session.execute(text("ALTER TABLE catalog ADD COLUMN IF NOT EXISTS content_hash TEXT"))
        await session.execute(text("ALTER TABLE students ADD COLUMN IF NOT EXISTS content_hash TEXT"))
        await session.execute(text("ALTER TABLE checkout ADD COLUMN IF NOT EXISTS content_hash TEXT"))
        await session.execute(text("ALTER TABLE book_embeddings ADD COLUMN IF NOT EXISTS content_hash TEXT"))
        
        # Create indexes for faster hash lookups
        await session.execute(text("CREATE INDEX IF NOT EXISTS idx_catalog_content_hash ON catalog(content_hash)"))
        await session.execute(text("CREATE INDEX IF NOT EXISTS idx_students_content_hash ON students(content_hash)"))
        await session.execute(text("CREATE INDEX IF NOT EXISTS idx_checkout_content_hash ON checkout(content_hash)"))
        await session.execute(text("CREATE INDEX IF NOT EXISTS idx_book_embeddings_content_hash ON book_embeddings(content_hash)"))
        
        logger.info("Content hash columns and indexes ensured")
    except Exception as e:
        logger.warning(f"Failed to ensure content hash columns: {e}")


async def run_ingestion():
    """Load CSVs → Postgres and build/update FAISS index with comprehensive idempotency checks.

    This function is exactly what used to be in `ingestion_service.main.ingest`.
    It is isolated here so callers (tests, CLI wrappers) can invoke it without
    dragging along CLI/async setup code.
    """

    vec_dir = S.vector_store_dir
    vec_dir.mkdir(parents=True, exist_ok=True)

    embeddings = OpenAIEmbeddings(api_key=S.openai_api_key, model=S.embedding_model)

    store = None
    if (vec_dir / "index.faiss").exists():
        logger.info("Loading existing FAISS index")
        store = FAISS.load_local(
            vec_dir, embeddings, allow_dangerous_deserialization=True
        )
        logger.info("FAISS index loaded", extra={"index_size": store.index.ntotal})

    # Database engine ---------------------------------------------------------
    async_db_url = S.async_db_url

    engine = create_async_engine(async_db_url, echo=False)
    await _bootstrap_schema(engine)

    start = time.perf_counter()
    stats = ProcessingStats()

    async with AsyncSession(engine) as sess:
        # Ensure content hash columns exist
        await ensure_content_hash_columns(sess)
        
        # -------- books ------------------------------------------------------
        book_texts: list[str] = []
        book_metadatas: list[dict] = []
        flattener = BookFlattener()
        
        for row in _load_csv(S.catalog_csv_path):
            try:
                # normalise genre/keywords to JSON strings
                for field in ["genre", "keywords"]:
                    v = row.get(field)
                    if isinstance(v, list):
                        row[field] = json.dumps(v)
                    elif isinstance(v, str):
                        try:
                            json.loads(v)
                        except Exception:
                            row[field] = json.dumps([v])
                    elif v is None:
                        row[field] = json.dumps([])

                # reading level ------------------------------------------
                rl: float | None = None
                if row.get("reading_level") not in (None, ""):
                    try:
                        rl = float(row["reading_level"])
                    except ValueError:
                        logger.warning(
                            "Bad reading_level value; estimating",
                            extra={"value": row["reading_level"]},
                        )
                if rl is None:
                    # readability_formula_estimator removed – reading level now supplied via CSV
                    pass

                # Remove original reading_level to avoid duplicate kwarg
                row_clean = row.copy()
                row_clean.pop("reading_level", None)
                
                # Fix for ISBN validation: Convert None values to empty strings
                # This prevents Pydantic validation errors for Optional[str] fields
                row_clean = {k: (v if v is not None else "") for k, v in row_clean.items()}

                item = models.BookCatalogItem(**row_clean, reading_level=rl)
                
                # Compute content hash for idempotency check
                book_content = {
                    "isbn": item.isbn,
                    "title": item.title,
                    "author": item.author,
                    "genre": item.genre,
                    "keywords": item.keywords,
                    "description": item.description,
                    "page_count": item.page_count,
                    "publication_year": item.publication_year,
                    "difficulty_band": item.difficulty_band,
                    "reading_level": rl,
                    "average_rating": item.average_rating,
                }
                content_hash = compute_content_hash(book_content)
                
                # Check if book exists and if content has changed
                exists, existing_hash = await check_existing_book(sess, item.book_id, content_hash)
                
                if exists and existing_hash == content_hash:
                    # Book exists and content is identical - skip processing
                    stats.books_skipped += 1
                    logger.debug(f"Book {item.book_id} unchanged, skipping", extra={"book_id": item.book_id})
                    continue
                
                # Book doesn't exist or content has changed - process it
                await sess.execute(
                    text(
                        """INSERT INTO catalog(book_id,isbn,title,author,genre,keywords,description,page_count,publication_year,difficulty_band,reading_level,average_rating,content_hash)
                            VALUES(:book_id,:isbn,:title,:author,:genre,:keywords,:description,:page_count,:publication_year,:difficulty_band,:reading_level,:average_rating,:content_hash)
                            ON CONFLICT(book_id) DO UPDATE SET
                                isbn=EXCLUDED.isbn,
                                author=EXCLUDED.author,
                                genre=EXCLUDED.genre,
                                keywords=EXCLUDED.keywords,
                                description=EXCLUDED.description,
                                page_count=COALESCE(EXCLUDED.page_count, catalog.page_count),
                                publication_year=COALESCE(EXCLUDED.publication_year, catalog.publication_year),
                                difficulty_band=COALESCE(EXCLUDED.difficulty_band, catalog.difficulty_band),
                                reading_level=COALESCE(EXCLUDED.reading_level, catalog.reading_level),
                                average_rating=EXCLUDED.average_rating,
                                content_hash=EXCLUDED.content_hash"""
                    ),
                    {
                        "book_id": item.book_id,
                        "isbn": item.isbn,
                        "title": item.title,
                        "author": item.author,
                        "genre": json.dumps(item.genre),
                        "keywords": json.dumps(item.keywords),
                        "description": item.description,
                        "page_count": item.page_count,
                        "publication_year": item.publication_year,
                        "difficulty_band": item.difficulty_band,
                        "reading_level": rl,
                        "average_rating": item.average_rating,
                        "content_hash": content_hash,
                    },
                )

                # Check if embedding needs to be generated
                embedding_content = {
                    "title": item.title,
                    "author": item.author,
                    "genre": item.genre,
                    "description": item.description,
                    "reading_level": rl,
                    "difficulty_band": item.difficulty_band,
                }
                embedding_hash = compute_content_hash(embedding_content)
                
                embedding_exists, existing_embedding_hash = await check_existing_embedding(sess, item.book_id, embedding_hash)
                
                if not embedding_exists or existing_embedding_hash != embedding_hash:
                    # Generate embedding text and metadata
                    doc_text, meta = flattener(
                        row | {"reading_level": rl, "book_id": item.book_id}
                    )
                    book_texts.append(doc_text)
                    book_metadatas.append(meta)
                    stats.embeddings_generated += 1
                    logger.debug(f"Book {item.book_id} embedding will be generated", extra={"book_id": item.book_id})
                else:
                    stats.embeddings_skipped += 1
                    logger.debug(f"Book {item.book_id} embedding unchanged, skipping", extra={"book_id": item.book_id})

                if exists:
                    stats.books_updated += 1
                else:
                    stats.books_processed += 1
                    
            except Exception:
                logger.error("Failed to process book row", exc_info=True)
        
        logger.info("Books processed", extra={
            "processed": stats.books_processed,
            "updated": stats.books_updated,
            "skipped": stats.books_skipped,
            "embeddings_generated": stats.embeddings_generated,
            "embeddings_skipped": stats.embeddings_skipped
        })

        # Emit event only if books were actually processed
        if stats.books_processed > 0 or stats.books_updated > 0:
            await publish_event(
                BOOK_EVENTS_TOPIC,
                BookAddedEvent(
                    count=stats.books_processed + stats.books_updated, 
                    book_ids=[m["book_id"] for m in book_metadatas]
                ).dict(),
            )

        # Build or extend FAISS only if new embeddings were generated
        if book_texts and store is None:
            store = FAISS.from_texts(book_texts, embeddings, metadatas=book_metadatas)
            stats.faiss_updated = True
            logger.info("New FAISS index created", extra={"texts_count": len(book_texts)})
        elif store is not None and book_texts:
            store.add_texts(book_texts, metadatas=book_metadatas)
            stats.faiss_updated = True
            logger.info("FAISS index extended", extra={"texts_added": len(book_texts)})
        else:
            logger.info("No new embeddings to add to FAISS index")

        # -------- students ---------------------------------------------------
        for row in _load_csv(S.students_csv_path):
            try:
                stu = models.StudentRecord(**row)
                
                # Compute content hash for idempotency check
                student_content = {
                    "grade_level": stu.grade_level,
                    "age": stu.age,
                    "homeroom_teacher": stu.homeroom_teacher,
                    "prior_year_reading_score": stu.prior_year_reading_score,
                    "lunch_period": str(stu.lunch_period),
                }
                content_hash = compute_content_hash(student_content)
                
                # Check if student exists and if content has changed
                exists, existing_hash = await check_existing_student(sess, stu.student_id, content_hash)
                
                if exists and existing_hash == content_hash:
                    # Student exists and content is identical - skip processing
                    stats.students_skipped += 1
                    logger.debug(f"Student {stu.student_id} unchanged, skipping", extra={"student_id": stu.student_id})
                    continue
                
                # Student doesn't exist or content has changed - process it
                logger.debug(f"Processing student {stu.student_id}")
                result = await sess.execute(
                    text(
                        "INSERT INTO students VALUES(:student_id,:grade,:age,:teacher,:score,:lunch,:content_hash) "
                        "ON CONFLICT(student_id) DO UPDATE SET "
                        "grade_level=EXCLUDED.grade_level, "
                        "age=EXCLUDED.age, "
                        "homeroom_teacher=EXCLUDED.homeroom_teacher, "
                        "prior_year_reading_score=EXCLUDED.prior_year_reading_score, "
                        "lunch_period=EXCLUDED.lunch_period, "
                        "content_hash=EXCLUDED.content_hash"
                    ),
                    {
                        "student_id": stu.student_id,
                        "grade": stu.grade_level,
                        "age": stu.age,
                        "teacher": stu.homeroom_teacher,
                        "score": stu.prior_year_reading_score,
                        "lunch": str(stu.lunch_period),
                        "content_hash": content_hash,
                    },
                )
                
                if exists:
                    stats.students_updated += 1
                    logger.debug(f"✓ Student {stu.student_id} updated successfully")
                else:
                    stats.students_processed += 1
                    logger.debug(f"✓ Student {stu.student_id} inserted successfully")
                    
            except Exception as e:
                logger.error(
                    f"Failed to process student row {row.get('student_id', 'unknown')}: {e}",
                    exc_info=True,
                )
        
        logger.info("Students processed", extra={
            "processed": stats.students_processed,
            "updated": stats.students_updated,
            "skipped": stats.students_skipped
        })

        if stats.students_processed > 0 or stats.students_updated > 0:
            await publish_event(
                STUDENT_EVENTS_TOPIC,
                StudentsAddedEvent(count=stats.students_processed + stats.students_updated).model_dump(),
            )

        # Commit books and students before processing checkouts so that FK constraints are satisfied even if checkouts fail
        await sess.commit()
        logger.info(
            "Books and students committed",
            extra={"books": stats.books_processed + stats.books_updated, "students": stats.students_processed + stats.students_updated},
        )

        # -------- checkouts --------------------------------------------------
        for row in _load_csv(S.checkouts_csv_path):
            try:
                chk = models.CheckoutRecord(**row)
                
                # Compute content hash for idempotency check
                checkout_content = {
                    "return_date": str(chk.return_date) if chk.return_date else None,
                    "student_rating": chk.student_rating,
                    "checkout_id": chk.checkout_id,
                }
                content_hash = compute_content_hash(checkout_content)
                
                # Check if checkout exists and if content has changed
                exists, existing_hash = await check_existing_checkout(sess, chk.student_id, chk.book_id, chk.checkout_date, content_hash)
                
                if exists and existing_hash == content_hash:
                    # Checkout exists and content is identical - skip processing
                    stats.checkouts_skipped += 1
                    logger.debug(f"Checkout {chk.student_id}-{chk.book_id}-{chk.checkout_date} unchanged, skipping")
                    continue
                
                # Checkout doesn't exist or content has changed - process it
                await sess.execute(
                    text(
                        "INSERT INTO checkout (student_id, book_id, checkout_date, return_date, student_rating, checkout_id, content_hash) "
                        "VALUES(:sid,:bid,:out,:back,:rating,:checkout_id,:content_hash) "
                        "ON CONFLICT (student_id, book_id, checkout_date) DO UPDATE SET "
                        "return_date=EXCLUDED.return_date, "
                        "student_rating=EXCLUDED.student_rating, "
                        "checkout_id=EXCLUDED.checkout_id, "
                        "content_hash=EXCLUDED.content_hash"
                    ),
                    {
                        "sid": chk.student_id,
                        "bid": chk.book_id,
                        "out": chk.checkout_date,
                        "back": chk.return_date,
                        "rating": chk.student_rating,
                        "checkout_id": chk.checkout_id,
                        "content_hash": content_hash,
                    },
                )
                
                if exists:
                    stats.checkouts_updated += 1
                else:
                    stats.checkouts_processed += 1

                await publish_event(
                    CHECKOUT_EVENTS_TOPIC,
                    CheckoutAddedEvent(
                        student_id=chk.student_id,
                        book_id=chk.book_id,
                        checkout_date=str(chk.checkout_date),
                    ).dict(),
                )
            except Exception:
                logger.error("Failed checkout row", exc_info=True)
                try:
                    await sess.rollback()
                except Exception:
                    logger.error("Session rollback failed", exc_info=True)
        
        logger.info("Checkouts processed", extra={
            "processed": stats.checkouts_processed,
            "updated": stats.checkouts_updated,
            "skipped": stats.checkouts_skipped
        })

        await sess.commit()

    # Persist FAISS index if built/updated -----------------------------------
    if store is not None and stats.faiss_updated:
        store.save_local(vec_dir)
        logger.info("FAISS index saved", extra={"index_size": store.index.ntotal})

    duration = round(time.perf_counter() - start, 2)
    await _publish(
        {
            "event": "ingestion_complete",
            "duration": duration,
            "books_processed": stats.books_processed,
            "books_updated": stats.books_updated,
            "books_skipped": stats.books_skipped,
            "students_processed": stats.students_processed,
            "students_updated": stats.students_updated,
            "students_skipped": stats.students_skipped,
            "checkouts_processed": stats.checkouts_processed,
            "checkouts_updated": stats.checkouts_updated,
            "checkouts_skipped": stats.checkouts_skipped,
            "embeddings_generated": stats.embeddings_generated,
            "embeddings_skipped": stats.embeddings_skipped,
            "faiss_index_updated": stats.faiss_updated,
        }
    )

    # Producer cleanup now handled automatically per event loop
    logger.info("Ingestion finished", extra={
        "duration_sec": duration,
        "total_processed": stats.books_processed + stats.students_processed + stats.checkouts_processed,
        "total_updated": stats.books_updated + stats.students_updated + stats.checkouts_updated,
        "total_skipped": stats.books_skipped + stats.students_skipped + stats.checkouts_skipped,
    })
