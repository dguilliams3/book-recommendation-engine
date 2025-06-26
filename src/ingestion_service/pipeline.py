"""High-level ingestion workflow extracted from `main.py`.

Keeping this logic in its own module makes `main.py` a clean entry-point while
allowing unit tests to import :func:`run_ingestion` directly.
"""
from __future__ import annotations

import asyncio, csv, json, sys, time, uuid
from pathlib import Path
from typing import Iterable

from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy import text

from common.settings import SettingsInstance as S
from common import models
from common.structured_logging import get_logger
from common.kafka_utils import event_producer
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


async def _publish(payload):  # fire-and-forget helper
    try:
        await event_producer.publish_event(TOPIC, payload)
        logger.debug("Metric published", extra={"payload": payload})
    except Exception:
        logger.error("Failed to publish metric", exc_info=True)


async def run_ingestion():
    """Load CSVs → Postgres and build/update FAISS index.

    This function is exactly what used to be in `ingestion_service.main.ingest`.
    It is isolated here so callers (tests, CLI wrappers) can invoke it without
    dragging along CLI/async setup code.
    """

    vec_dir = S.data_dir / "vector_store"
    vec_dir.mkdir(parents=True, exist_ok=True)

    embeddings = OpenAIEmbeddings(api_key=S.openai_api_key, model=S.embedding_model)

    store = None
    if (vec_dir / "index.faiss").exists():
        logger.info("Loading existing FAISS index")
        store = FAISS.load_local(vec_dir, embeddings, allow_dangerous_deserialization=True)
        logger.info("FAISS index loaded", extra={"index_size": store.index.ntotal})

    # Database engine ---------------------------------------------------------
    db_url_str = str(S.db_url)
    # Ensure we have an asyncpg driver URL for SQLAlchemy
    if db_url_str.startswith("postgresql://"):
        async_db_url = db_url_str.replace("postgresql://", "postgresql+asyncpg://")
    elif db_url_str.startswith("postgresql+psycopg2://"):
        async_db_url = db_url_str.replace("postgresql+psycopg2://", "postgresql+asyncpg://")
    else:
        async_db_url = db_url_str

    engine = create_async_engine(async_db_url, echo=False)
    await _bootstrap_schema(engine)

    start = time.perf_counter()

    async with AsyncSession(engine) as sess:
        # -------- books ------------------------------------------------------
        book_texts: list[str] = []
        book_metadatas: list[dict] = []
        flattener = BookFlattener()
        book_count = 0
        for row in _load_csv(Path("data/catalog_sample.csv")):
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
                        logger.warning("Bad reading_level value; estimating", extra={"value": row["reading_level"]})
                if rl is None:
                    # readability_formula_estimator removed – reading level now supplied via CSV
                    pass

                # Remove original reading_level to avoid duplicate kwarg
                row_clean = row.copy()
                row_clean.pop("reading_level", None)

                item = models.BookCatalogItem(**row_clean, reading_level=rl)

                await sess.execute(
                    text(
                        """INSERT INTO catalog(book_id,isbn,title,genre,keywords,description,page_count,publication_year,difficulty_band,reading_level,average_student_rating)
                            VALUES(:book_id,:isbn,:title,:genre,:keywords,:description,:page_count,:publication_year,:difficulty_band,:reading_level,:rating)
                            ON CONFLICT(book_id) DO UPDATE SET
                                isbn=EXCLUDED.isbn,
                                genre=EXCLUDED.genre,
                                keywords=EXCLUDED.keywords,
                                description=EXCLUDED.description,
                                page_count=COALESCE(EXCLUDED.page_count, catalog.page_count),
                                publication_year=COALESCE(EXCLUDED.publication_year, catalog.publication_year),
                                difficulty_band=COALESCE(EXCLUDED.difficulty_band, catalog.difficulty_band),
                                reading_level=COALESCE(EXCLUDED.reading_level, catalog.reading_level),
                                average_student_rating=EXCLUDED.average_student_rating"""
                    ),
                    {
                        "book_id": item.book_id,
                        "isbn": item.isbn,
                        "title": item.title,
                        "genre": json.dumps(item.genre),
                        "keywords": json.dumps(item.keywords),
                        "description": item.description,
                        "page_count": item.page_count,
                        "publication_year": item.publication_year,
                        "difficulty_band": item.difficulty_band,
                        "reading_level": rl,
                        "rating": item.average_student_rating,
                    },
                )

                doc_text, meta = flattener(row | {"reading_level": rl, "book_id": item.book_id})
                book_texts.append(doc_text)
                book_metadatas.append(meta)
                book_count += 1
            except Exception:
                logger.error("Failed to process book row", exc_info=True)
        logger.info("Books processed", extra={"count": book_count})

        # Emit event
        if book_count:
            await event_producer.publish_event(
                BOOK_EVENTS_TOPIC, BookAddedEvent(count=book_count, book_ids=[m["book_id"] for m in book_metadatas]).dict()
            )

        # Build or extend FAISS ------------------------------------------------
        if book_texts and store is None:
            store = FAISS.from_texts(book_texts, embeddings, metadatas=book_metadatas)
        elif store is not None and book_texts:
            store.add_texts(book_texts, metadatas=book_metadatas)

        # -------- students ---------------------------------------------------
        student_count = 0
        for row in _load_csv(Path("data/students_sample.csv")):
            try:
                stu = models.StudentRecord(**row)
                await sess.execute(
                    text(
                        "INSERT INTO students VALUES(:student_id,:grade,:age,:teacher,:score,:lunch)"
                        "ON CONFLICT(student_id) DO UPDATE SET grade_level=EXCLUDED.grade_level, age=EXCLUDED.age, homeroom_teacher=EXCLUDED.homeroom_teacher, prior_year_reading_score=EXCLUDED.prior_year_reading_score, lunch_period=EXCLUDED.lunch_period"
                    ),
                    {
                        "student_id": stu.student_id,
                        "grade": stu.grade_level,
                        "age": stu.age,
                        "teacher": stu.homeroom_teacher,
                        "score": stu.prior_year_reading_score,
                        "lunch": stu.lunch_period,
                    },
                )
                student_count += 1
            except Exception:
                logger.error("Failed to process student row", exc_info=True)
        logger.info("Students processed", extra={"count": student_count})

        if student_count:
            await event_producer.publish_event(
                STUDENT_EVENTS_TOPIC,
                StudentsAddedEvent(count=student_count).model_dump(),
            )

        # -------- checkouts --------------------------------------------------
        checkout_count = 0
        for row in _load_csv(Path("data/checkouts_sample.csv")):
            try:
                chk = models.CheckoutRecord(**row)
                await sess.execute(
                    text(
                        "INSERT INTO checkout VALUES(:sid,:bid,:out,:back,:rating)"
                        "ON CONFLICT (student_id, book_id, checkout_date) DO UPDATE SET return_date=EXCLUDED.return_date, student_rating=EXCLUDED.student_rating"
                    ),
                    {
                        "sid": chk.student_id,
                        "bid": chk.book_id,
                        "out": chk.checkout_date,
                        "back": chk.return_date,
                        "rating": chk.student_rating,
                    },
                )
                checkout_count += 1

                await event_producer.publish_event(
                    CHECKOUT_EVENTS_TOPIC,
                    CheckoutAddedEvent(student_id=chk.student_id, book_id=chk.book_id, checkout_date=str(chk.checkout_date)).dict(),
                )
            except Exception:
                logger.error("Failed checkout row", exc_info=True)
        logger.info("Checkouts processed", extra={"count": checkout_count})

        await sess.commit()

    # Persist FAISS index if built/updated -----------------------------------
    if store is not None:
        store.save_local(vec_dir)

    duration = round(time.perf_counter() - start, 2)
    await _publish(
        {
            "event": "ingestion_complete",
            "duration": duration,
            "rows_ingested": store.index.ntotal if store else 0,
            "request_id": uuid.uuid4().hex,
            "timestamp": time.time(),
            "books_processed": book_count,
            "students_processed": student_count,
            "checkouts_processed": checkout_count,
        }
    )

    await event_producer.close()
    logger.info("Ingestion finished", extra={"duration_sec": duration}) 