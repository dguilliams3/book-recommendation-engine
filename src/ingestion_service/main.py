"""
1. Ensure schema exists (executes 00_init_schema.sql once).
2. Validate & coerce CSV rows → Pydantic models.
3. Upsert rows into Postgres and build / update FAISS embeddings.
"""

import asyncio, csv, json, time, uuid, sys
from pathlib import Path
from typing import Iterable
from recommendation_api.tools.readability_formula_estimator import readability_formula_estimator

# Add src to Python path so we can find the common module when run directly
sys.path.insert(0, str(Path(__file__).parent.parent))

from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import text

from common.settings import SettingsInstance as S
from common import models
from common.structured_logging import get_logger
from common.kafka_utils import event_producer
from common.events import (BookAddedEvent, BOOK_EVENTS_TOPIC, StudentAddedEvent,
                            StudentUpdatedEvent, CheckoutAddedEvent, STUDENT_EVENTS_TOPIC,
                            CHECKOUT_EVENTS_TOPIC)
from .csv_utils import _load_csv
from .db_utils import _bootstrap_schema

logger = get_logger(__name__)
TOPIC = "ingestion_metrics"

async def _publish(payload):  # fire-and-forget
    try:
        await event_producer.publish_event(TOPIC, payload)
        logger.debug("Metric published", extra={"payload": payload})
    except Exception as e:
        logger.error("Failed to publish metric", exc_info=True, extra={"payload": payload})

async def ingest():
    logger.info("Starting data ingestion process")
    
    # Setup vector store
    vec_dir = S.data_dir / "vector_store"
    vec_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Vector store directory prepared", extra={"vector_dir": str(vec_dir)})
    
    # Initialize embeddings
    logger.info("Initializing OpenAI embeddings")
    embeddings = OpenAIEmbeddings(api_key=S.openai_api_key, model=S.embedding_model)
    
    # Load or create FAISS store (LangChain wrapper expects embeddings arg)
    if (vec_dir / "index.faiss").exists():
        logger.info("Loading existing FAISS index")
        store = FAISS.load_local(vec_dir, embeddings, allow_dangerous_deserialization=True)
        logger.info("FAISS index loaded successfully", extra={"index_size": store.index.ntotal})
    else:
        logger.info("Creating new FAISS index")
        # We'll build the index from real data during processing
        store = None
        logger.info("FAISS index will be created from book data")

    # Setup database
    logger.info("Initializing database connection")
    # Ensure we have the correct async URL format for asyncpg
    db_url_str = str(S.db_url)
    if db_url_str.startswith("postgresql://"):
        async_db_url = db_url_str.replace("postgresql://", "postgresql+asyncpg://")
    elif db_url_str.startswith("postgresql+asyncpg://"):
        async_db_url = db_url_str
    else:
        async_db_url = db_url_str
    engine = create_async_engine(async_db_url, echo=False)
    await _bootstrap_schema(engine)

    # Debug: Log the Kafka bootstrap servers being used
    logger.info(f"Connecting to Kafka at: {S.kafka_bootstrap}")
    
    start = time.perf_counter()

    async with AsyncSession(engine) as sess:
        # books ----------------------------------------------------------
        logger.info("Processing books catalog")
        book_count = 0
        book_texts = []
        book_metadatas = []
        
        for row in _load_csv(Path("data/catalog_sample.csv")):
            try:
                # Ensure genre/keywords are always lists or JSON strings
                for field in ["genre", "keywords"]:
                    v = row.get(field)
                    if isinstance(v, list):
                        row[field] = json.dumps(v)
                    elif isinstance(v, str):
                        try:
                            # Try to parse as JSON; if fails, treat as single-item list
                            json.loads(v)
                        except Exception:
                            row[field] = json.dumps([v])
                    elif v is None:
                        row[field] = json.dumps([])
                
                # Compute FKGL reading level from description
                reading_meta = readability_formula_estimator(row.get("description", ""))
                reading_level_val = reading_meta.get("average_grade_level")

                item = models.BookCatalogItem(**row, reading_level=reading_level_val)
                await sess.execute(
                    text("""INSERT INTO catalog(book_id,isbn,title,genre,keywords,description,page_count,publication_year,difficulty_band,reading_level,average_student_rating)
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
                            average_student_rating=EXCLUDED.average_student_rating"""),
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
                        "reading_level": reading_level_val,
                        "rating": item.average_student_rating,
                    },
                )
                
                # Collect text and metadata for FAISS index
                book_texts.append(f"{item.title}. {item.description or ''}")
                book_metadatas.append({"book_id": item.book_id, "reading_level": reading_level_val})
                book_count += 1
                
                if book_count % 100 == 0:
                    logger.debug("Processed books", extra={"count": book_count})
                    
            except Exception as e:
                logger.error("Failed to process book", exc_info=True, extra={"book_id": row.get("book_id"), "row": row})
                continue
        
        logger.info("Books processing completed", extra={"total_books": book_count})
        
        # Publish book added event if new books were processed
        if book_count > 0:
            try:
                book_event = BookAddedEvent(
                    count=book_count,
                    book_ids=[metadata["book_id"] for metadata in book_metadatas]
                )
                success = await event_producer.publish_event(BOOK_EVENTS_TOPIC, book_event.dict())
                if success:
                    logger.info("Published book added event", extra={"book_count": book_count})
                else:
                    logger.warning("Failed to publish book added event", extra={"book_count": book_count})
            except Exception as e:
                logger.error("Error publishing book added event", exc_info=True, extra={"book_count": book_count})
        
        # Build FAISS index from collected book data
        if book_texts and store is None:
            logger.info("Building FAISS index from book data")
            store = FAISS.from_texts(book_texts, embeddings, metadatas=book_metadatas)
            logger.info("FAISS index built successfully", extra={"index_size": store.index.ntotal})
        elif store is not None and book_texts:
            logger.info("Adding new books to existing FAISS index")
            store.add_texts(book_texts, metadatas=book_metadatas)
            logger.info("Books added to FAISS index", extra={"index_size": store.index.ntotal})
        elif not book_texts:
            logger.warning("No books found; skipping FAISS index creation")

        # students -------------------------------------------------------
        logger.info("Processing students data")
        student_count = 0
        for row in _load_csv(Path("data/students_sample.csv")):
            try:
                stu = models.StudentRecord(**row)
                await sess.execute(
                    text("INSERT INTO students VALUES(:student_id,:grade,:age,:teacher,:score,:lunch)"
                        "ON CONFLICT(student_id) DO UPDATE SET "
                        "grade_level=EXCLUDED.grade_level, age=EXCLUDED.age, homeroom_teacher=EXCLUDED.homeroom_teacher, "
                        "prior_year_reading_score=EXCLUDED.prior_year_reading_score, lunch_period=EXCLUDED.lunch_period"),
                    {"student_id": stu.student_id, "grade": stu.grade_level, "age": stu.age,
                    "teacher": stu.homeroom_teacher, "score": stu.prior_year_reading_score,
                    "lunch": stu.lunch_period}
                )
                student_count += 1
                    
                if student_count % 100 == 0:
                    logger.debug("Processed students", extra={"count": student_count})
                    
                # Publish student added event
                try:
                    s_event = StudentAddedEvent(student_id=stu.student_id)
                    await event_producer.publish_event(STUDENT_EVENTS_TOPIC, s_event.dict())
                except Exception:
                    logger.warning("Failed to publish student added event", extra={"student_id": stu.student_id})

            except Exception as e:
                logger.error("Failed to process student", exc_info=True, extra={"student_id": row.get("student_id"), "row": row})
                continue
        
        logger.info("Students processing completed", extra={"total_students": student_count})
        
        # Publish student events
        if student_count > 0:
            try:
                stu_event = StudentAddedEvent(count=student_count if hasattr(StudentAddedEvent, 'count') else None)  # placeholder not needed
            except Exception:
                pass
        
        # checkouts ------------------------------------------------------
        logger.info("Processing checkout data")
        checkout_count = 0
        for row in _load_csv(Path("data/checkouts_sample.csv")):
            try:
                chk = models.CheckoutRecord(**row)
                # Only insert if book and student exist
                book_exists = await sess.execute(text("SELECT 1 FROM catalog WHERE book_id=:bid"), {"bid": chk.book_id})
                student_exists = await sess.execute(text("SELECT 1 FROM students WHERE student_id=:sid"), {"sid": chk.student_id})
                if book_exists.scalar() and student_exists.scalar():
                    await sess.execute(
                        text("INSERT INTO checkout VALUES(:sid,:bid,:out,:back,:rating)"
                            "ON CONFLICT (student_id, book_id, checkout_date) DO UPDATE SET "
                            "return_date=EXCLUDED.return_date, student_rating=EXCLUDED.student_rating"),
                        {"sid": chk.student_id, "bid": chk.book_id, "out": chk.checkout_date,
                        "back": chk.return_date, "rating": chk.student_rating}
                    )
                    checkout_count += 1

                    # Publish checkout added event
                    try:
                        co_event = CheckoutAddedEvent(student_id=chk.student_id, book_id=chk.book_id, checkout_date=str(chk.checkout_date))
                        await event_producer.publish_event(CHECKOUT_EVENTS_TOPIC, co_event.dict())
                    except Exception:
                        logger.warning("Failed to publish checkout event", extra={"student_id": chk.student_id, "book_id": chk.book_id})

                else:
                    logger.warning("Skipping checkout: missing book or student", extra={"student_id": chk.student_id, "book_id": chk.book_id})
                    
                if checkout_count % 100 == 0:
                    logger.debug("Processed checkouts", extra={"count": checkout_count})
                        
            except Exception as e:
                logger.error("Failed to process checkout", exc_info=True, extra={"student_id": row.get("student_id"), "book_id": row.get("book_id"), "row": row})
                continue
        
        logger.info("Checkouts processing completed", extra={"total_checkouts": checkout_count})
        
        # Commit all changes
        logger.info("Committing database changes")
        await sess.commit()
        logger.info("Database changes committed successfully")

    # Save vector store
    if store is not None:
        logger.info("Saving FAISS index")
        store.save_local(vec_dir)
        logger.info("FAISS index saved successfully", extra={"index_size": store.index.ntotal})
    else:
        logger.info("No FAISS index to save")
    
    # Final metrics
    duration = round(time.perf_counter() - start, 2)
    final_payload = {
                "event": "ingestion_complete",
                "duration": duration,
                "rows_ingested": store.index.ntotal if store else 0,
                "request_id": uuid.uuid4().hex,
                "timestamp": time.time(),
        "books_processed": book_count,
        "students_processed": student_count,
        "checkouts_processed": checkout_count,
    }
    
    await _publish(final_payload)
    
    logger.info("Ingestion process completed successfully", extra={
        "duration_sec": duration,
        "total_vectors": store.index.ntotal if store else 0,
        "books_processed": book_count,
        "students_processed": student_count,
        "checkouts_processed": checkout_count,
    })
    
    # Cleanup event producer
    await event_producer.close()

if __name__ == "__main__":
    logger.info("Starting ingestion service")
    try:
        asyncio.run(ingest()) 
        logger.info("Ingestion service completed successfully")
    except Exception as e:
        logger.error("Ingestion service failed", exc_info=True)
        raise 