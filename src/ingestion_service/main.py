"""
1. Ensure schema exists (executes 00_init_schema.sql once).
2. Validate & coerce CSV rows → Pydantic models.
3. Upsert rows into Postgres and build / update FAISS embeddings.
"""

import asyncio, csv, json, time, uuid
from pathlib import Path
from typing import Iterable

from aiokafka import AIOKafkaProducer
from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import text

from common import SettingsInstance as S, models
from common.logging import get_logger

logger = get_logger(__name__)
TOPIC = "ingestion_metrics"

async def _publish(k, payload):  # fire-and-forget
    try:
        await k.send_and_wait(TOPIC, json.dumps(payload).encode())
        logger.debug("Metric published", extra={"payload": payload})
    except Exception as e:
        logger.error("Failed to publish metric", exc_info=True, extra={"payload": payload})

def _load_csv(path: Path) -> Iterable[dict]:
    logger.debug("Loading CSV file", extra={"file_path": str(path)})
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            row_count = 0
            for row in reader:
                row_count += 1
                yield {k: (v or None) for k, v in row.items()}
        logger.info("CSV file loaded successfully", extra={"file_path": str(path), "row_count": row_count})
    except Exception as e:
        logger.error("Failed to load CSV file", exc_info=True, extra={"file_path": str(path)})
        raise

async def _bootstrap_schema(engine):
    sql_path = Path("sql/00_init_schema.sql")
    logger.info("Bootstrapping database schema", extra={"sql_file": str(sql_path)})
    try:
        async with engine.begin() as conn:
            await conn.execute(text(sql_path.read_text()))
        logger.info("Database schema bootstrapped successfully")
    except Exception as e:
        logger.error("Failed to bootstrap database schema", exc_info=True)
        raise

async def ingest():
    logger.info("Starting data ingestion process")
    
    # Setup vector store
    vec_dir = S.data_dir / "vector_store"
    vec_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Vector store directory prepared", extra={"vector_dir": str(vec_dir)})
    
    # Initialize embeddings
    logger.info("Initializing OpenAI embeddings")
    embeddings = OpenAIEmbeddings(api_key=S.openai_api_key, model=S.model_name)
    
    # Load or create FAISS store
    if (vec_dir / "index.faiss").exists():
        logger.info("Loading existing FAISS index")
        store = FAISS.load_local(vec_dir)
        logger.info("FAISS index loaded successfully", extra={"index_size": store.index.ntotal})
    else:
        logger.info("Creating new FAISS index")
        store = FAISS(embedding_function=embeddings)

    # Setup database
    logger.info("Initializing database connection")
    engine = create_async_engine(S.db_url, echo=False)
    await _bootstrap_schema(engine)

    async with AIOKafkaProducer(bootstrap_servers=S.kafka_bootstrap) as kafka:
        start = time.perf_counter()
        
        async with AsyncSession(engine) as sess:
            # books ----------------------------------------------------------
            logger.info("Processing books catalog")
            book_count = 0
            for row in _load_csv(Path("data/catalog_sample.csv")):
                try:
                    item = models.BookCatalogItem(**row)
                    await sess.execute(
                        text("""INSERT INTO catalog(book_id,isbn,title,genre,keywords,description,page_count,publication_year,difficulty_band,average_student_rating)
                             VALUES(:book_id,:isbn,:title,:genre,:keywords,:description,:page_count,:publication_year,:difficulty_band,:rating)
                             ON CONFLICT(book_id) DO UPDATE SET
                                 isbn=EXCLUDED.isbn,
                                 genre=EXCLUDED.genre,
                                 keywords=EXCLUDED.keywords,
                                 description=EXCLUDED.description,
                                 page_count=COALESCE(EXCLUDED.page_count, catalog.page_count),
                                 publication_year=COALESCE(EXCLUDED.publication_year, catalog.publication_year),
                                 difficulty_band=COALESCE(EXCLUDED.difficulty_band, catalog.difficulty_band),
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
                            "rating": item.average_student_rating,
                        },
                    )
                    store.add_texts(
                        [f"{item.title}. {item.description or ''}"],
                        metadatas=[{"book_id": item.book_id}],
                    )
                    book_count += 1
                    
                    if book_count % 100 == 0:
                        logger.debug("Processed books", extra={"count": book_count})
                        
                except Exception as e:
                    logger.error("Failed to process book", exc_info=True, extra={"book_id": row.get("book_id"), "row": row})
                    continue
            
            logger.info("Books processing completed", extra={"total_books": book_count})

            # students -------------------------------------------------------
            logger.info("Processing students data")
            student_count = 0
            for row in _load_csv(Path("data/students_sample.csv")):
                try:
                    stu = models.StudentRecord(**row)
                    await sess.execute(
                        text("INSERT INTO students VALUES(:student_id,:grade,:age,:teacher,:score,:lunch)"
                             "ON CONFLICT(student_id) DO NOTHING"),
                        {"student_id": stu.student_id, "grade": stu.grade_level, "age": stu.age,
                         "teacher": stu.homeroom_teacher, "score": stu.prior_year_reading_score,
                         "lunch": stu.lunch_period}
                    )
                    student_count += 1
                    
                    if student_count % 100 == 0:
                        logger.debug("Processed students", extra={"count": student_count})
                        
                except Exception as e:
                    logger.error("Failed to process student", exc_info=True, extra={"student_id": row.get("student_id"), "row": row})
                    continue
            
            logger.info("Students processing completed", extra={"total_students": student_count})

            # checkouts ------------------------------------------------------
            logger.info("Processing checkout data")
            checkout_count = 0
            for row in _load_csv(Path("data/checkouts_sample.csv")):
                try:
                    chk = models.CheckoutRecord(**row)
                    await sess.execute(
                        text("INSERT INTO checkout VALUES(:sid,:bid,:out,:back,:rating)"
                             "ON CONFLICT DO NOTHING"),
                        {"sid": chk.student_id, "bid": chk.book_id, "out": chk.checkout_date,
                         "back": chk.return_date, "rating": chk.student_rating}
                    )
                    checkout_count += 1
                    
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
        logger.info("Saving FAISS index")
        store.save_local(vec_dir)
        logger.info("FAISS index saved successfully", extra={"index_size": store.index.ntotal})
        
        # Final metrics
        duration = round(time.perf_counter() - start, 2)
        final_payload = {
            "event": "ingestion_complete",
            "duration": duration,
            "rows_ingested": store.index.ntotal,
            "request_id": uuid.uuid4().hex,
            "timestamp": time.time(),
            "books_processed": book_count,
            "students_processed": student_count,
            "checkouts_processed": checkout_count,
        }
        
        await kafka.send_and_wait(
            TOPIC,
            json.dumps(final_payload).encode(),
        )
        
        logger.info("Ingestion process completed successfully", extra={
            "duration_sec": duration,
            "total_vectors": store.index.ntotal,
            "books_processed": book_count,
            "students_processed": student_count,
            "checkouts_processed": checkout_count,
        })

if __name__ == "__main__":
    logger.info("Starting ingestion service")
    try:
        asyncio.run(ingest())
        logger.info("Ingestion service completed successfully")
    except Exception as e:
        logger.error("Ingestion service failed", exc_info=True)
        raise 