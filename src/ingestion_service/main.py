"""
1. Ensure schema exists (executes 00_init_schema.sql once).
2. Validate & coerce CSV rows → Pydantic models.
3. Upsert rows into Postgres and build / update FAISS embeddings.
"""

import asyncio, csv, json, time, uuid, logging
from pathlib import Path
from typing import Iterable

from aiokafka import AIOKafkaProducer
from langchain.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import text

from common import SettingsInstance as S, models

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
TOPIC = "ingestion_metrics"

async def _publish(k, payload):  # fire-and-forget
    await k.send_and_wait(TOPIC, json.dumps(payload).encode())

def _load_csv(path: Path) -> Iterable[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield {k: (v or None) for k, v in row.items()}

async def _bootstrap_schema(engine):
    sql_path = Path("sql/00_init_schema.sql")
    async with engine.begin() as conn:
        await conn.execute(text(sql_path.read_text()))

async def ingest():
    vec_dir = S.data_dir / "vector_store"
    vec_dir.mkdir(parents=True, exist_ok=True)
    embeddings = OpenAIEmbeddings(api_key=S.openai_api_key, model=S.model_name)
    store = FAISS.load_local(vec_dir) if (vec_dir / "index.faiss").exists() else FAISS(embedding_function=embeddings)

    engine = create_async_engine(S.db_url, echo=False)
    await _bootstrap_schema(engine)

    async with AIOKafkaProducer(bootstrap_servers=S.kafka_bootstrap) as kafka:
        start = time.perf_counter()
        async with AsyncSession(engine) as sess:
            # books ----------------------------------------------------------
            for row in _load_csv(Path("data/catalog_sample.csv")):
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

            # students -------------------------------------------------------
            for row in _load_csv(Path("data/students_sample.csv")):
                stu = models.StudentRecord(**row)
                await sess.execute(
                    text("INSERT INTO students VALUES(:student_id,:grade,:age,:teacher,:score,:lunch)"
                         "ON CONFLICT(student_id) DO NOTHING"),
                    {"student_id": stu.student_id, "grade": stu.grade_level, "age": stu.age,
                     "teacher": stu.homeroom_teacher, "score": stu.prior_year_reading_score,
                     "lunch": stu.lunch_period}
                )

            # checkouts ------------------------------------------------------
            for row in _load_csv(Path("data/checkouts_sample.csv")):
                chk = models.CheckoutRecord(**row)
                await sess.execute(
                    text("INSERT INTO checkout VALUES(:sid,:bid,:out,:back,:rating)"
                         "ON CONFLICT DO NOTHING"),
                    {"sid": chk.student_id, "bid": chk.book_id, "out": chk.checkout_date,
                     "back": chk.return_date, "rating": chk.student_rating}
                )
            await sess.commit()

        store.save_local(vec_dir)
        duration = round(time.perf_counter() - start, 2)
        await kafka.send_and_wait(
            TOPIC,
            json.dumps(
                {
                    "event": "ingestion_complete",
                    "duration": duration,
                    "rows_ingested": store.index.ntotal,
                    "request_id": uuid.uuid4().hex,
                    "timestamp": time.time(),
                }
            ).encode(),
        )
        logging.info("Ingestion complete: %.2fs, vectors=%d", duration, store.index.ntotal)

if __name__ == "__main__":
    asyncio.run(ingest()) 