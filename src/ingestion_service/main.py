import asyncio, csv, json, time, uuid, logging
from pathlib import Path
from aiokafka import AIOKafkaProducer
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import FAISS
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import text
from common import SettingsInstance as S, models
from common.logging import get_logger

TOPIC = "ingestion_metrics"

async def publish(kafka, metric: dict):
    await kafka.send_and_wait(TOPIC, json.dumps(metric).encode())

async def ingest_loop():
    kafka = AIOKafkaProducer(bootstrap_servers=S.kafka_bootstrap)
    await kafka.start()
    logger = get_logger("ingestion_service", kafka)
    try:
        vec_index_path = S.data_dir / "vector_store"
        vec_index_path.mkdir(parents=True, exist_ok=True)
        logger.info("Starting ingestion service", extra={"event": "startup"})

        embeddings = OpenAIEmbeddings(openai_api_key=S.openai_api_key, model=S.model_name)
        vector_store = FAISS.load_local(vec_index_path) if (vec_index_path / "index.faiss").exists() \
            else FAISS(embedding_function=embeddings)

        engine = create_async_engine(S.db_url, echo=False, pool_size=5)
        start = time.perf_counter()
        for csv_file in Path(S.data_dir).glob("*.csv"):
            with open(csv_file, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                async with AsyncSession(engine) as sess:
                    for row in reader:
                        if "catalog" in csv_file.name:
                            item = models.BookCatalogItem(**row)
                            await sess.execute(
                                text("""INSERT INTO catalog (book_id, title, genre, keywords, description)
                                        VALUES (:book_id, :title, :genre, :keywords, :description)
                                        ON CONFLICT (book_id) DO NOTHING"""),
                                {"book_id": item.book_id,
                                 "title": item.title,
                                 "genre": json.dumps(item.genre),
                                 "keywords": json.dumps(item.keywords),
                                 "description": item.description},
                            )
                            vector_store.add_texts(
                                texts=[f"{item.title}. {item.description or ''}"],
                                metadatas=[{"book_id": item.book_id}],
                            )
                        # similar blocks for students & checkouts...
                    await sess.commit()
        vector_store.save_local(vec_index_path)
        duration = time.perf_counter() - start
        logger.info(f"Ingestion finished in {duration:.2f} s", extra={"event": "ingestion_complete", "duration_sec": duration})
        await publish(kafka, {
            "event": "ingestion_complete",
            "duration_sec": duration,
            "request_id": str(uuid.uuid4()),
            "timestamp": time.time(),
        })
    except Exception as e:
        logger.error(f"Ingestion error: {e}", exc_info=True, extra={"event": "error"})
    finally:
        await kafka.stop()

if __name__ == "__main__":
    asyncio.run(ingest_loop()) 