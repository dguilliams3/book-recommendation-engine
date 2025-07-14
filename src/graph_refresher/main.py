"""
Nightly job (<60 s CPU) that:
1. Loads recent checkout events (4× half‑life window).
2. Generates embeddings in parallel batches.
3. Upserts pgvector table, builds ivfflat(lists=32) index.
4. Writes top‑K neighbours ≥ similarity_threshold to student_similarity.
5. Publishes edge‑count delta metric to Kafka topic `graph_delta`.

Now also listens for book added events to trigger refresh.
"""

import asyncio, json, math, time, uuid, sys, gc
from datetime import date, timedelta
from collections import defaultdict
from pathlib import Path
from typing import Optional
from contextlib import asynccontextmanager

# Add src to Python path so we can find the common module when run directly
sys.path.insert(0, str(Path(__file__).parent.parent))

import aiokafka, asyncpg
from langchain_openai import OpenAIEmbeddings
from common.settings import settings as S
from common.structured_logging import get_logger
from common.kafka_utils import KafkaEventConsumer, publish_event
from common.events import BOOK_EVENTS_TOPIC

logger = get_logger(__name__)

EMB = OpenAIEmbeddings(
    model=S.embedding_model,
    api_key=S.openai_api_key,
    request_timeout=15,
)

# Debouncing for event-triggered refreshes
_refresh_task: Optional[asyncio.Task] = None
_refresh_delay = (
    S.graph_refresh_delay_seconds
)  # seconds to wait after last event before refreshing


async def debounced_refresh():
    """Debounced refresh that waits for events to settle before running."""
    global _refresh_task

    if _refresh_task and not _refresh_task.done():
        logger.info("Cancelling previous refresh task")
        _refresh_task.cancel()

    _refresh_task = asyncio.create_task(_delayed_refresh())


async def _delayed_refresh():
    """Wait for the delay period then run the refresh."""
    try:
        await asyncio.sleep(_refresh_delay)
        logger.info("Starting event-triggered graph refresh")
        await main()
        logger.info("Event-triggered graph refresh completed")
    except asyncio.CancelledError:
        logger.info("Event-triggered refresh was cancelled")
    except Exception as e:
        logger.error("Event-triggered refresh failed", exc_info=True)


async def handle_book_event(event_data: dict):
    """Handle book added events by triggering a debounced refresh."""
    try:
        logger.info("Received book added event", extra={"event": event_data})
        await debounced_refresh()
    except Exception as e:
        logger.error(
            "Error handling book event", exc_info=True, extra={"event": event_data}
        )


def half_life_weight(days: int) -> float:
    return 0.5 ** (days / S.half_life_days)


@asynccontextmanager
async def managed_vector_processing(batch_size: int = 100):
    """Context manager for explicit memory management during vector processing."""
    try:
        yield batch_size
    finally:
        # Force garbage collection to free memory
        gc.collect()
        logger.debug("Memory cleanup completed")


async def fetch_events(conn):
    window_start = date.today() - timedelta(days=S.half_life_days * 4)
    logger.info(
        "Fetching checkout events",
        extra={
            "window_start": str(window_start),
            "half_life_days": S.half_life_days,
            "window_days": S.half_life_days * 4,
        },
    )

    try:
        rows = await conn.fetch(
            """SELECT c.difficulty_band, co.student_id, co.checkout_date
                   FROM checkout co
                   JOIN catalog c USING(book_id)
                  WHERE co.checkout_date >= $1""",
            window_start,
        )
        logger.info("Fetched checkout events", extra={"event_count": len(rows)})
        return rows
    except Exception as e:
        logger.error("Failed to fetch checkout events", exc_info=True)
        raise


async def _wait_for_data(timeout_sec: int = 60):
    """Poll the checkout table until we have at least one row or timeout."""
    start = time.perf_counter()
    pg_url = str(S.db_url)
    if pg_url.startswith("postgresql+asyncpg://"):
        pg_url = pg_url.replace("postgresql+asyncpg://", "postgresql://")
    while time.perf_counter() - start < timeout_sec:
        try:
            conn = await asyncpg.connect(pg_url)
            cnt = await conn.fetchval("SELECT COUNT(*) FROM checkout")
            await conn.close()
            if cnt and cnt > 0:
                logger.info("Checkout data detected", extra={"rows": cnt})
                return True
            logger.info(
                "Waiting for checkout data…",
                extra={"elapsed_sec": round(time.perf_counter() - start, 1)},
            )
        except Exception:
            logger.debug("Checkout poll failed, retrying", exc_info=True)
        await asyncio.sleep(5)
    logger.warning("Timeout waiting for checkout data – proceeding anyway")
    return False


async def main():
    logger.info("Starting graph refresh process")
    t0 = time.perf_counter()

    # Connect to database
    logger.info("Connecting to database")
    pg_url = str(S.db_url)
    if pg_url.startswith("postgresql+asyncpg://"):
        pg_url = pg_url.replace("postgresql+asyncpg://", "postgresql://")
    elif pg_url.startswith("postgresql+psycopg2://"):
        pg_url = pg_url.replace("postgresql+psycopg2://", "postgresql://")
    try:
        conn = await asyncpg.connect(pg_url)
        logger.info("Database connection established")
    except Exception as e:
        logger.error("Failed to connect to database", exc_info=True)
        raise

    # Wait until ingestion has populated checkout data
    await _wait_for_data()

    # Fetch events
    rows = await fetch_events(conn)

    # Aggregate weighted tokens per student
    logger.info("Processing checkout events and calculating weights")
    tokens = defaultdict(list)
    today = date.today()
    student_count = 0

    for r in rows:
        d_band = r["difficulty_band"]
        days = (today - r["checkout_date"]).days
        w = half_life_weight(days)
        # Ensure student key exists even if difficulty_band is None
        _list = tokens[r["student_id"]]  # touch to create default list
        if d_band is not None:
            _list.append((d_band, w))
        student_count = max(student_count, len(tokens))

    logger.info(
        "Processed events",
        extra={"total_events": len(rows), "unique_students": student_count},
    )

    # Build embedding docs
    logger.info("Building embedding documents")
    docs = []
    keys = []
    for sid, pairs in tokens.items():
        # repeat token proportional to weight *10 to approximate weighting
        doc = " ".join(t * max(1, round(w * 10)) for t, w in pairs)
        docs.append(doc or "no_history")
        keys.append(sid)

    logger.info("Generated embedding documents", extra={"document_count": len(docs)})

    # Generate embeddings with memory management
    logger.info("Generating embeddings with OpenAI")
    vectors = None
    async with managed_vector_processing() as batch_size:
        try:
            # Process in batches to avoid memory pressure
            if len(docs) > batch_size:
                logger.info(
                    "Processing embeddings in batches",
                    extra={"total_docs": len(docs), "batch_size": batch_size},
                )
                vectors = []
                for i in range(0, len(docs), batch_size):
                    batch = docs[i : i + batch_size]
                    batch_vectors = EMB.embed_documents(batch)
                    vectors.extend(batch_vectors)
                    # Explicit cleanup of batch data
                    del batch_vectors, batch
                    gc.collect()
                    logger.debug(
                        "Batch processed",
                        extra={
                            "batch_num": i // batch_size + 1,
                            "total_batches": (len(docs) + batch_size - 1) // batch_size,
                        },
                    )
            else:
                vectors = EMB.embed_documents(docs)

            logger.info(
                "Embeddings generated successfully",
                extra={
                    "vector_count": len(vectors),
                    "vector_dimension": len(vectors[0]) if vectors else 0,
                },
            )
        except Exception as e:
            logger.error("Failed to generate embeddings", exc_info=True)
            raise

    # Always create the similarity table first (doesn't require vectors)
    logger.info("Setting up similarity table")
    try:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS student_similarity(a TEXT,b TEXT,sim REAL,PRIMARY KEY(a,b))"
        )
        await conn.execute("TRUNCATE student_similarity")
        logger.info("Student similarity table prepared")
    except Exception as e:
        logger.error("Failed to prepare similarity table", exc_info=True)
        raise

    # Setup vector extension and table with correct dimension (only if we have vectors)
    logger.info("Setting up vector extension and table")
    try:
        await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")

        # Debug: Check vectors right before the condition
        logger.info(
            "Debug before vectors check",
            extra={
                "vectors_type": type(vectors).__name__,
                "vectors_truthy": bool(vectors),
                "vectors_length": len(vectors) if vectors else "N/A",
            },
        )

        if not vectors:
            logger.warning(
                "No embeddings generated, skipping vector operations but similarity table created"
            )
            await conn.close()
            logger.info("Database connection closed")
            return

        dim = len(vectors[0])
        if dim <= 0:
            logger.warning("Invalid embedding dimension, skipping vector operations")
            await conn.close()
            logger.info("Database connection closed")
            return

        # Drop table if exists with wrong dimension
        await conn.execute("DROP TABLE IF EXISTS student_embeddings")
        await conn.execute(
            f"CREATE TABLE student_embeddings( student_id TEXT PRIMARY KEY, vec VECTOR({dim}), last_event UUID )"
        )
        logger.info(
            "Vector extension and table setup completed", extra={"dimension": dim}
        )
    except Exception as e:
        logger.error("Failed to setup vector extension/table", exc_info=True)
        raise

    # Insert embeddings (convert list -> pgvector literal)
    logger.info("Inserting student embeddings")
    try:
        rows_to_insert = []
        for sid, vec in zip(keys, vectors):
            pg_vec = "[" + ",".join(f"{x:.6f}" for x in vec) + "]"
            rows_to_insert.append((sid, pg_vec, None))

        await conn.executemany(
            "INSERT INTO student_embeddings VALUES($1,$2,$3)"
            "ON CONFLICT(student_id) DO UPDATE SET vec = EXCLUDED.vec",
            rows_to_insert,
        )
        logger.info(
            "Student embeddings inserted successfully",
            extra={"embedding_count": len(keys)},
        )

        # Clean up large vectors from memory
        del vectors, rows_to_insert
        gc.collect()
        logger.debug("Vector memory cleaned up after database insertion")
    except Exception as e:
        logger.error("Failed to insert embeddings", exc_info=True)
        raise

    # Create index
    logger.info("Creating vector index")
    try:
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_vec_ivfflat ON student_embeddings USING ivfflat(vec) WITH (lists=32)"
        )
        logger.info("Vector index created successfully")
    except Exception as e:
        logger.error("Failed to create vector index", exc_info=True)
        raise

    # Compute neighbours with pgvector operator
    logger.info("Computing student similarities")

    insert_rows = []
    similarity_count = 0

    for i, sid in enumerate(keys):
        try:
            sims = await conn.fetch(
                """WITH src AS (SELECT vec FROM student_embeddings WHERE student_id=$1)
                   SELECT student_id, 1-(student_embeddings.vec <=> src.vec) AS sim
                     FROM student_embeddings, src
                    WHERE student_id <> $1
                 ORDER BY student_embeddings.vec <=> src.vec LIMIT 15""",
                sid,
            )

            valid_sims = [
                (sid, row["student_id"], row["sim"])
                for row in sims
                if row["sim"] >= S.similarity_threshold
            ]
            insert_rows.extend(valid_sims)
            similarity_count += len(valid_sims)

            if (i + 1) % 50 == 0:
                logger.debug(
                    "Similarity computation progress",
                    extra={
                        "processed": i + 1,
                        "total": len(keys),
                        "similarities_found": similarity_count,
                    },
                )

        except Exception as e:
            logger.error(
                "Failed to compute similarities for student",
                exc_info=True,
                extra={"student_id": sid},
            )
            continue

    logger.info(
        "Similarity computation completed",
        extra={
            "total_similarities": similarity_count,
            "similarity_threshold": S.similarity_threshold,
        },
    )

    # Insert similarities
    logger.info("Inserting student similarities")
    try:
        await conn.executemany(
            "INSERT INTO student_similarity VALUES($1,$2,$3)", insert_rows
        )
        logger.info(
            "Student similarities inserted successfully",
            extra={"similarity_count": len(insert_rows)},
        )
    except Exception as e:
        logger.error("Failed to insert similarities", exc_info=True)
        raise

    await conn.close()
    logger.info("Database connection closed")

    # Publish metrics
    logger.info("Publishing graph delta metrics")
    try:
        metric_payload = {
            "edges": len(insert_rows),
            "timestamp": time.time(),
            "run_id": uuid.uuid4().hex,
        }

        await publish_event("graph_delta", metric_payload)
        logger.info("Graph delta metrics published successfully", extra=metric_payload)
    except Exception:
        logger.error("Failed to publish graph delta metrics", exc_info=True)

    duration = time.perf_counter() - t0
    logger.info(
        "Graph refresh process completed",
        extra={
            "duration_sec": round(duration, 2),
            "edges": len(insert_rows),
            "students_processed": len(keys),
            "similarities_found": similarity_count,
        },
    )


if __name__ == "__main__":
    logger.info("Starting graph refresher service")
    try:
        # Run initial refresh and event listener in parallel
        async def run_service():
            # Start event listener
            consumer = KafkaEventConsumer(BOOK_EVENTS_TOPIC, "graph_refresher")
            listener_task = asyncio.create_task(consumer.start(handle_book_event))

            # Wait until ingestion has populated checkout data
            await _wait_for_data()

            # Run initial refresh
            try:
                await main()
                logger.info("Initial graph refresh completed")
            except Exception as e:
                logger.error("Initial graph refresh failed", exc_info=True)

            # Keep the event listener running
            logger.info("Graph refresher service running - listening for events")
            try:
                await listener_task
            except KeyboardInterrupt:
                logger.info("Graph refresher interrupted by user")
                await consumer.stop()

        asyncio.run(run_service())
        logger.info("Graph refresher service completed successfully")
    except KeyboardInterrupt:
        logger.info("Graph refresher interrupted by user")
    except Exception as e:
        logger.error("Graph refresher failed", exc_info=True)
        raise
