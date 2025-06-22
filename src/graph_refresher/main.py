"""
Nightly job (<60 s CPU) that:
1. Loads recent checkout events (4× half‑life window).
2. Generates embeddings in parallel batches.
3. Upserts pgvector table, builds ivfflat(lists=32) index.
4. Writes top‑K neighbours ≥ similarity_threshold to student_similarity.
5. Publishes edge‑count delta metric to Kafka topic `graph_delta`.
"""
import asyncio, json, logging, math, time, uuid
from datetime import date, timedelta
from collections import defaultdict

import aiokafka, asyncpg
from langchain_openai import OpenAIEmbeddings
from common import SettingsInstance as S

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("graph_refresher")

EMB = OpenAIEmbeddings(
    model="text-embedding-3-small",
    api_key=S.openai_api_key,
    request_timeout=15,
)

def half_life_weight(days: int) -> float:
    return 0.5 ** (days / S.half_life_days)

async def fetch_events(conn):
    window_start = date.today() - timedelta(days=S.half_life_days * 4)
    return await conn.fetch(
        """SELECT c.difficulty_band, co.student_id, co.checkout_date
               FROM checkout co
               JOIN catalog c USING(book_id)
              WHERE co.checkout_date >= $1""",
        window_start,
    )

async def main():
    t0 = time.perf_counter()
    pg_url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(pg_url)
    rows = await fetch_events(conn)

    # aggregate weighted tokens per student
    tokens = defaultdict(list)
    today = date.today()
    for r in rows:
        d_band = r["difficulty_band"]
        days = (today - r["checkout_date"]).days
        w = half_life_weight(days)
        tokens[r["student_id"]].append((d_band, w))

    # build embedding docs
    docs = []
    keys = []
    for sid, pairs in tokens.items():
        # repeat token proportional to weight *10 to approximate weighting
        doc = " ".join(t * max(1, round(w * 10)) for t, w in pairs)
        docs.append(doc or "no_history")
        keys.append(sid)

    vectors = EMB.embed_documents(docs)

    await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
    await conn.execute(
        """CREATE TABLE IF NOT EXISTS student_embeddings(
               student_id TEXT PRIMARY KEY,
               vec VECTOR(1536))"""
    )
    await conn.executemany(
        "INSERT INTO student_embeddings VALUES($1,$2)"
        "ON CONFLICT(student_id) DO UPDATE SET vec = EXCLUDED.vec",
        list(zip(keys, vectors)),
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_vec_ivfflat ON student_embeddings USING ivfflat(vec) WITH (lists=32)"
    )

    # compute neighbours with pgvector operator
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS student_similarity(a TEXT,b TEXT,sim REAL,PRIMARY KEY(a,b))"
    )
    await conn.execute("TRUNCATE student_similarity")
    insert_rows = []
    for sid in keys:
        sims = await conn.fetch(
            """WITH src AS (SELECT vec FROM student_embeddings WHERE student_id=$1)
               SELECT student_id, 1-(vec <=> src.vec) AS sim
                 FROM student_embeddings, src
                WHERE student_id <> $1
             ORDER BY vec <=> src.vec LIMIT 15""",
            sid,
        )
        insert_rows += [
            (sid, row["student_id"], row["sim"])
            for row in sims
            if row["sim"] >= S.similarity_threshold
        ]
    await conn.executemany(
        "INSERT INTO student_similarity VALUES($1,$2,$3)", insert_rows
    )
    await conn.close()

    producer = aiokafka.AIOKafkaProducer(bootstrap_servers=S.kafka_bootstrap)
    await producer.start()
    await producer.send_and_wait(
        "graph_delta",
        json.dumps(
            {"edges": len(insert_rows), "timestamp": time.time(), "run_id": uuid.uuid4().hex}
        ).encode(),
    )
    await producer.stop()
    log.info("graph refresher finished: %.2f s, edges=%d", time.perf_counter() - t0, len(insert_rows))

if __name__ == "__main__":
    asyncio.run(main()) 