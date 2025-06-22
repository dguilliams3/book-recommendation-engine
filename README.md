# Book Recommendation Engine

## 1  Vector‑store & Database decision tables

### 1.1 Vector stores

| Store        | Pros                                                                                  | Cons                                                                  | Free‑tier viability        | Ease of swap                       | Scaling notes                             | **Demo Suitability (1‑5)** |
| ------------ | ------------------------------------------------------------------------------------- | --------------------------------------------------------------------- | -------------------------- | ---------------------------------- | ----------------------------------------- | -------------------------- |
| **FAISS**    | • In‑process, zero network latency<br>• No account/keys<br>• Mature LangChain wrapper | • Single‑node only (RAM‑bound)<br>• No persistence without extra work | ✅ fully local              | Very high – just replace file path | Scale‑up (larger RAM) or shard behind API | **5**                      |
| **Chroma**   | • Local *or* client‑server modes<br>• Built‑in persistence                            | • Early‑stage, smaller community<br>• Docker image adds 500 MB        | ✅ local → free             | High – single URL switch           | Horizontal scaling experimental           | 4                          |
| **Pinecone** | • Fully managed, automatic sharding<br>• Production SLA                               | • Network latency<br>• API‑key management<br>• Paid beyond hobby tier | Limited – 5 k vectors free | Medium – swap env + install SDK    | Effortless horizontal scale               | 3                          |

### 1.2 Databases

| DB           | Pros                                                         | Cons                                                             | Free‑tier viability | Ease of swap                 | Scaling notes                    | **Demo Suitability (1‑5)** |
| ------------ | ------------------------------------------------------------ | ---------------------------------------------------------------- | ------------------- | ---------------------------- | -------------------------------- | -------------------------- |
| **SQLite**   | • Zero install, single file<br>• ACID, full SQL              | • Single‑writer lock<br>• No native JSON indexes                 | ✅ infinite          | Very high – change URI       | Vertical only; good backup story | 4                          |
| **Postgres** | • Rich SQL + JSONB<br>• Great LangChain + SQLAlchemy support | • Requires service container<br>• Slightly heavier RAM (≈200 MB) | ✅ docker, free      | High – change URI and driver | Horizontal via read‑replicas     | **5**                      |
| **InfluxDB** | • Time‑series first‑class<br>• Good for metrics              | • Poor relational joins<br>• Extra query language                | Limited OSS         | Low – different query model  | Scales well for metrics only     | 3                          |

### 1.3 Chosen stack (≤150 words)

A single‑school deployment needs **low latency, zero external dependencies, and smooth developer ergonomics**.
**FAISS** scores highest because it is in‑process, free, and trivial to vendor‑in; durability is solved by writing the FAISS index to the shared `vector_store/` volume after each ingest run.
For the relational layer, **Postgres** wins: we already spin up Kafka and multiple micro‑services – adding one lightweight Postgres container costs little but unlocks concurrent writes, JSONB columns for flexible catalog metadata, and future migration paths (RDS, Cloud SQL) with no code changes.
Both choices swap cleanly via `.env` edits (`VECTOR_STORE_TYPE`, `DB_URL`).

---

## 2  Mermaid architecture diagram

```mermaid
flowchart LR
    subgraph Ingestion_Service
        A1[CSV Watcher] --> A2[Vector Embed & Upsert (FAISS)]
        A1 --> A3[Relational Upsert (Postgres)]
        A1 --> A4[Kafka Producer<br>metrics.topic]
    end

    subgraph Recommendation_API
        B1[/FastAPI/] -->|spawn| B2[FastMCP registry<br>(stdio)]
        B1 --> B3[LangGraph flow<br>(GPT‑4o)]
        B1 --> B4[Kafka Producer]
    end

    subgraph Streamlit_UI
        C1[streamlit app.py] -->|REST| B1
    end

    subgraph Metrics_Consumer
        D1[Kafka Consumer] --> D2[Structured log writer]
    end

    subgraph Optional_Stubs
        E1[TTS_Worker]:::stub
        E2[Image_Worker]:::stub
    end

    Postgres[(Postgres)]
    FAISS[(FAISS index)]
    ZK[(ZooKeeper)]
    Kafka[(Kafka Broker)]

    A2 --> FAISS
    B3 --> FAISS
    A3 --> Postgres
    B3 --> Postgres
    A4 --> Kafka
    B4 --> Kafka
    Kafka --> D1
    ZK --coord--> Kafka

    classDef stub stroke-dasharray: 5 5,color:#999
```

---

## 3  Directory tree

```text
C:\Users\Dan Guilliams\OneDrive\Code Projects\book_recommendation_engine
│  docker-compose.yml
│  .env.template
│  README.md
└─src
    ├─common
    │      __init__.py
    │      settings.py
    │      models.py
    ├─ingestion_service
    │      __init__.py
    │      main.py
    │      Dockerfile
    ├─recommendation_api
    │      __init__.py
    │      main.py
    │      mcp_book_server.py
    │      Dockerfile
    ├─streamlit_ui
    │      __init__.py
    │      app.py
    │      Dockerfile
    ├─metrics_consumer
    │      __init__.py
    │      main.py
    │      Dockerfile
    └─stubs
        ├─tts_worker
        │      __init__.py
        │      main.py
        │      Dockerfile
        │      README.md
        └─image_worker
               __init__.py
               main.py
               Dockerfile
               README.md
```

---

## 4  Exact PowerShell commands to scaffold

```powershell
# 0. set root once
$root = "C:\Users\Dan Guilliams\OneDrive\Code Projects\book_recommendation_engine"

# 1. directories
mkdir -Force $root, "$root\src", `
    "$root\src\common", `
    "$root\src\ingestion_service", `
    "$root\src\recommendation_api", `
    "$root\src\streamlit_ui", `
    "$root\src\metrics_consumer", `
    "$root\src\stubs", `
    "$root\src\stubs\tts_worker", `
    "$root\src\stubs\image_worker" | Out-Null

# 2. top‑level files
ni "$root\.env.template" -ItemType File
ni "$root\docker-compose.yml" -ItemType File
ni "$root\README.md" -ItemType File

# 3. common
ni "$root\src\common\__init__.py" -ItemType File
ni "$root\src\common\settings.py" -ItemType File
ni "$root\src\common\models.py" -ItemType File

# 4. ingestion_service
ni "$root\src\ingestion_service\__init__.py" -ItemType File
ni "$root\src\ingestion_service\main.py" -ItemType File
ni "$root\src\ingestion_service\Dockerfile" -ItemType File

# 5. recommendation_api
ni "$root\src\recommendation_api\__init__.py" -ItemType File
ni "$root\src\recommendation_api\main.py" -ItemType File
ni "$root\src\recommendation_api\mcp_book_server.py" -ItemType File
ni "$root\src\recommendation_api\Dockerfile" -ItemType File

# 6. streamlit_ui
ni "$root\src\streamlit_ui\__init__.py" -ItemType File
ni "$root\src\streamlit_ui\app.py" -ItemType File
ni "$root\src\streamlit_ui\Dockerfile" -ItemType File

# 7. metrics_consumer
ni "$root\src\metrics_consumer\__init__.py" -ItemType File
ni "$root\src\metrics_consumer\main.py" -ItemType File
ni "$root\src\metrics_consumer\Dockerfile" -ItemType File

# 8. stubs
# tts_worker
ni "$root\src\stubs\tts_worker\__init__.py" -ItemType File
ni "$root\src\stubs\tts_worker\main.py" -ItemType File
ni "$root\src\stubs\tts_worker\Dockerfile" -ItemType File
ni "$root\src\stubs\tts_worker\README.md" -ItemType File
# image_worker
ni "$root\src\stubs\image_worker\__init__.py" -ItemType File
ni "$root\src\stubs\image_worker\main.py" -ItemType File
ni "$root\src\stubs\image_worker\Dockerfile" -ItemType File
ni "$root\src\stubs\image_worker\README.md" -ItemType File
```

---

## 5  One‑liner setup & run commands

```bash
# 1. bootstrap poetry & deps
cd "C:\Users\Dan Guilliams\OneDrive\Code Projects\book_recommendation_engine"
poetry env use 3.11
poetry install  # root has no pyproject, but sub‑modules do; Poetry will flatten

# 2. copy env template
cp .env.template .env

# 3. build & run whole stack
docker compose up --build
``` 