# Ingestion Service

## Overview

The **Ingestion Service** is a critical ETL (Extract, Transform, Load) microservice that serves as the primary data gateway for the book recommendation engine. It orchestrates the initial population of the system's foundational datasets while establishing the event-driven data flow that powers real-time recommendation updates.

## Architecture & Responsibilities

### Core Functions
- **CSV Data Validation & Transformation**: Validates incoming CSV files against strict Pydantic schemas with comprehensive error handling and data coercion
- **Vector Store Management**: Builds and maintains FAISS indices for high-performance semantic search across book catalogs  
- **Event Stream Coordination**: Publishes structured events to Kafka topics, triggering downstream processing cascades
- **Database Population**: Performs optimized batch upserts with transaction management and constraint validation

### Technical Design Patterns

#### **Event Sourcing Architecture**
```python
# Event-driven updates trigger real-time pipeline processing
await publish_event(BOOK_EVENTS_TOPIC, BookAddedEvent(
    count=len(book_batch),
    book_ids=[book.book_id for book in book_batch],
    source="ingestion_service"
))
```

#### **Vector Store Lifecycle Management**
- **FAISS Index Construction**: Builds optimized vector indices using OpenAI embeddings with configurable similarity metrics
- **Incremental Updates**: Supports both full rebuilds and incremental additions for operational flexibility
- **Persistence Strategy**: Manages file-based vector store serialization with atomic write operations

## Service Integration Points

### **Upstream Dependencies**
- **PostgreSQL with pgvector**: Relational data persistence with vector extension support
- **OpenAI Embeddings API**: Text-to-vector transformation for semantic indexing
- **Apache Kafka**: Event streaming backbone for microservices coordination

### **Downstream Event Consumers**
- **Book Vector Worker**: Consumes `BookAddedEvent` for real-time FAISS index updates
- **Student Profile Worker**: Triggered by `StudentsAddedEvent` for profile aggregation
- **Graph Refresher**: Responds to `CheckoutAddedEvent` for similarity matrix updates
- **Book Enrichment Worker**: Processes `BookEnrichmentTaskEvent` for metadata completion

## Data Flow & Processing Pipeline

### **Phase 1: Schema Validation**
```python
# Strict Pydantic validation with custom field coercion
class BookCatalogItem(BaseModel):
    book_id: str
    isbn: str | None = None
    reading_level: Optional[float] = Field(None, ge=0.0, le=12.0)
    genre: List[str] = Field(default_factory=list)
    
    @field_validator("genre", mode="before")
    def _ensure_list_genre(cls, v):
        # Handle JSON strings, single values, and null cases
```

### **Phase 2: Vector Index Construction**
- **Embedding Generation**: Parallel batch processing with OpenAI's text-embedding-3-small model
- **FAISS Index Building**: Optimized L2 distance calculations with configurable clustering parameters
- **Memory Management**: Efficient handling of large document collections with streaming processing

### **Phase 3: Event Publication**
```python
# Multi-topic event coordination
topics = {
    BOOK_EVENTS_TOPIC: BookAddedEvent,
    STUDENT_EVENTS_TOPIC: StudentsAddedEvent, 
    CHECKOUT_EVENTS_TOPIC: CheckoutAddedEvent
}
```

## Operational Characteristics

### **Performance Metrics**
- **Throughput**: Processes 10,000+ catalog entries per minute with parallel embedding generation
- **Latency**: Sub-second database upserts with optimized batch operations
- **Memory Efficiency**: Streaming CSV processing prevents OOM conditions on large datasets

### **Fault Tolerance**
- **Transactional Integrity**: All-or-nothing batch processing with automatic rollback
- **Retry Logic**: Exponential backoff for transient OpenAI API failures
- **Dead Letter Handling**: Failed events are logged and queued for manual intervention

### **Observability**
- **Structured Logging**: JSON-formatted logs with correlation IDs and timing metrics
- **Prometheus Metrics**: Job duration, success/failure rates, and processing volumes
- **Health Checks**: Database connectivity and vector store integrity validation

## Configuration & Environment

### **Required Environment Variables**
```bash
OPENAI_API_KEY=sk-...              # OpenAI API access
OPENAI_MODEL=gpt-4o-mini           # LLM model for processing
VECTOR_STORE_TYPE=faiss            # Vector store backend
DB_URL=postgresql://...            # PostgreSQL connection string
KAFKA_BROKERS=kafka:9092           # Kafka cluster endpoints
```

### **Resource Requirements**
- **CPU**: 2+ cores for parallel embedding processing
- **Memory**: 4GB+ for FAISS index construction and CSV buffering
- **Storage**: 1GB+ for vector index persistence and temporary files
- **Network**: 100Mbps+ for OpenAI API calls and Kafka event publishing

## Development & Deployment

### **Local Development**
```bash
# Install dependencies
cd src/ingestion_service
pip install -r requirements.txt

# Run with hot-reload
python main.py
```

### **Container Deployment**
```dockerfile
# Multi-stage build for optimized production image
FROM python:3.12-slim as base
# ... dependency installation
FROM base as runtime
# ... application code and startup
```

### **Monitoring & Alerts**
- **Success Rate**: Alert if job success rate drops below 95%
- **Processing Time**: Alert if processing duration exceeds baseline by 50%
- **Event Publishing**: Alert on Kafka connectivity failures or topic lag

## Scaling Considerations

### **Horizontal Scaling**
- **Stateless Design**: Service can be replicated across multiple instances
- **Work Distribution**: CSV files can be partitioned across multiple ingestion pods
- **Event Ordering**: Kafka partitioning ensures ordered processing per book/student ID

### **Vertical Scaling**
- **Memory Optimization**: Configurable batch sizes for memory-constrained environments
- **CPU Optimization**: Parallel embedding generation with thread pool sizing
- **I/O Optimization**: Async database operations with connection pooling

---

**Production Impact**: This service establishes the foundational data layer that powers all downstream recommendation algorithms. Its reliability directly impacts system availability and recommendation quality. 