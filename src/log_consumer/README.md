# Log Consumer Service

## Overview

The **Log Consumer Service** is a critical observability component that aggregates structured logs from across the distributed system into centralized storage for analysis and monitoring. This service demonstrates advanced patterns in log aggregation, structured data processing, and production-grade observability infrastructure.

## Architecture & Responsibilities

### **Core Functions**
- **Centralized Log Aggregation**: Collects structured logs from all microservices via Kafka streaming
- **JSON Log Processing**: Parses and validates structured log entries with error handling and data quality assurance
- **File-Based Persistence**: Maintains organized log files with rotation and compression for long-term storage
- **Real-Time Processing**: Provides near-instantaneous log availability for debugging and monitoring

### **Structured Logging Pipeline**

#### **Event Stream Consumption**
```python
# Multi-service log aggregation from dedicated Kafka topic
consumer = KafkaEventConsumer("logs", group_id="log_aggregator")

async def handle_log_event(log_data: dict):
    # Parse structured log entries with validation
    # Enrich with metadata and timestamps
    # Persist to organized file structure
    pass
```

**Log Event Processing**:
- **Service Attribution**: Automatically identifies source service from event metadata
- **Timestamp Normalization**: Ensures consistent time representation across all logs
- **Data Validation**: Validates log entry structure and required fields
- **Error Handling**: Gracefully handles malformed or incomplete log entries

#### **Structured Data Processing**
```python
# JSON log entry validation and enrichment
def process_log_entry(raw_log: dict) -> dict:
    processed = {
        "timestamp": raw_log.get("timestamp", datetime.utcnow().isoformat()),
        "service": raw_log.get("service", "unknown"),
        "level": raw_log.get("level", "INFO"),
        "message": raw_log.get("message", ""),
        "extra": raw_log.get("extra", {}),
        "correlation_id": raw_log.get("correlation_id"),
        "request_id": raw_log.get("request_id")
    }
    return processed
```

**Enrichment Features**:
- **Correlation ID Tracking**: Maintains request correlation across service boundaries
- **Level Standardization**: Normalizes log levels across different service implementations
- **Metadata Extraction**: Parses service-specific metadata for enhanced searchability
- **Error Classification**: Categorizes errors and exceptions for alerting and analysis

## File Management & Storage

### **Organized File Structure**
```python
# Hierarchical log organization for efficient access
log_structure = {
    "service_logs.jsonl": "Primary aggregated log file",
    "error_logs.jsonl": "Error and exception entries only", 
    "performance_logs.jsonl": "Performance and timing metrics",
    "audit_logs.jsonl": "Security and compliance events"
}
```

**Storage Strategy**:
- **JSONL Format**: Newline-delimited JSON for efficient streaming and parsing
- **Categorical Separation**: Separate files for different log types enable targeted analysis
- **Chronological Organization**: Time-based sorting for efficient temporal queries
- **Compression Ready**: File format optimized for standard compression algorithms

### **Log Rotation & Retention**
```python
# Automated log rotation with configurable policies
class LogRotationManager:
    def __init__(self, max_size_mb=100, retention_days=30):
        self.max_size = max_size_mb * 1024 * 1024
        self.retention_period = timedelta(days=retention_days)
    
    async def rotate_if_needed(self, log_file: Path):
        if log_file.stat().st_size > self.max_size:
            await self.rotate_log(log_file)
```

**Rotation Features**:
- **Size-Based Rotation**: Automatic rotation when files exceed configured size limits
- **Time-Based Retention**: Automatic cleanup of logs older than retention period
- **Compression**: Gzip compression of rotated logs for storage efficiency
- **Index Maintenance**: Updates log indices for efficient searching

## Real-Time Processing & Streaming

### **Event-Driven Architecture**
```python
# High-throughput event processing with backpressure handling
async def consume_logs():
    async for log_batch in kafka_consumer.get_batches(max_batch_size=100):
        await process_log_batch(log_batch)
        await flush_to_disk()
```

**Processing Characteristics**:
- **Batch Processing**: Groups log entries for efficient I/O operations
- **Backpressure Management**: Handles high-volume log bursts without data loss
- **Async I/O**: Non-blocking file operations for maximum throughput
- **Buffer Management**: Configurable buffering for latency vs. throughput optimization

### **Performance Optimization**
```python
# Optimized write patterns for high-throughput logging
class BufferedLogWriter:
    def __init__(self, buffer_size=1000, flush_interval=5):
        self.buffer = []
        self.buffer_size = buffer_size
        self.flush_interval = flush_interval
    
    async def write_log(self, log_entry: dict):
        self.buffer.append(json.dumps(log_entry))
        if len(self.buffer) >= self.buffer_size:
            await self.flush()
```

**Optimization Features**:
- **Write Batching**: Groups multiple log entries for efficient disk writes
- **Periodic Flushing**: Time-based flushing ensures logs aren't held indefinitely
- **Memory Management**: Bounded buffer sizes prevent memory exhaustion
- **Compression**: Real-time compression for storage and network efficiency

## Observability & Monitoring

### **System Health Metrics**
```python
# Comprehensive metrics for log processing pipeline
LOG_PROCESSING_METRICS = {
    "logs_processed_total": Counter("Total log entries processed"),
    "logs_dropped_total": Counter("Log entries dropped due to errors"),
    "processing_latency": Histogram("Log processing latency in milliseconds"),
    "file_size_bytes": Gauge("Current log file sizes"),
    "kafka_consumer_lag": Gauge("Consumer lag in messages")
}
```

**Monitoring Capabilities**:
- **Processing Throughput**: Real-time tracking of log processing rates
- **Error Rates**: Monitoring of dropped or malformed log entries
- **Latency Tracking**: End-to-end processing time from Kafka to disk
- **Storage Utilization**: Disk usage and growth rate monitoring

### **Quality Assurance**
```python
# Log quality validation and reporting
async def validate_log_quality():
    stats = {
        "total_entries": 0,
        "valid_entries": 0,
        "malformed_entries": 0,
        "missing_timestamps": 0,
        "services_seen": set()
    }
    # Analysis logic for log data quality
    return stats
```

**Quality Metrics**:
- **Data Completeness**: Tracks required field presence across log entries
- **Service Coverage**: Ensures all services are contributing logs appropriately
- **Format Compliance**: Validates structured log format adherence
- **Temporal Consistency**: Detects timestamp anomalies and clock skew issues

## Configuration & Deployment

### **Environment Configuration**
```bash
KAFKA_BROKERS=kafka:9092                 # Event streaming endpoints
LOG_TOPIC=logs                           # Kafka topic for log aggregation
LOG_OUTPUT_DIR=/app/logs                 # Persistent log storage directory
LOG_LEVEL=INFO                           # Minimum log level to process
MAX_LOG_FILE_SIZE_MB=100                 # Log rotation size threshold
LOG_RETENTION_DAYS=30                    # Log retention period
BUFFER_SIZE=1000                         # Write buffer size for performance
FLUSH_INTERVAL_SEC=5                     # Maximum buffer flush interval
```

### **Container Deployment**
```dockerfile
# Optimized container with persistent log storage
FROM python:3.12-slim
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app/
VOLUME ["/app/logs"]  # Persistent log storage
CMD ["python", "main.py"]
```

### **Resource Requirements**
- **CPU**: 1 core for log processing and I/O operations
- **Memory**: 512MB-1GB for buffering and processing
- **Storage**: 10GB-100GB for log storage (scales with retention period)
- **Network**: Reliable connectivity to Kafka cluster

## Error Handling & Recovery

### **Fault Tolerance**
```python
# Robust error handling with graceful degradation
async def safe_log_processing(log_entry: dict):
    try:
        processed = await process_log_entry(log_entry)
        await write_to_file(processed)
    except ValidationError as e:
        logger.warning(f"Malformed log entry: {e}")
        await write_to_error_file(log_entry)
    except IOError as e:
        logger.error(f"Disk write failed: {e}")
        await buffer_for_retry(log_entry)
```

**Recovery Strategies**:
- **Error Quarantine**: Malformed logs written to separate error files for analysis
- **Retry Logic**: Transient failures trigger exponential backoff retry attempts
- **Circuit Breaker**: Prevents cascade failures during sustained error conditions
- **Dead Letter Queue**: Persistent storage for logs that cannot be processed

### **Data Integrity**
- **Checksum Validation**: File integrity checking for corrupted log files
- **Atomic Writes**: Temporary file writes with atomic rename operations
- **Backup Procedures**: Regular backup of critical log files to external storage

## Integration Points

### **Log Sources**
- **Ingestion Service**: ETL pipeline logs and processing metrics
- **Recommendation API**: Request logs and performance metrics
- **Worker Services**: Background processing logs and error reports
- **Infrastructure Services**: Database, cache, and messaging system logs

### **Log Consumers**
- **Streamlit Dashboard**: Real-time log viewing and analysis interface
- **Alerting Systems**: Error detection and notification pipelines
- **Analytics Platforms**: Log data mining and pattern analysis
- **Compliance Systems**: Audit trail and regulatory reporting

## Advanced Features

### **Log Analytics Integration**
```python
# Structured query interface for log analysis
class LogAnalyzer:
    def __init__(self, log_directory: Path):
        self.log_dir = log_directory
    
    async def query_logs(self, filters: dict, time_range: tuple):
        # Efficient log querying with time-based indexing
        # Support for complex filter expressions
        # Real-time and historical log analysis
        pass
```

### **Real-Time Alerting**
```python
# Pattern-based alerting for critical events
ERROR_PATTERNS = [
    {"pattern": "level=ERROR", "threshold": 10, "window": "5m"},
    {"pattern": "OutOfMemoryError", "threshold": 1, "window": "1m"},
    {"pattern": "database.*timeout", "threshold": 5, "window": "10m"}
]
```

## Development & Testing

### **Local Development**
```bash
# Install dependencies
pip install -r requirements.txt

# Set up test environment
export LOG_OUTPUT_DIR="./test_logs"
export KAFKA_BROKERS="localhost:9092"

# Run log consumer
python main.py
```

### **Testing Strategy**
- **Unit Tests**: Log parsing and processing logic validation
- **Integration Tests**: End-to-end log flow from Kafka to files
- **Load Tests**: High-volume log processing performance validation
- **Recovery Tests**: Error handling and system recovery procedures

## Scaling Considerations

### **Horizontal Scaling**
- **Partition-Based Processing**: Multiple consumers process different Kafka partitions
- **Load Balancing**: Automatic partition assignment for optimal workload distribution
- **Shared Storage**: Network-attached storage for multi-instance deployments

### **Performance Optimization**
- **Index Creation**: Time-based indexing for efficient log searching
- **Compression**: Real-time log compression for storage efficiency
- **Caching**: In-memory caching of frequently accessed log metadata

---

**Production Impact**: This service provides the observability foundation essential for troubleshooting, performance optimization, and compliance monitoring. Its reliability and performance directly impact the team's ability to maintain and optimize the distributed system effectively. 