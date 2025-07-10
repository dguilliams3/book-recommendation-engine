# Feedback Worker

Kafka consumer service for processing Reader Mode feedback events.

## Features

- **Event Processing**: Consumes `feedback_events` from Kafka
- **Database Storage**: Stores feedback in PostgreSQL `feedback` table
- **Redis Caching**: Updates Redis cache with aggregated feedback scores
- **Privacy**: Works with hashed user identifiers
- **Metrics**: Prometheus metrics for monitoring

## Architecture

```
Kafka feedback_events → Feedback Worker → [PostgreSQL, Redis]
```

## Event Processing

The worker processes `FeedbackEvent` messages:

```json
{
  "event_type": "feedback_received",
  "user_hash_id": "abc123...",
  "book_id": "book_001",
  "score": 1,
  "timestamp": "2025-01-08T22:00:00Z",
  "source": "feedback_worker"
}
```

## Redis Cache Structure

Feedback scores are stored in Redis sorted sets:

```
Key: feedback:book:{book_id}
Value: Sorted set of (user_hash_id, cumulative_score)
```

## Configuration

Environment variables:
- `DB_HOST`: Database host (default: postgres)
- `DB_PORT`: Database port (default: 5432)
- `KAFKA_HOST`: Kafka host (default: kafka)
- `KAFKA_PORT`: Kafka port (default: 9092)
- `REDIS_URL`: Redis connection URL (default: redis://redis:6379/0)

## Running

```bash
# Development
python main.py

# Docker
docker build -t feedback-worker:reader-mode-v1 .
docker run feedback-worker:reader-mode-v1
```

## Monitoring

The worker exposes Prometheus metrics:
- `kafka_messages_consumed_total`: Total messages processed
- `job_runs_total`: Total job executions
- `job_duration_seconds`: Job execution time 