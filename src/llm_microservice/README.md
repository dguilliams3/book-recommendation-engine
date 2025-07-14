# LLM Microservice

A production-ready, containerized FastAPI microservice for OpenAI LLM calls via LangChain with comprehensive caching, logging, and monitoring.

## Features

- **🚀 Production-Ready**: Built with FastAPI, comprehensive error handling, and monitoring
- **🔄 Idempotency**: Request ID-based deduplication prevents duplicate charges
- **⚡ Caching**: Redis-based caching with graceful fallback to in-memory cache
- **📊 Logging**: Structured logging with Kafka producer and file fallback
- **🔄 Retry Logic**: Exponential backoff with jitter for OpenAI API calls
- **🛡️ Error Handling**: Comprehensive error mapping and structured responses
- **📈 Monitoring**: Built-in health checks and performance metrics
- **🐳 Containerized**: Single Docker image with all dependencies
- **🔧 Configurable**: Environment variable-based configuration
- **🎯 Smart Prompting**: Flexible prompt handling with LangChain integration

## Quick Start

### Docker (Recommended)

```bash
# Pull and run the container
docker run -p 8000:8000 \
  -e OPENAI_API_KEY=your_api_key_here \
  -e REDIS_URL=redis://localhost:6379 \
  llm-microservice:latest
```

### Local Development

```bash
# Clone and setup
git clone <repository>
cd llm-microservice
pip install -r requirements.txt

# Set environment variables
export OPENAI_API_KEY=your_api_key_here
export REDIS_URL=redis://localhost:6379

# Run the service
python main.py
```

## API Usage

### Basic Request

```bash
curl -X POST "http://localhost:8000/invoke" \
  -H "Content-Type: application/json" \
  -d '{
    "request_id": "550e8400-e29b-41d4-a716-446655440000",
    "user_prompt": "Explain quantum computing in simple terms",
    "openai_api_key": "sk-your-api-key-here",
    "model": "gpt-4o-mini",
    "temperature": 0.7,
    "max_tokens": 500
  }'
```

### Response Format

```json
{
  "success": true,
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "data": {
    "response": "Quantum computing uses quantum mechanical phenomena...",
    "confidence": 0.95
  },
  "usage": {
    "prompt_tokens": 25,
    "completion_tokens": 45,
    "total_tokens": 70
  },
  "performance": {
    "request_latency_ms": 1250.5,
    "llm_latency_ms": 1200.0,
    "cache_hit": false,
    "retry_count": 0,
    "rate_limited": false
  },
  "model": "gpt-4o-mini",
  "cached": false,
  "timestamp": "2025-01-11T10:30:00.123Z",
  "error": null
}
```

### Advanced Usage

#### With System Prompt

```json
{
  "request_id": "550e8400-e29b-41d4-a716-446655440001",
  "user_prompt": "Explain quantum computing",
  "system_prompt": "You are a physics professor. Explain concepts clearly and concisely.",
  "openai_api_key": "sk-your-api-key-here"
}
```

#### With Structured Messages

```json
{
  "request_id": "550e8400-e29b-41d4-a716-446655440002",
  "user_prompt": "Follow up question",
  "openai_api_key": "sk-your-api-key-here",
  "messages": [
    {"type": "system", "content": "You are a helpful assistant."},
    {"type": "human", "content": "What is quantum computing?"},
    {"type": "ai", "content": "Quantum computing is a type of computation..."},
    {"type": "human", "content": "Can you give me a simple example?"}
  ]
}
```

## Configuration

### Environment Variables

```bash
# Core Configuration
LLM_APP_NAME=llm-microservice
LLM_VERSION=1.0.0
LLM_DEBUG=false
LLM_HOST=0.0.0.0
LLM_PORT=8000
LLM_LOG_LEVEL=INFO

# Default Model Settings
LLM_DEFAULT_MODEL=gpt-4o-mini
LLM_DEFAULT_TEMPERATURE=0.7
LLM_DEFAULT_MAX_TOKENS=1000

# Authentication
LLM_ENABLE_AUTH=false

# Redis Configuration
REDIS_URL=redis://localhost:6379
REDIS_ENABLED=true
REDIS_TTL_HOURS=24
REDIS_MAX_CONNECTIONS=10

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_ENABLED=false
KAFKA_REQUEST_TOPIC=llm-requests
KAFKA_METRICS_TOPIC=llm-metrics
KAFKA_ERROR_TOPIC=llm-errors

# Logging
LLM_LOG_FILE=/app/logs/llm-service.log
```

### Supported Models

- `gpt-4o-mini` (default)
- `gpt-4o`
- `gpt-3.5-turbo`
- `gpt-3.5-turbo-16k`

## Endpoints

### POST /invoke

Main endpoint for LLM calls with:
- Request validation
- Caching and idempotency
- Error handling and retries
- Performance tracking

### GET /health

Health check endpoint that returns:
- Service status
- Dependency health (Redis, Kafka, LangChain)
- Version information

### GET /stats

Statistics endpoint with:
- Request counts and performance metrics
- Cache hit/miss ratios
- Token usage and cost tracking
- Error rates and retry counts

### GET /docs

Auto-generated OpenAPI documentation (Swagger UI)

## Error Handling

The service provides structured error responses with proper HTTP status codes:

```json
{
  "success": false,
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "error": {
    "error_type": "validation_error",
    "error_code": "INVALID_API_KEY",
    "message": "Invalid OpenAI API key format",
    "field": "openai_api_key",
    "details": {"expected_format": "sk-..."}
  },
  "timestamp": "2025-01-11T10:30:00.123Z"
}
```

### Error Types

- `400` - Validation errors
- `401` - Authentication errors
- `409` - Duplicate request ID
- `429` - Rate limit exceeded
- `500` - Internal server errors
- `503` - OpenAI API unavailable

## Architecture

### Components

1. **FastAPI Application** - HTTP API layer with middleware
2. **LLM Service** - Core business logic with LangChain integration
3. **Cache Service** - Redis-based caching with in-memory fallback
4. **Logging Service** - Structured logging with Kafka and file fallback
5. **Error Handling** - Comprehensive error mapping and responses

### Data Flow

1. **Request Validation** - Pydantic models validate all inputs
2. **Idempotency Check** - Cache lookup by request ID
3. **LLM Processing** - LangChain integration with retry logic
4. **Response Caching** - Store results for future requests
5. **Logging** - Structured logs to Kafka/file
6. **Response** - Formatted response with metadata

## Deployment

### Docker Compose

```yaml
version: '3.8'

services:
  llm-microservice:
    image: llm-microservice:latest
    ports:
      - "8000:8000"
    environment:
      - REDIS_URL=redis://redis:6379
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
      - KAFKA_ENABLED=true
      - LLM_LOG_LEVEL=INFO
    depends_on:
      - redis
      - kafka
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data

  kafka:
    image: confluentinc/cp-kafka:latest
    ports:
      - "9092:9092"
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    depends_on:
      - zookeeper

  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

volumes:
  redis_data:
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: llm-microservice
spec:
  replicas: 3
  selector:
    matchLabels:
      app: llm-microservice
  template:
    metadata:
      labels:
        app: llm-microservice
    spec:
      containers:
      - name: llm-microservice
        image: llm-microservice:latest
        ports:
        - containerPort: 8000
        env:
        - name: REDIS_URL
          value: "redis://redis-service:6379"
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "kafka-service:9092"
        resources:
          requests:
            cpu: 100m
            memory: 256Mi
          limits:
            cpu: 500m
            memory: 512Mi
        readinessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 10
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 15
          periodSeconds: 20
---
apiVersion: v1
kind: Service
metadata:
  name: llm-microservice
spec:
  selector:
    app: llm-microservice
  ports:
  - port: 80
    targetPort: 8000
  type: LoadBalancer
```

## Monitoring

### Health Checks

```bash
# Basic health check
curl http://localhost:8000/health

# Detailed statistics
curl http://localhost:8000/stats
```

### Metrics

The service tracks:
- Request latency and throughput
- Cache hit/miss ratios
- Token usage and costs
- Error rates by type
- Retry counts and patterns

### Logging

Structured logs are sent to:
- **Kafka topics** (if enabled)
- **File logs** (fallback)
- **Console output** (development)

## Development

### Running Tests

```bash
# Install test dependencies
pip install pytest pytest-asyncio pytest-cov

# Run tests with coverage
pytest --cov=src tests/

# Run specific test categories
pytest tests/test_api.py -v
pytest tests/test_models.py -v
pytest tests/test_services.py -v
```

### Code Quality

```bash
# Format code
black src/
isort src/

# Lint code
flake8 src/
mypy src/
```

## Troubleshooting

### Common Issues

1. **Redis Connection Failed**
   - Check Redis URL and connectivity
   - Service gracefully falls back to in-memory cache

2. **Kafka Producer Errors**
   - Verify Kafka configuration
   - Service falls back to file logging

3. **OpenAI API Errors**
   - Check API key format and validity
   - Monitor rate limits and quotas

4. **High Latency**
   - Enable Redis caching
   - Check network connectivity
   - Monitor OpenAI API performance

### Debug Mode

```bash
# Enable debug logging
export LLM_DEBUG=true
export LLM_LOG_LEVEL=DEBUG

# Run with debug output
python main.py
```

## License

This project is licensed under the MIT License.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## Support

For issues and questions:
- Check the troubleshooting section
- Review the API documentation at `/docs`
- Open an issue in the repository 