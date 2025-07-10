# User Ingest Service

FastAPI service for handling Reader Mode book uploads from public users.

## Features

- **JSON Upload**: POST `/upload_books` - Upload books via JSON payload
- **CSV Upload**: POST `/upload_books_csv` - Upload books via CSV file
- **Privacy**: User identifiers are hashed before storage
- **Validation**: File size limits (1MB), row limits (100), UTF-8 encoding
- **Event Publishing**: Publishes `user_uploaded` events to Kafka
- **Health Check**: GET `/health` - Service health status

## API Endpoints

### Upload Books (JSON)
```bash
curl -X POST "http://localhost:8004/upload_books" \
  -H "Content-Type: application/json" \
  -d '{
    "user_identifier": "user@example.com",
    "books": [
      {
        "title": "The Great Gatsby",
        "author": "F. Scott Fitzgerald",
        "rating": 4,
        "notes": "Classic American literature"
      }
    ]
  }'
```

### Upload Books (CSV)
```bash
curl -X POST "http://localhost:8004/upload_books_csv" \
  -F "file=@books.csv" \
  -F "user_identifier=user@example.com"
```

CSV format:
```csv
title,author,rating,notes
The Great Gatsby,F. Scott Fitzgerald,4,Classic American literature
```

## Configuration

Environment variables:
- `DB_HOST`: Database host (default: postgres)
- `DB_PORT`: Database port (default: 5432)
- `KAFKA_HOST`: Kafka host (default: kafka)
- `KAFKA_PORT`: Kafka port (default: 9092)

## Running

```bash
# Development
python main.py

# Docker
docker build -t user-ingest-service:reader-mode-v1 .
docker run -p 8004:8004 user-ingest-service:reader-mode-v1
``` 