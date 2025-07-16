FROM python:3.11-slim

LABEL service="user_ingest_service"
LABEL version="reader-mode-v1"

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY src/user_ingest_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/user_ingest_service/ .
COPY src/common ./common

# Expose port
EXPOSE 8004

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8004/health || exit 1

# Run the application
CMD ["python", "main.py"] 