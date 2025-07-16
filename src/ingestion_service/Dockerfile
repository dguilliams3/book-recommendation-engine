# 1. --- Builder Stage ---
FROM python:3.11-slim AS builder

WORKDIR /app

# Install system dependencies for building
RUN apt-get update && apt-get install -y build-essential gcc

# Copy requirements and install dependencies
COPY src/ingestion_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install CLI dependencies
RUN pip install --no-cache-dir typer rich

# 2. --- Final Stage ---
FROM python:3.11-slim

WORKDIR /app

# Install Postgres client for schema bootstrapping
RUN apt-get update && apt-get install -y postgresql-client && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code and data
COPY src/common /app/common
COPY sql /app/sql
COPY data /app/data
COPY src/ingestion_service /app/ingestion_service
COPY src/cli.py /app/cli.py

# Include shared utilities used by the ingestion pipeline (e.g. readability estimator)
COPY src/recommendation_api /app/recommendation_api
COPY src/embedding /app/embedding

ENV PYTHONPATH="/app"

# Keep the original command as default, but allow CLI override
CMD ["python", "-m", "ingestion_service.main"] 