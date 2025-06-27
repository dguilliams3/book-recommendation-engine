# 1. --- Builder Stage ---
FROM python:3.11-slim AS builder

WORKDIR /app

# Copy requirements and install dependencies
COPY src/recommendation_api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# 2. --- Final Stage ---
FROM python:3.11-slim

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY src/common /app/common
COPY src/recommendation_api /app/recommendation_api

ENV PYTHONPATH="/app"

EXPOSE 8000
CMD ["uvicorn", "recommendation_api.main:app", "--host", "0.0.0.0", "--port", "8000"] 