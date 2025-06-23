# 1. --- Builder Stage ---
FROM python:3.11-slim as builder

WORKDIR /app

# Copy requirements and install dependencies
COPY src/log_consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# 2. --- Final Stage ---
FROM python:3.11-slim

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY src/common /app/common
COPY src/log_consumer /app/log_consumer

ENV PYTHONPATH="/app"

CMD ["python", "-m", "log_consumer.main"] 