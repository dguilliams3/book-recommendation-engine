FROM python:3.11-slim

LABEL service="feedback_worker"
LABEL version="reader-mode-v1"

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY src/feedback_worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/feedback_worker/ .
COPY src/common ./common

# Health check (check if process is running)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD pgrep -f "python main.py" || exit 1

# Run the application
CMD ["python", "main.py"] 