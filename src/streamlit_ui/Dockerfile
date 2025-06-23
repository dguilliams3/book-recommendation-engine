# 1. --- Builder Stage ---
FROM python:3.11-slim as builder

WORKDIR /app

# Copy requirements and install dependencies
COPY src/streamlit_ui/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# 2. --- Final Stage ---
FROM python:3.11-slim

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY src/common /app/common
COPY src/streamlit_ui /app/streamlit_ui

ENV PYTHONPATH="/app"

EXPOSE 8501
CMD ["streamlit", "run", "streamlit_ui/app.py", "--server.port", "8501", "--server.address", "0.0.0.0"] 