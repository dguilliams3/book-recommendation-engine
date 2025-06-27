# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY src/book_enrichment_worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install schedule

# Copy application code
COPY src/book_enrichment_worker/ .
COPY src/common/ /app/common/
COPY src/recommendation_api/tools /app/recommendation_api/tools

# Create a Python scheduler script
RUN echo '#!/usr/bin/env python3\n\
import schedule\n\
import time\n\
import subprocess\n\
import sys\n\
import os\n\
\n\
def run_enrichment():\n\
    try:\n\
        subprocess.run([sys.executable, "main.py"], check=True)\n\
        print("Book enrichment completed successfully")\n\
    except subprocess.CalledProcessError as e:\n\
        print(f"Book enrichment failed: {e}")\n\
\n\
# Schedule the job to run daily at 01:00\n\
schedule.every().day.at("01:00").do(run_enrichment)\n\
\n\
print("Book enrichment scheduler started. Will run daily at 01:00")\n\
\n\
# Run immediately on startup\n\
run_enrichment()\n\
\n\
# Keep the scheduler running\n\
while True:\n\
    schedule.run_pending()\n\
    time.sleep(60)\n\
' > /app/scheduler.py

# Make the scheduler executable
RUN chmod +x /app/scheduler.py

# Run the scheduler
CMD ["python", "/app/scheduler.py"] 