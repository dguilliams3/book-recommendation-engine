FROM python:3.11-slim
WORKDIR /app
COPY src/common /app/common
COPY src/incremental_workers/similarity /app
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "main.py"] 