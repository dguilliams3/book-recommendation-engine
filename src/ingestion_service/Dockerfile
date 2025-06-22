FROM python:3.11-slim
WORKDIR /app
COPY ../../src/common /app/common
COPY ../../sql /app/sql
COPY ../../data /app/data
COPY . /app/ingestion_service
RUN apt-get update && apt-get install -y build-essential gcc && \
    pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false && \
    poetry init -n && \
    poetry add langchain langchain_openai openai aiokafka sqlalchemy[asyncio] psycopg2-binary pandas
CMD ["python", "-m", "ingestion_service.main"] 