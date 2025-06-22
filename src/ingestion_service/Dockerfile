FROM python:3.11-slim
WORKDIR /app
COPY ../../src/common /app/common
COPY . /app/ingestion_service
RUN pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false && \
    poetry init -n && \
    poetry add langchain langgraph openai aiokafka sqlalchemy[asyncio] psycopg2-binary pandas
CMD ["python", "-m", "ingestion_service.main"] 