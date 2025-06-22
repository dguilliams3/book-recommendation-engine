FROM python:3.11-slim
WORKDIR /app
COPY ../../src/common /app/common
COPY . /app/recommendation_api
RUN pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false && \
    poetry init -n && \
    poetry add fastapi "uvicorn[standard]" langchain langgraph openai fastmcp aiokafka "sqlalchemy[asyncio]" aio_limiter
ENV PYTHONPATH=/app
EXPOSE 8000
CMD ["uvicorn", "recommendation_api.main:app", "--host", "0.0.0.0", "--port", "8000"] 