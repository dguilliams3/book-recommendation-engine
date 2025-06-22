FROM python:3.11-slim
WORKDIR /app
COPY ../../src/common /app/common
COPY . /app/log_consumer
RUN pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false && \
    poetry init -n && \
    poetry add aiokafka
CMD ["python", "-m", "log_consumer.main"] 