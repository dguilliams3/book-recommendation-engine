FROM python:3.11-slim
WORKDIR /app
COPY ../../src/common /app/common
COPY . /app/graph_refresher
RUN apt-get update && apt-get install -y curl && \
    curl -L -o /usr/local/bin/supercronic https://github.com/aptible/supercronic/releases/download/v0.2.4/supercronic-linux-amd64 && \
    chmod +x /usr/local/bin/supercronic && \
    pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false && \
    poetry init -n --python "^3.11" && \
    poetry add asyncpg aiokafka langchain_openai
COPY --chmod=0644 <<'CRON' /app/graph_cron
0 2 * * * python -m graph_refresher.main
CRON
CMD ["supercronic","/app/graph_cron"] 