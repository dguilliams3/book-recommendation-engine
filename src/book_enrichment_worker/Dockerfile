FROM python:3.11-slim
WORKDIR /app
COPY ../../src/common /app/common
COPY . /app/book_enrichment_worker
COPY ../../src/recommendation_api/tools /app/tools
RUN apt-get update && apt-get install -y curl && \
    curl -L -o /usr/local/bin/supercronic https://github.com/aptible/supercronic/releases/download/v0.2.4/supercronic-linux-amd64 && \
    chmod +x /usr/local/bin/supercronic && \
    pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false && \
    poetry init -n && \
    poetry add aiohttp asyncpg textstat
COPY --chmod=0644 <<'CRON' /app/enrich_cron
0 1 * * * python -m book_enrichment_worker.main
CRON
ENV PYTHONPATH="/app"
CMD ["supercronic","/app/enrich_cron"] 