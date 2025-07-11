from fastapi import FastAPI
from fastapi.responses import Response
try:
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    _PROM = True
except ImportError:
    _PROM = False
app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok", "stub": True}

@app.post("/process")
def process():
    return {"message": "stubbed"}

@app.get("/metrics")
def metrics():
    if not _PROM:
        return {"status": "prometheus_client missing"}
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/live")
def live():
    return {"status": "alive"}

@app.get("/ready")
def ready():
    return {"status": "ready"} 