from fastapi import FastAPI
app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok", "stub": True}

@app.post("/process")
def process():
    return {"message": "stubbed"} 