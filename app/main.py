from fastapi import FastAPI
from app.routers import ingest, export, scheduler

app = FastAPI(title="Incoming API", version="1.0")

app.include_router(ingest.router, prefix="/ingest", tags=["Ingestion"])
app.include_router(export.router, prefix="/export", tags=["Export"])
app.include_router(scheduler.router, prefix="/scheduler", tags=["Scheduler"])

@app.get("/")
def root():
    return {"message": "Bienvenue dans Incoming API 🚀"}
