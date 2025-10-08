from fastapi import FastAPI
from app.routers import ingest, export, scheduler as scheduler_router  # 👈 on renomme ici

from apscheduler.schedulers.background import BackgroundScheduler
from app.jobs.sftp_ingest_job import auto_ingest_yesterday
import logging
from fastapi.responses import HTMLResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


app = FastAPI(title="Incoming API", version="1.0")

# 🚀 Scheduler (tâche quotidienne à 1h00)
job_scheduler = BackgroundScheduler()   # 👈 nouveau nom
job_scheduler.add_job(auto_ingest_yesterday, "cron", hour=3, minute=0)
job_scheduler.start()

# 🚀 Inclusion des routers FastAPI
app.include_router(ingest.router, prefix="/ingest", tags=["Ingestion"])
app.include_router(export.router, prefix="/export", tags=["Export"])
app.include_router(scheduler_router.router, prefix="/scheduler", tags=["Scheduler"])  # 👈 corrigé

@app.get("/", response_class=HTMLResponse)
def root():
    html_content = """
    <html>
        <head>
            <title>Ma Page</title>
        </head>
        <body>
            <h1>Bienvenue dans Incoming API 🚀</h1>
        </body>
    </html>
    """
    return html_content #{"message": "Bienvenue dans Incoming API 🚀"}
