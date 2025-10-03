from fastapi import APIRouter
from app.services.scheduler_service import SchedulerService

router = APIRouter()

CSV_DIR = r"D:\Utilisateurs\soava.rakotomanana\Documents"
SEPTEMBER_DIR = r"D:\Utilisateurs\soava.rakotomanana\Documents\september"

@router.post("/daily")
def run_daily():
    return SchedulerService.run_daily(CSV_DIR)

@router.post("/monthly")
def run_monthly():
    return SchedulerService.run_monthly(SEPTEMBER_DIR)
