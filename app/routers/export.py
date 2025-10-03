from fastapi import APIRouter
from datetime import date
from app.services.export_service import ExportService

router = APIRouter()

@router.get("/daily")
def export_daily(day: date):
    path = ExportService.export_csv(day, day, f"export_{day}.csv")
    return {"status": "ok", "file": path}

@router.get("/range")
def export_range(start: date, end: date):
    path = ExportService.export_csv(start, end, f"export_{start}_{end}.csv")
    return {"status": "ok", "file": path}
