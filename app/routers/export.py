from fastapi import APIRouter
from datetime import date
from app.services.export_service import ExportService

router = APIRouter()

@router.get("/daily")
def export_daily(day: date):
    path = ExportService.export_csv_by_date(day, day, f"export_{day}.csv")
    return {"status": "ok", "file": path}

@router.get("/rangeofdate")
def export_range(start: date, end: date):
    path = ExportService.export_csv_by_date(start, end, f"export_{start}_{end}.csv")
    return {"status": "ok", "file": path}

@router.get("/weekly")
def export_daily(week: str):
    path = ExportService.export_csv_by_week(week, week, f"export_{week}.csv")
    return {"status": "ok", "file": path}

@router.get("/rangeofweek")
def export_range(start: str, end: str):
    path = ExportService.export_csv_by_week(start, end, f"export_{start}_{end}.csv")
    return {"status": "ok", "file": path}
