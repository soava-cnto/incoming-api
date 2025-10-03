import os, glob, datetime
from app.services.ingestion_service import IngestionService

class SchedulerService:
    @staticmethod
    def run_daily(csv_dir: str):
        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        filename = f"{yesterday}_VocalCom_Incoming.csv"
        file_path = os.path.join(csv_dir, filename)

        if not os.path.exists(file_path):
            return {"status": "not_found", "file": file_path}

        return IngestionService.process_csv(file_path)

    @staticmethod
    def run_monthly(folder: str):
        files = glob.glob(os.path.join(folder, "*.csv"))
        if not files:
            return {"status": "empty", "folder": folder}

        results = []
        for f in files:
            results.append(IngestionService.process_csv(f))
        return {"status": "done", "files": results}
