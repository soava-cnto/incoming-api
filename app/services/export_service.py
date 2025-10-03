import pandas as pd
from app.db_writer import DBWriter
from app.config import DB_CONFIG, TABLE_NAME, VIEW_NAME
from datetime import date
import os

class ExportService:
    @staticmethod
    def export_csv(start_date: date, end_date: date, output_path="export.csv"):
        db_writer = DBWriter(DB_CONFIG, TABLE_NAME, VIEW_NAME)
        engine = db_writer.get_engine()

        query = f"""
            SELECT * FROM public.{VIEW_NAME}
            WHERE date_appel::date BETWEEN '{start_date}' AND '{end_date}'
        """
        df = pd.read_sql(query, engine)
        df.to_csv(output_path, index=False, encoding="utf-8")
        return os.path.abspath(output_path)
