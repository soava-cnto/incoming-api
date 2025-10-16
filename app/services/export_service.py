import pandas as pd
from app.db_writer import DBWriter
from app.config import DB_CONFIG, TABLE_NAME, VIEW_NAME
from datetime import date
import os

class ExportService:
    @staticmethod
    def export_csv_by_date(start_date: date, end_date: date, output_path="export.csv"):
        db_writer = DBWriter(DB_CONFIG, TABLE_NAME, VIEW_NAME)
        engine = db_writer.get_engine()

        query = f"""
            SELECT * FROM public.{VIEW_NAME}
            WHERE date_appel::date BETWEEN '{start_date}' AND '{end_date}'
        """
        df = pd.read_sql(query, engine)
        df.to_csv(output_path, index=False, encoding="utf-8")
        return os.path.abspath(output_path)
    
    @staticmethod
    def export_csv_by_week(start_week: str, end_week: str, output_path="export.csv"):
        db_writer = DBWriter(DB_CONFIG, TABLE_NAME, VIEW_NAME)
        engine = db_writer.get_engine()

        query = f"""
            SELECT * FROM public.{VIEW_NAME}
            WHERE semaine::text BETWEEN '{start_week}' AND '{end_week}'
        """
        df = pd.read_sql(query, engine)
        df.to_csv(output_path, index=False, encoding="utf-8")
        return os.path.abspath(output_path)
    
    @staticmethod
    def export_all_to_csv(output_dir="export_all_data.csv"):
        db_writer = DBWriter(DB_CONFIG, TABLE_NAME, VIEW_NAME)
        engine = db_writer.get_engine()

        # Si c’est un dossier → crée le fichier à l’intérieur
        if os.path.isdir(output_dir):
            output_path = os.path.join(output_dir, "export_all_data.csv")
        else:
            output_path = output_dir  # si un chemin complet a été passé

        query = f"""
            SELECT * FROM public.{VIEW_NAME}
        """
        df = pd.read_sql(query, engine)
        df.to_csv(output_path, index=False, encoding="utf-8")
        return os.path.abspath(output_path)

