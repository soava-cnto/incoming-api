import pandas as pd
import re
from app.db_writer import DBWriter
from app.config import DB_CONFIG, TABLE_NAME, VIEW_NAME, VIEW_FLASHPROD
from datetime import date
import os

class ExportService:
    @staticmethod
    def export_csv_by_date(start_date: date, end_date: date, output_path="export.csv"):
        db_writer = DBWriter(DB_CONFIG, TABLE_NAME, VIEW_NAME)
        engine = db_writer.get_engine()

        query = f"""
            SELECT * FROM incoming.{VIEW_NAME}
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
            SELECT * FROM incoming.{VIEW_NAME}
            WHERE semaine::text BETWEEN '{start_week}' AND '{end_week}'
        """
        df = pd.read_sql(query, engine)
        df.to_csv(output_path, index=False, encoding="utf-8")
        return os.path.abspath(output_path)
    
    @staticmethod
    def export_all_to_csv(output_dir="./directory"):
        db_writer = DBWriter(DB_CONFIG, TABLE_NAME, VIEW_NAME, VIEW_FLASHPROD)
        engine = db_writer.get_engine()

        # Si c’est un dossier → crée le fichier à l’intérieur
        if os.path.isdir(output_dir):
            output_path = os.path.join(output_dir, "incoming_all_data.csv")
        else:
            output_path = output_dir  # si un chemin complet a été passé

        query = f"""
            SELECT * FROM incoming.{VIEW_NAME}
        """
        df = pd.read_sql(query, engine)
        df.to_csv(output_path, index=False, encoding="utf-8")
        return os.path.abspath(output_path)
    
    @staticmethod
    def export_flashprod_to_csv(output_dir="./directory"):
        db_writer = DBWriter(DB_CONFIG, TABLE_NAME,VIEW_NAME, VIEW_FLASHPROD)
        engine = db_writer.get_engine()

        # Si c’est un dossier → crée le fichier à l’intérieur
        if os.path.isdir(output_dir):
            output_path = os.path.join(output_dir, "flashprod_data.csv")
        else:
            output_path = output_dir  # si un chemin complet a été passé

        query = f"""
            SELECT * FROM incoming.{VIEW_FLASHPROD}
        """
        df = pd.read_sql(query, engine)
        df.to_csv(output_path, index=False, encoding="utf-8")
        return os.path.abspath(output_path)
    
    @staticmethod
    def export_all_to_csv_by_week(start_week: str, end_week: str, output_dir="./directory"):
        db_writer = DBWriter(DB_CONFIG, TABLE_NAME, VIEW_NAME, VIEW_FLASHPROD)
        engine = db_writer.get_engine()

        # Si c’est un dossier → crée le fichier à l’intérieur
        if os.path.isdir(output_dir):
            output_path = os.path.join(output_dir, f"incoming_{start_week}_{end_week}_data.csv")
        else:
            output_path = output_dir  # si un chemin complet a été passé

        query = f"""
            SELECT * FROM incoming.{VIEW_NAME}
            WHERE semaine::text BETWEEN '{start_week}' AND '{end_week}'
        """
        df = pd.read_sql(query, engine)
        df.to_csv(output_path, index=False, encoding="utf-8")
        return os.path.abspath(output_path)
    
    @staticmethod
    def export_all_mvola(start_year: int, end_year: int, output_dir="./directory"):
        db_writer = DBWriter(DB_CONFIG, TABLE_NAME, VIEW_NAME, VIEW_FLASHPROD)
        engine = db_writer.get_engine()

        if os.path.isdir(output_dir):
            output_path = os.path.join(output_dir, f"base_mvola_{start_year}_{end_year}.csv")
            # output_path = os.path.join(output_dir, f"base_yas_comores_{start_year}_{end_year}.csv")
        else:
            output_path = output_dir

        query = f"""
            SELECT * FROM incoming.v_all_mvola
            WHERE annee BETWEEN '{start_year}' AND '{end_year}'
        """
        conn = engine.raw_connection()
        try:
            with conn.cursor() as cursor:
                with open(output_path, 'w', encoding='utf-8') as f:
                    cursor.copy_expert(
                        f"COPY ({query}) TO STDOUT WITH (FORMAT CSV, HEADER true, DELIMITER E'\\t')",
                        f
                    )
            conn.commit()
        finally:
            conn.close()

        # Remplacer les séparateurs décimaux . → , dans les colonnes numériques
        _replace_decimal_in_file(output_path)

        return os.path.abspath(output_path)


_NUMERIC_RE = re.compile(r"^-?\d+\.?\d*$")


def _replace_decimal_in_file(filepath: str):
    temp_path = filepath + ".tmp"
    with open(filepath, 'r', encoding='utf-8') as fin, \
         open(temp_path, 'w', encoding='utf-8') as fout:
        for line in fin:
            parts = line.split("\t")
            converted = [
                p.replace(".", ",") if _NUMERIC_RE.match(p.strip()) else p
                for p in parts
            ]
            fout.write("\t".join(converted))
    os.replace(temp_path, filepath)

