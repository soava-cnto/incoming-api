import os
import logging
import pandas as pd
from app.csv_reader import CSVReader
from app.data_cleaner import DataCleaner
from app.db_writer import DBWriter
from app.config import DB_CONFIG, TABLE_NAME, VIEW_NAME, SFTP_CONFIG
from app.utils.sftp_client import SFTPClient  # 🔹 nouvelle classe

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

class IngestionService:
    @staticmethod
    def process_csv(path: str, include_comment=False):
        file_name = os.path.basename(path)
        writer = DBWriter(DB_CONFIG, TABLE_NAME, VIEW_NAME)

        if writer.already_imported(file_name):
            writer.close()
            return {"status": "skipped", "file": file_name}

        reader = CSVReader(path, chunksize=50000, include_comment=include_comment)

        total_rows = 0
        for i, chunk in enumerate(reader.get_chunks()):
            clean_df = DataCleaner.clean(chunk)
            writer.copy_dataframe(clean_df)
            total_rows += len(clean_df)

        writer.log_import(file_name)
        writer.close()
        return {"status": "success", "file": file_name, "rows": total_rows}
    
    
    @staticmethod
    def process_path(path: str, include_comment=False):
        """
        Si path = dossier → traite tous les CSV à l’intérieur.
        Si path = fichier → traite le fichier unique.
        """
        if os.path.isdir(path):
            results = []
            for file in os.listdir(path):
                if file.lower().endswith(".csv"):
                    file_path = os.path.join(path, file)
                    res = IngestionService.process_csv(file_path, include_comment)
                    results.append(res)
            return results
        else:
            return IngestionService.process_csv(path, include_comment)

    @staticmethod
    def process_sftp_file(remote_path: str, include_comment=False):
        """
        Lecture directe depuis un fichier CSV sur un serveur SFTP
        sans passer par le disque local.
        """
        logging.info(f"Ingestion depuis SFTP: {remote_path}")
        sftp_client = SFTPClient(SFTP_CONFIG)
        file_name = os.path.basename(remote_path)
        writer = DBWriter(DB_CONFIG, TABLE_NAME, VIEW_NAME)

        if writer.already_imported(file_name):
            writer.close()
            return {"status": "skipped", "file": file_name}

        # 🔸 Lecture du CSV distant en mémoire
        with sftp_client.open_file(remote_path) as remote_file:
            for chunk in pd.read_csv(remote_file, chunksize=50000):
                clean_df = DataCleaner.clean(chunk)
                writer.copy_dataframe(clean_df)

        writer.log_import(file_name)
        writer.close()
        sftp_client.close()

        return {"status": "success", "file": file_name}