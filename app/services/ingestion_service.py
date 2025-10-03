import os
import logging
from app.csv_reader import CSVReader
from app.data_cleaner import DataCleaner
from app.db_writer import DBWriter
from app.config import DB_CONFIG, TABLE_NAME, VIEW_NAME

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
