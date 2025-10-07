from io import StringIO
import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from app.csv_reader import CSVReader
from app.data_cleaner import DataCleaner
from app.db_writer import DBWriter
from app.config import DB_CONFIG, TABLE_NAME, VIEW_NAME, SFTP_CONFIG
from app.utils.sftp_client import SFTPClient  # 🔹 nouvelle classe
from app.utils.sftp_csv_reader import SFTPCSVReader

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


logger = logging.getLogger("AUTO")

class IngestionService:
    CHUNK_SIZE = 5000
    BAD_LINES_PATH = "bad_lines.csv"
    
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
    def process_sftp_file(remote_path: str):
        logger.info(f"[SFTP] Lecture du fichier : {remote_path}")

        # Téléchargement du fichier depuis SFTP
        sftp = SFTPClient()
        file_content = sftp.read_file(remote_path)
        encoding = sftp.detect_encoding(file_content)
        logger.info(f"[SFTP] Encodage détecté : {encoding}")

        # Lecture CSV avec tolérance
        str_io = StringIO(file_content.decode(encoding, errors="ignore"))
        good_rows = []
        bad_lines = []

        try:
            for chunk in pd.read_csv(
                str_io,
                chunksize=IngestionService.CHUNK_SIZE,
                sep=",",
                quotechar='"',
                on_bad_lines="skip",     # ignorer proprement les lignes invalides
                encoding=encoding,
                engine="python"
            ):
                good_rows.append(chunk)

        except Exception as e:
            logger.error(f"Erreur lecture CSV : {e}")
            raise

        # Fusion des bons morceaux
        if good_rows:
            df = pd.concat(good_rows, ignore_index=True)
            logger.info(f"{len(df)} lignes valides détectées.")
        else:
            logger.warning("Aucune ligne valide trouvée.")
            return "Aucune donnée valide."

        # Sauvegarde des lignes corrompues à part
        bad_lines = IngestionService.extract_bad_lines(file_content.decode(encoding))
        if bad_lines:
            IngestionService.save_bad_lines(bad_lines)
            logger.warning(f"{len(bad_lines)} lignes invalides sauvegardées dans {IngestionService.BAD_LINES_PATH}")

        # Insertion dans la base
        IngestionService.insert_into_db(df)
        return "Ingestion terminée avec succès."

    @staticmethod
    def extract_bad_lines(content: str):
        """
        Repère grossièrement les lignes multi-lignes brisées contenant des guillemets non fermés.
        """
        bad_lines = []
        for line in content.splitlines():
            if line.count('"') % 2 != 0:  # guillemets non pair → probablement ligne corrompue
                bad_lines.append(line)
        return bad_lines

    @staticmethod
    def save_bad_lines(bad_lines):
        with open(IngestionService.BAD_LINES_PATH, "a", encoding="utf-8") as f:
            for line in bad_lines:
                f.write(line + "\n")

    @staticmethod
    def insert_into_db(df: pd.DataFrame):
        from app.config import DB_CONFIG, TABLE_NAME

        engine = create_engine(
            f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        df.to_sql(TABLE_NAME, con=engine, if_exists="append", index=False)
        logger.info(f"{len(df)} lignes insérées dans la table {TABLE_NAME}")