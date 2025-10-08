import errno
from io import StringIO
import os
import logging
from time import time
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
    def clean_csv_remove_comment_column(raw_data: bytes, encoding: str) -> StringIO:
        """
        Supprime la dernière colonne (COMMENTAIRE) de chaque ligne CSV avant lecture.
        Cette approche évite les erreurs liées aux retours à la ligne dans le champ commentaire.
        """
        decoded = raw_data.decode(encoding, errors="ignore").splitlines()

        # Si le fichier est vide
        if not decoded:
            raise ValueError("Fichier CSV vide ou illisible")

        # Détection de la colonne COMMENTAIRE
        header = decoded[0].split(",")
        if "COMMENTAIRE" in header:
            comment_idx = header.index("COMMENTAIRE")
            logger.info(f"[CLEAN] Colonne 'COMMENTAIRE' détectée à l’index {comment_idx}, suppression.")
        else:
            comment_idx = None
            logger.warning("[CLEAN] Aucune colonne 'COMMENTAIRE' détectée, rien à supprimer.")

        cleaned_lines = []
        for line in decoded:
            # on coupe avant la colonne commentaire si elle existe
            if comment_idx is not None:
                parts = line.split(",")
                if len(parts) > comment_idx:
                    parts = parts[:comment_idx]
                cleaned_lines.append(",".join(parts))
            else:
                cleaned_lines.append(line)

        cleaned_csv = "\n".join(cleaned_lines)
        return StringIO(cleaned_csv)


    @staticmethod
    def process_sftp_file(remote_path: str):
        """
        Télécharge un fichier CSV depuis le SFTP, détecte l'encodage,
        nettoie les colonnes commentaires, normalise les noms de colonnes,
        vérifie si le fichier a déjà été importé, insère les données en base,
        et log l'import pour suivi.
        Si PermissionError, retente toutes les 20 minutes jusqu'à succès.
        """
        file_name = os.path.basename(remote_path)
        logger.info(f"[SFTP] Début du traitement du fichier {file_name}")
        sftp_client = None
        db_writer = DBWriter(DB_CONFIG, TABLE_NAME, VIEW_NAME)

        try:
            # Vérification si le fichier a déjà été importé
            if db_writer.already_imported(file_name):
                db_writer.close()
                logger.info(f"[SFTP] Fichier {file_name} déjà importé, skipped.")
                return {"status": "skipped", "file": file_name}

            while True:
                try:
                    # Connexion au SFTP
                    sftp_client = SFTPClient(SFTP_CONFIG)

                    # Lecture du fichier distant
                    raw_data = sftp_client.read_file(remote_path)
                    logger.info(f"[SFTP] Lecture réussie du fichier {file_name} ({len(raw_data)} octets)")

                    # Détection de l'encodage
                    encoding = sftp_client.detect_encoding(raw_data)
                    logger.info(f"[SFTP] Encodage détecté : {encoding}")

                    # Nettoyage du CSV pour supprimer la colonne "COMMENTAIRE"
                    str_io = IngestionService.clean_csv_remove_comment_column(raw_data, encoding)

                    inserted_rows = 0

                    # Lecture du CSV par chunk
                    for chunk in pd.read_csv(
                        str_io,
                        chunksize=IngestionService.CHUNK_SIZE,
                        dtype=str,
                        encoding=encoding,
                        on_bad_lines="warn"
                    ):
                        # Nettoyage complet via DataCleaner
                        clean_df = DataCleaner.clean(chunk)

                        # Insertion dans la base
                        db_writer.copy_dataframe(clean_df)
                        inserted_rows += len(clean_df)

                    # Log du fichier importé pour suivi
                    db_writer.log_import(file_name)
                    logger.info(f"[INGESTION] {inserted_rows} lignes insérées dans la base depuis {file_name}.")
                    return {"status": "success", "file": file_name, "rows": inserted_rows, "encoding": encoding}

                except PermissionError as e:
                    if getattr(e, "errno", None) == errno.EACCES or "[Errno 13]" in str(e):
                        logger.warning(f"[SFTP] Permission denied pour {file_name}, nouvelle tentative dans 20 minutes...")
                        time.sleep(20*60)  # attend 20 minutes
                    else:
                        raise  # relance pour les autres erreurs

        except Exception as e:
            logger.error(f"Erreur lors de l’ingestion SFTP du fichier {file_name} : {e}", exc_info=True)
            return {"status": "error", "file": file_name, "message": str(e)}

        finally:
            if sftp_client:
                sftp_client.close()
            db_writer.close()
                
    @staticmethod
    def insert_into_db(df: pd.DataFrame):
        """
        Insère un DataFrame déjà nettoyé dans la base.
        """
        clean_df = DataCleaner.clean(df)

        engine = create_engine(
            f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        clean_df.to_sql(TABLE_NAME, con=engine, if_exists="append", index=False)
        logger.info(f"{len(clean_df)} lignes insérées dans la table {TABLE_NAME}")
