import logging
from datetime import date, timedelta
from app.services.ingestion_service import IngestionService
from app.config import SFTP_CONFIG

logger = logging.getLogger(__name__)

def auto_ingest_yesterday():
    """
    Télécharge et ingère automatiquement le fichier du jour J-1 depuis le SFTP.
    """
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    file_name = f"{yesterday}_VocalCom_Incoming.csv"
    remote_path = f"{SFTP_CONFIG['remote_dir']}{file_name}"

    logger.info(f"remote_path: {remote_path}")
    logger.info(f"[AUTO] Ingestion automatique du fichier : {remote_path}")

    try:
        result = IngestionService.process_sftp_file(remote_path)
        logger.info(f"[AUTO] Résultat ingestion : {result}")
    except Exception as e:
        logger.error(f"[AUTO] Erreur lors de l'ingestion automatique: {e}", exc_info=True)
