from datetime import datetime, date, timedelta
from app.services.ingestion_service import IngestionService
from app.config import SFTP_CONFIG
import logging

logger = logging.getLogger(__name__)

def auto_ingest_yesterday():
    """
    Télécharge et ingère automatiquement le fichier du jour J-1 depuis le SFTP.
    Si une erreur survient, replanifie la tâche dans 20 minutes.
    """
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    file_name = f"{yesterday}_VocalCom_Incoming.csv"
    remote_path = f"{SFTP_CONFIG['remote_dir']}{file_name}"

    logger.info(f"remote_path: {remote_path}")
    logger.info(f"[AUTO] Ingestion automatique du fichier : {remote_path}")

    try:
        result = IngestionService.process_sftp_file(remote_path)
        logger.info(f"[AUTO] Résultat ingestion : {result}")
        return result

    except Exception as e:
        logger.error(f"[AUTO] Erreur lors de l'ingestion automatique: {e}", exc_info=True)

        # 🔁 Replanifier dans 20 minutes
        try:
            from app.main import job_scheduler  # le scheduler global
            next_run_time = datetime.now() + timedelta(minutes=20)

            job_scheduler.add_job(
                auto_ingest_yesterday,
                "date",
                run_date=next_run_time
            )
            logger.warning(f"[AUTO] Nouvelle tentative planifiée à {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")

        except Exception as inner_e:
            logger.error(f"Impossible de replanifier la tâche: {inner_e}")
