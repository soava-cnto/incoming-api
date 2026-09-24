import logging
import paramiko
import os
from datetime import date
from dotenv import load_dotenv
from app.services.export_service import ExportService

load_dotenv()

logger = logging.getLogger(__name__)

LOCAL_OUTPUT_DIR = os.getenv("FLASHPROD_EXPORT_DIR")

def auto_export_mvola_data():
    """
    Exporte base_mvola_2026_2026.csv.
    """
    local_filename = f"base_mvola_2026_2026.csv"
    local_path = os.path.join(LOCAL_OUTPUT_DIR, local_filename)

    try:
        export_path = ExportService.export_all_mvola(2026, 2026, local_path)
        logger.info(f"[MVOLA] Fichier exporté : {export_path}")
    except Exception as e:
        logger.error(f"[] Erreur export CSV : {e}", exc_info=True)
        return {"status": "export_error", "message": str(e)}

    