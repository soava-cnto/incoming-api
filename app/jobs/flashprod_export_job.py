import logging
import paramiko
import os
from datetime import date
from dotenv import load_dotenv
from app.services.export_service import ExportService

load_dotenv()

logger = logging.getLogger(__name__)

UBUNTU_HOST = os.getenv("UBUNTU_HOST")
UBUNTU_PORT = int(os.getenv("UBUNTU_PORT"))
UBUNTU_USER = os.getenv("UBUNTU_USER")
UBUNTU_PASSWORD = os.getenv("UBUNTU_PASSWORD")
UBUNTU_REMOTE_DIR = os.getenv("UBUNTU_REMOTE_DIR")

LOCAL_OUTPUT_DIR = os.getenv("FLASHPROD_EXPORT_DIR")

def auto_export_flashprod():
    """
    Exporte flashprod_data.csv et le copie vers le serveur Ubuntu.
    """
    today = date.today().strftime("%Y-%m-%d")
    # local_filename = f"flashprod_data_{today}.csv"
    local_filename = f"flashprod_data.csv"
    local_path = os.path.join(LOCAL_OUTPUT_DIR, local_filename)

    try:
        export_path = ExportService.export_flashprod_to_csv(local_path)
        logger.info(f"[FLASHPROD] Fichier exporté : {export_path}")
    except Exception as e:
        logger.error(f"[FLASHPROD] Erreur export CSV : {e}", exc_info=True)
        return {"status": "export_error", "message": str(e)}

    try:
        transport = paramiko.Transport((UBUNTU_HOST, UBUNTU_PORT))
        transport.connect(username=UBUNTU_USER, password=UBUNTU_PASSWORD)
        sftp = paramiko.SFTPClient.from_transport(transport)

        try:
            sftp.stat(UBUNTU_REMOTE_DIR)
        except FileNotFoundError:
            sftp.mkdir(UBUNTU_REMOTE_DIR)

        remote_path = os.path.join(UBUNTU_REMOTE_DIR, local_filename).replace("\\", "/")
        sftp.put(local_path, remote_path)
        logger.info(f"[FLASHPROD] Fichier copié vers {remote_path}")

        sftp.close()
        transport.close()

        return {"status": "success", "file": local_filename, "remote": remote_path}

    except Exception as e:
        logger.error(f"[FLASHPROD] Erreur copie SFTP vers Ubuntu : {e}", exc_info=True)
        return {"status": "sftp_error", "message": str(e)}
