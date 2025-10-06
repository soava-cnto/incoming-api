from fastapi import APIRouter, UploadFile, File, status
import shutil, tempfile
from app.services.ingestion_service import IngestionService

router = APIRouter()

@router.post("/file", status_code = status.HTTP_201_CREATED)
async def ingest_file(file: UploadFile = File(...)):
    tmp_dir = tempfile.mkdtemp()
    tmp_path = f"{tmp_dir}/{file.filename}"
    with open(tmp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    return IngestionService.process_csv(tmp_path)

@router.post("/path", status_code=status.HTTP_201_CREATED)
def ingest_path(path: str):
    """
    Permet d'envoyer soit un fichier CSV, soit un dossier contenant plusieurs CSV.
    """
    return IngestionService.process_path(path)

@router.post("/sftp", status_code=status.HTTP_201_CREATED)
def ingest_from_sftp(remote_path: str = "/home/connecteo/files/Received/"):
    """
    Ingestion directe depuis un fichier CSV sur un serveur SFTP.
    Exemple d'appel :
      POST /ingest/sftp?remote_path=/remote/path/mon_fichier.csv
    """
    return IngestionService.process_sftp_file(remote_path)

@router.post("/sftp/auto", status_code=status.HTTP_201_CREATED)
def ingest_yesterday():
    """
    Ingestion manuelle du fichier d'hier depuis le SFTP
    """
    from app.jobs.sftp_ingest_job import auto_ingest_yesterday
    return auto_ingest_yesterday()
