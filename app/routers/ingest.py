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

@router.post("/path", status_code = status.HTTP_201_CREATED)
def ingest_path(path: str):
    return IngestionService.process_csv(path)
