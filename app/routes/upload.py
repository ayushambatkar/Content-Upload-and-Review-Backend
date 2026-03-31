from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db.mongodb import get_database
from app.models.job import JobCreateResponse
from app.services.upload_service import (
    create_job,
    enqueue_csv_processing,
    save_upload_to_temp,
)

router = APIRouter(prefix="", tags=["upload"])


@router.post(
    "/upload", response_model=JobCreateResponse, status_code=status.HTTP_202_ACCEPTED
)
async def upload_csv(
    file: UploadFile = File(...),
    db: AsyncIOMotorDatabase = Depends(get_database),
) -> JobCreateResponse:
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only CSV files are supported",
        )

    job_id = await create_job(db, filename=file.filename)
    file_path = await save_upload_to_temp(file, job_id)
    enqueue_csv_processing(db, job_id, file_path)

    return JobCreateResponse(job_id=job_id, status="pending")
