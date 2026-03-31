from fastapi import APIRouter, Depends, HTTPException, status
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db.mongodb import get_database
from app.models.job import JobStatusResponse

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.get("/{job_id}", response_model=JobStatusResponse)
async def get_job_status(
    job_id: str,
    db: AsyncIOMotorDatabase = Depends(get_database),
) -> JobStatusResponse:
    job = await db.jobs.find_one(
        {"_id": job_id},
        {"_id": 0, "status": 1, "processed_rows": 1, "total_rows": 1, "failed_rows": 1},
    )

    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Job not found"
        )

    return JobStatusResponse(**job)
