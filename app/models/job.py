from datetime import datetime
from typing import Literal

from pydantic import BaseModel


JobStatusLiteral = Literal["pending", "processing", "completed", "failed"]


class JobCreateResponse(BaseModel):
    job_id: str
    status: JobStatusLiteral


class JobStatusResponse(BaseModel):
    status: JobStatusLiteral
    processed_rows: int
    total_rows: int
    failed_rows: int


class JobRecord(BaseModel):
    _id: str
    status: JobStatusLiteral
    filename: str
    total_rows: int = 0
    processed_rows: int = 0
    failed_rows: int = 0
    error_message: str | None = None
    created_at: datetime
    updated_at: datetime
