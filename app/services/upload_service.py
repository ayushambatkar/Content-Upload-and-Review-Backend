import asyncio
import csv
import json
import logging
import os
from datetime import UTC, datetime
from uuid import uuid4

import aiofiles
from fastapi import HTTPException, UploadFile, status
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from app.core.config import get_settings
from app.models.movie import REQUIRED_CSV_HEADERS

logger = logging.getLogger(__name__)


_processing_tasks: dict[str, asyncio.Task] = {}


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _is_nullish(value: str | None) -> bool:
    if value is None:
        return True
    return value.strip().lower() in {"", "null", "none", "nan", "na", "n/a"}


def _parse_int(value: str | None, field_name: str) -> int | None:
    if _is_nullish(value):
        return None
    try:
        return int(float(value or "0"))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid integer for {field_name}: {value}") from exc


def _parse_float(value: str | None, field_name: str) -> float | None:
    if _is_nullish(value):
        return None
    try:
        return float(value or "0")
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid float for {field_name}: {value}") from exc


def _parse_release_date(value: str | None) -> datetime | None:
    if _is_nullish(value):
        return None

    text = (value or "").strip()
    formats = ["%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%m/%d/%Y"]
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"Invalid release_date: {value}") from exc


def _parse_languages(value: str | None) -> list[str]:
    if _is_nullish(value):
        return []

    text = (value or "").strip()
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed if str(item).strip()]
        except json.JSONDecodeError:
            pass

    for separator in ["|", ";", ","]:
        if separator in text:
            return [part.strip() for part in text.split(separator) if part.strip()]

    return [text]


def _clean_string(value: str | None) -> str | None:
    if _is_nullish(value):
        return None
    return (value or "").strip()


def _validate_headers(fieldnames: list[str] | None) -> None:
    if fieldnames is None:
        raise ValueError("CSV file does not include a header row")

    normalized = [name.strip() for name in fieldnames]
    if normalized != REQUIRED_CSV_HEADERS:
        raise ValueError(
            "Invalid CSV headers. Expected exact columns/order: "
            + ", ".join(REQUIRED_CSV_HEADERS)
        )


def _parse_row(row: dict[str, str]) -> dict:
    release_date = _parse_release_date(row.get("release_date"))
    original_title = _clean_string(row.get("original_title"))

    if release_date is None or not original_title:
        raise ValueError(
            "Row missing required unique key fields original_title or release_date"
        )

    return {
        "budget": _parse_float(row.get("budget"), "budget"),
        "homepage": _clean_string(row.get("homepage")),
        "original_language": _clean_string(row.get("original_language")),
        "original_title": original_title,
        "overview": _clean_string(row.get("overview")),
        "release_date": release_date,
        "revenue": _parse_float(row.get("revenue"), "revenue"),
        "runtime": _parse_int(row.get("runtime"), "runtime"),
        "status": _clean_string(row.get("status")),
        "title": _clean_string(row.get("title")),
        "vote_average": _parse_float(row.get("vote_average"), "vote_average"),
        "vote_count": _parse_int(row.get("vote_count"), "vote_count"),
        "production_company_id": _parse_int(
            row.get("production_company_id"), "production_company_id"
        ),
        "genre_id": _parse_int(row.get("genre_id"), "genre_id"),
        "languages": _parse_languages(row.get("languages")),
    }


async def create_job(db: AsyncIOMotorDatabase, filename: str) -> str:
    job_id = str(uuid4())
    now = _now_utc()
    await db.jobs.insert_one(
        {
            "_id": job_id,
            "status": "pending",
            "filename": filename,
            "total_rows": 0,
            "processed_rows": 0,
            "failed_rows": 0,
            "error_message": None,
            "created_at": now,
            "updated_at": now,
        }
    )
    return job_id


async def save_upload_to_temp(upload_file: UploadFile, job_id: str) -> str:
    settings = get_settings()
    os.makedirs(settings.temp_upload_dir, exist_ok=True)
    destination = os.path.join(settings.temp_upload_dir, f"{job_id}.csv")

    content_length = upload_file.headers.get("content-length")
    if content_length is not None and int(content_length) > settings.max_upload_bytes:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Uploaded file exceeds 1GB limit",
        )

    total_written = 0
    chunk_size = 8 * 1024 * 1024

    async with aiofiles.open(destination, "wb") as out_file:
        while True:
            chunk = await upload_file.read(chunk_size)
            if not chunk:
                break
            total_written += len(chunk)
            if total_written > settings.max_upload_bytes:
                raise HTTPException(
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    detail="Uploaded file exceeds 1GB limit",
                )
            await out_file.write(chunk)

    await upload_file.close()
    return destination


async def _update_job_progress(
    db: AsyncIOMotorDatabase,
    job_id: str,
    *,
    status_value: str | None = None,
    total_rows: int,
    processed_rows: int,
    failed_rows: int,
    error_message: str | None = None,
) -> None:
    update_doc: dict = {
        "total_rows": total_rows,
        "processed_rows": processed_rows,
        "failed_rows": failed_rows,
        "updated_at": _now_utc(),
        "error_message": error_message,
    }
    if status_value is not None:
        update_doc["status"] = status_value

    await db.jobs.update_one({"_id": job_id}, {"$set": update_doc})


async def _flush_batch(
    db: AsyncIOMotorDatabase, operations: list[UpdateOne]
) -> tuple[int, int]:
    if not operations:
        return 0, 0

    try:
        await db.movies.bulk_write(operations, ordered=False)
        return len(operations), 0
    except BulkWriteError as exc:
        details = exc.details or {}
        write_errors = details.get("writeErrors", [])
        failed_count = len(write_errors)
        processed_count = max(len(operations) - failed_count, 0)
        logger.exception("Bulk write encountered errors", extra={"details": details})
        return processed_count, failed_count


async def process_csv_job(
    db: AsyncIOMotorDatabase, job_id: str, file_path: str
) -> None:
    settings = get_settings()
    total_rows = 0
    processed_rows = 0
    failed_rows = 0

    await _update_job_progress(
        db,
        job_id,
        status_value="processing",
        total_rows=0,
        processed_rows=0,
        failed_rows=0,
    )

    try:
        with open(
            file_path, mode="r", encoding="utf-8-sig", newline="", errors="replace"
        ) as csv_file:
            reader = csv.DictReader(csv_file)
            _validate_headers(reader.fieldnames)

            operations: list[UpdateOne] = []
            for row in reader:
                total_rows += 1

                try:
                    movie_doc = _parse_row(row)
                    operations.append(
                        UpdateOne(
                            {
                                "original_title": movie_doc["original_title"],
                                "release_date": movie_doc["release_date"],
                            },
                            {"$setOnInsert": movie_doc},
                            upsert=True,
                        )
                    )
                except ValueError as exc:
                    failed_rows += 1
                    logger.warning(
                        "Skipping invalid row",
                        extra={
                            "job_id": job_id,
                            "row_number": total_rows + 1,
                            "error": str(exc),
                        },
                    )

                if len(operations) >= settings.csv_batch_size:
                    processed, failed = await _flush_batch(db, operations)
                    processed_rows += processed
                    failed_rows += failed
                    operations.clear()
                    await _update_job_progress(
                        db,
                        job_id,
                        total_rows=total_rows,
                        processed_rows=processed_rows,
                        failed_rows=failed_rows,
                    )

            if operations:
                processed, failed = await _flush_batch(db, operations)
                processed_rows += processed
                failed_rows += failed

        await _update_job_progress(
            db,
            job_id,
            status_value="completed",
            total_rows=total_rows,
            processed_rows=processed_rows,
            failed_rows=failed_rows,
        )
    except Exception as exc:
        logger.exception("CSV processing failed", extra={"job_id": job_id})
        await _update_job_progress(
            db,
            job_id,
            status_value="failed",
            total_rows=total_rows,
            processed_rows=processed_rows,
            failed_rows=failed_rows + 1,
            error_message=str(exc),
        )
    finally:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except OSError:
            logger.warning(
                "Unable to delete temporary upload file", extra={"path": file_path}
            )


def enqueue_csv_processing(
    db: AsyncIOMotorDatabase, job_id: str, file_path: str
) -> None:
    task = asyncio.create_task(process_csv_job(db, job_id, file_path))
    _processing_tasks[job_id] = task

    def _cleanup(done_task: asyncio.Task) -> None:
        _processing_tasks.pop(job_id, None)
        if done_task.cancelled():
            logger.warning("Processing task cancelled", extra={"job_id": job_id})
            return
        exception = done_task.exception()
        if exception is not None:
            logger.exception("Processing task crashed", exc_info=exception)

    task.add_done_callback(_cleanup)
