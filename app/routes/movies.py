from typing import Literal

from fastapi import APIRouter, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db.mongodb import get_database
from app.models.movie import MovieOut, MoviesResponse
from app.services.movie_service import fetch_movies

router = APIRouter(prefix="/movies", tags=["movies"])


@router.get("", response_model=MoviesResponse)
async def get_movies(
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=20, ge=1, le=100),
    release_year: int | None = Query(default=None, ge=1800, le=3000),
    language: str | None = Query(default=None, min_length=1),
    sort_by: Literal["release_date", "vote_average"] = Query(default="release_date"),
    sort_order: Literal["asc", "desc"] = Query(default="desc"),
    db: AsyncIOMotorDatabase = Depends(get_database),
) -> MoviesResponse:
    total, records = await fetch_movies(
        db,
        page=page,
        limit=limit,
        release_year=release_year,
        language=language,
        sort_by=sort_by,
        sort_order=sort_order,
    )

    results = [MovieOut(**record) for record in records]
    return MoviesResponse(total=total, page=page, limit=limit, results=results)
