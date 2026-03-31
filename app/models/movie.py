from datetime import datetime

from pydantic import BaseModel, Field


REQUIRED_CSV_HEADERS = [
    "budget",
    "homepage",
    "original_language",
    "original_title",
    "overview",
    "release_date",
    "revenue",
    "runtime",
    "status",
    "title",
    "vote_average",
    "vote_count",
    "production_company_id",
    "genre_id",
    "languages",
]


class MovieOut(BaseModel):
    budget: int | float | None = None
    homepage: str | None = None
    original_language: str | None = None
    original_title: str | None = None
    overview: str | None = None
    release_date: datetime | None = None
    revenue: int | float | None = None
    runtime: int | None = None
    status: str | None = None
    title: str | None = None
    vote_average: float | None = None
    vote_count: int | None = None
    production_company_id: int | None = None
    genre_id: int | None = None
    languages: list[str] = Field(default_factory=list)


class MoviesResponse(BaseModel):
    total: int
    page: int
    limit: int
    results: list[MovieOut]
