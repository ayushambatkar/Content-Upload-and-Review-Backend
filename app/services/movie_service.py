from datetime import datetime

from motor.motor_asyncio import AsyncIOMotorDatabase


def _build_sort(sort_by: str, sort_order: str) -> list[tuple[str, int]]:
    field_map = {
        "release_date": "release_date",
        "vote_average": "vote_average",
    }
    field_name = field_map.get(sort_by, "release_date")
    direction = 1 if sort_order == "asc" else -1
    return [(field_name, direction)]


def _build_release_year_filter(release_year: int) -> dict:
    start = datetime(release_year, 1, 1)
    end = datetime(release_year + 1, 1, 1)
    return {"release_date": {"$gte": start, "$lt": end}}


def _build_language_filter(language: str) -> dict:
    return {
        "$or": [
            {"original_language": language},
            {"languages": language},
        ]
    }


async def fetch_movies(
    db: AsyncIOMotorDatabase,
    *,
    page: int,
    limit: int,
    release_year: int | None,
    language: str | None,
    sort_by: str,
    sort_order: str,
) -> tuple[int, list[dict]]:
    query: dict = {}

    if release_year is not None:
        query.update(_build_release_year_filter(release_year))

    if language:
        query.update(_build_language_filter(language))

    total = await db.movies.count_documents(query)
    cursor = (
        db.movies.find(query, {"_id": 0})
        .sort(_build_sort(sort_by, sort_order))
        .skip((page - 1) * limit)
        .limit(limit)
    )

    results = await cursor.to_list(length=limit)
    return total, results
