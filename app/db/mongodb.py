from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import PyMongoError

from app.core.config import get_settings


class MongoDB:
    def __init__(self) -> None:
        self._client: AsyncIOMotorClient | None = None
        self._database: AsyncIOMotorDatabase | None = None

    async def connect(self) -> None:
        settings = get_settings()
        mongodb_uri = settings.mongodb_uri.strip()

        self._client = AsyncIOMotorClient(mongodb_uri, serverSelectionTimeoutMS=10000)
        try:
            await self._client.admin.command("ping")
        except PyMongoError as exc:
            self._client.close()
            self._client = None
            raise RuntimeError(
                "Unable to connect to MongoDB. Check MONGODB_URI credentials, Atlas IP allowlist, and cluster availability."
            ) from exc

        self._database = self._client[settings.mongodb_database]

    async def disconnect(self) -> None:
        if self._client is not None:
            self._client.close()
        self._client = None
        self._database = None

    @property
    def database(self) -> AsyncIOMotorDatabase:
        if self._database is None:
            raise RuntimeError("MongoDB connection is not initialized")
        return self._database


mongodb = MongoDB()


async def get_database() -> AsyncIOMotorDatabase:
    return mongodb.database


async def ensure_indexes() -> None:
    db = mongodb.database
    movies = db.movies
    jobs = db.jobs

    await movies.create_index([("release_date", DESCENDING)], name="idx_release_date")
    await movies.create_index(
        [("original_language", ASCENDING)], name="idx_original_language"
    )
    await movies.create_index([("vote_average", DESCENDING)], name="idx_vote_average")
    await movies.create_index(
        [("original_title", ASCENDING), ("release_date", ASCENDING)],
        unique=True,
        name="uniq_original_title_release_date",
    )

    await jobs.create_index([("created_at", DESCENDING)], name="idx_jobs_created_at")
