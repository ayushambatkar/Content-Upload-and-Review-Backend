import logging

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from app.core.config import get_settings
from app.db.mongodb import ensure_indexes, mongodb
from app.routes.jobs import router as jobs_router
from app.routes.movies import router as movies_router
from app.routes.upload import router as upload_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

settings = get_settings()
app = FastAPI(title=settings.app_name)


@app.on_event("startup")
async def startup_event() -> None:
    await mongodb.connect()
    await ensure_indexes()


@app.on_event("shutdown")
async def shutdown_event() -> None:
    await mongodb.disconnect()


@app.exception_handler(Exception)
async def unhandled_exception_handler(_: Request, exc: Exception) -> JSONResponse:
    logging.getLogger(__name__).exception("Unhandled application error", exc_info=exc)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Internal server error"},
    )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


app.include_router(upload_router)
app.include_router(movies_router)
app.include_router(jobs_router)
