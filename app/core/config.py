from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Content Upload and Review API"
    mongodb_uri: str = "mongodb://localhost:27017"
    mongodb_database: str = "content_review"
    csv_batch_size: int = 5000
    max_upload_bytes: int = 1024 * 1024 * 1024
    temp_upload_dir: str = "./tmp_uploads"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
