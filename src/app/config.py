from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    service_name: str = "mvp-oficina-recursos"
    service_version: str = "0.1.0"
    environment: Literal["dev", "test", "prod"] = "dev"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    database_url: str = ""
    jwt_secret_key: str = "unsafe"
    jwt_algorithm: Literal["HS256"] = "HS256"
    jwt_access_token_expire_minutes: int = 30

    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    def validate_runtime(self) -> None:
        if not self.database_url:
            raise ValueError("DATABASE_URL must be provided at runtime")
        if self.environment == "prod" and self.jwt_secret_key == "unsafe":
            raise ValueError("JWT_SECRET_KEY must be provided in production")


@lru_cache
def get_settings() -> Settings:
    return Settings()
