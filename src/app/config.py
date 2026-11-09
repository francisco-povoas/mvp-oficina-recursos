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
    jwt_issuer: str = "mvp-oficina-ordens"
    jwt_user_audience: str = "mvp-oficina-users"
    aws_region: str = "us-east-1"
    sqs_endpoint_url: str | None = None
    sqs_recursos_to_ordens_url: str = ""
    sqs_connect_timeout_seconds: float = 2.0
    sqs_read_timeout_seconds: float = 25.0
    sqs_send_timeout_seconds: float = 5.0
    publisher_worker_id: str = ""
    publisher_batch_size: int = 10
    publisher_lease_seconds: float = 30.0
    publisher_poll_interval_seconds: float = 1.0
    publisher_max_attempts: int = 8
    publisher_initial_backoff_seconds: float = 1.0
    publisher_max_backoff_seconds: float = 60.0
    publisher_contract_schema_path: str = (
        "contracts/vendor/ordens/saga-stock.v1.schema.json"
    )

    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    def validate_runtime(self) -> None:
        if not self.database_url:
            raise ValueError("DATABASE_URL must be provided at runtime")
        if self.environment == "prod" and self.jwt_secret_key == "unsafe":
            raise ValueError("JWT_SECRET_KEY must be provided in production")

    def validate_publisher_runtime(self) -> None:
        self.validate_runtime()
        if not self.sqs_recursos_to_ordens_url:
            raise ValueError("SQS_RECURSOS_TO_ORDENS_URL must be provided")
        if self.environment == "prod" and self.sqs_endpoint_url is not None:
            raise ValueError("SQS_ENDPOINT_URL must be omitted in production")
        values = {
            "SQS_CONNECT_TIMEOUT_SECONDS": self.sqs_connect_timeout_seconds,
            "SQS_READ_TIMEOUT_SECONDS": self.sqs_read_timeout_seconds,
            "SQS_SEND_TIMEOUT_SECONDS": self.sqs_send_timeout_seconds,
            "PUBLISHER_LEASE_SECONDS": self.publisher_lease_seconds,
            "PUBLISHER_POLL_INTERVAL_SECONDS": self.publisher_poll_interval_seconds,
            "PUBLISHER_INITIAL_BACKOFF_SECONDS": self.publisher_initial_backoff_seconds,
            "PUBLISHER_MAX_BACKOFF_SECONDS": self.publisher_max_backoff_seconds,
        }
        invalid = [name for name, value in values.items() if value <= 0]
        if invalid:
            raise ValueError(f"publisher settings must be positive: {', '.join(invalid)}")
        if self.publisher_batch_size <= 0 or self.publisher_max_attempts <= 0:
            raise ValueError("publisher batch size and max attempts must be positive")
        if self.publisher_max_backoff_seconds < self.publisher_initial_backoff_seconds:
            raise ValueError("publisher max backoff must be >= initial backoff")
        if self.publisher_lease_seconds <= self.sqs_send_timeout_seconds:
            raise ValueError("publisher lease must exceed the SQS send timeout")


@lru_cache
def get_settings() -> Settings:
    return Settings()
