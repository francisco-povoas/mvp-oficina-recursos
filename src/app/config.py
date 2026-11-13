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
    sqs_ordens_to_recursos_url: str = ""
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
    consumer_batch_size: int = 10
    consumer_concurrency: int = 4
    consumer_wait_time_seconds: int = 20
    consumer_visibility_timeout_seconds: int = 60
    consumer_heartbeat_seconds: float = 20.0
    consumer_contract_schema_path: str = (
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

    def validate_consumer_runtime(self) -> None:
        self.validate_runtime()
        if not self.sqs_ordens_to_recursos_url:
            raise ValueError("SQS_ORDENS_TO_RECURSOS_URL must be provided")
        if self.environment == "prod" and self.sqs_endpoint_url is not None:
            raise ValueError("SQS_ENDPOINT_URL must be omitted in production")
        if not 1 <= self.consumer_batch_size <= 10:
            raise ValueError("consumer batch size must be between 1 and 10")
        if self.consumer_concurrency <= 0:
            raise ValueError("consumer concurrency must be positive")
        if not 0 <= self.consumer_wait_time_seconds <= 20:
            raise ValueError("consumer wait time must be between 0 and 20")
        if self.consumer_visibility_timeout_seconds <= 0:
            raise ValueError("consumer visibility timeout must be positive")
        if not 0 < self.consumer_heartbeat_seconds < self.consumer_visibility_timeout_seconds:
            raise ValueError("consumer heartbeat must be positive and below visibility timeout")


@lru_cache
def get_settings() -> Settings:
    return Settings()
