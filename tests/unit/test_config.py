import pytest

from app.config import Settings


def test_runtime_requires_database_url() -> None:
    settings = Settings(environment="test", database_url="")

    with pytest.raises(ValueError, match="DATABASE_URL"):
        settings.validate_runtime()


def test_production_rejects_unsafe_jwt_secret() -> None:
    settings = Settings(
        environment="prod",
        database_url="postgresql+asyncpg://db/recursos",
        jwt_secret_key="unsafe",
    )

    with pytest.raises(ValueError, match="JWT_SECRET_KEY"):
        settings.validate_runtime()


def test_publisher_valida_fila_lease_e_endpoint_de_producao() -> None:
    missing = Settings(
        environment="test",
        database_url="postgresql+asyncpg://db/recursos",
        sqs_recursos_to_ordens_url="",
    )
    with pytest.raises(ValueError, match="SQS_RECURSOS_TO_ORDENS_URL"):
        missing.validate_publisher_runtime()

    short = Settings(
        environment="test",
        database_url="postgresql+asyncpg://db/recursos",
        sqs_recursos_to_ordens_url="queue.fifo",
        publisher_lease_seconds=5,
        sqs_send_timeout_seconds=5,
    )
    with pytest.raises(ValueError, match="lease"):
        short.validate_publisher_runtime()

    production = Settings(
        environment="prod",
        database_url="postgresql+asyncpg://db/recursos",
        jwt_secret_key="seguro",
        sqs_endpoint_url="http://elasticmq:9324",
        sqs_recursos_to_ordens_url="https://sqs/queue.fifo",
    )
    with pytest.raises(ValueError, match="SQS_ENDPOINT_URL"):
        production.validate_publisher_runtime()
