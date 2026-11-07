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
