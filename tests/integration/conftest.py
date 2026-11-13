"""Fixtures para testes de integração contra PostgreSQL real."""

import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
import pytest_asyncio
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

from alembic import command

DATABASE_URL = os.environ["DATABASE_URL"]
PROJECT_ROOT = Path(__file__).resolve().parents[2]
TABLES = (
    "inbox_message",
    "inbox_effect",
    "outbox_message",
    "reserva_estoque",
    "item_estoque",
    "catalogo_servico",
)


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="session", autouse=True)
def migrated_database() -> Iterator[Config]:
    """Aplica o histórico desde a base e remove tudo ao encerrar a suíte."""

    config = Config(str(PROJECT_ROOT / "alembic.ini"))
    command.downgrade(config, "base")
    command.upgrade(config, "head")
    yield config
    command.downgrade(config, "base")


async def _truncate(engine: AsyncEngine) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            text(f"TRUNCATE TABLE {', '.join(TABLES)} RESTART IDENTITY CASCADE")
        )


@pytest_asyncio.fixture
async def db_engine(migrated_database: Config) -> AsyncIterator[AsyncEngine]:
    """Fornece um engine por teste e garante isolamento antes e depois."""

    engine = create_async_engine(DATABASE_URL, echo=False)
    await _truncate(engine)
    yield engine
    await _truncate(engine)
    await engine.dispose()


@pytest_asyncio.fixture
async def db_session(db_engine: AsyncEngine) -> AsyncIterator[AsyncSession]:
    factory = async_sessionmaker(
        db_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    async with factory() as session:
        yield session
