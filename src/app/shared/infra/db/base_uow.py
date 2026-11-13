from typing import Protocol

from app.shared.infra.db import DBSession
from app.shared.infra.db.outbox_repository import PostgresOutboxRepository
from app.shared.outbox import OutboxRepository


class UnitOfWork(Protocol):
    """Protocolo do padrão Unit of Work.

    Garante que todas as operações dentro de uma unidade sejam confirmadas
    ou revertidas atomicamente.
    """

    outbox: OutboxRepository

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            await self.commit()
        else:
            await self.rollback()

    async def commit(self): ...

    async def rollback(self): ...


class BaseUnitOfWork(UnitOfWork):
    def __init__(self, session: DBSession) -> None:
        self.session = session
        self.outbox = PostgresOutboxRepository(session)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                try:
                    await self.commit()
                except BaseException:
                    await self.rollback()
                    raise
            else:
                await self.rollback()
        finally:
            await self.session.close()

    async def commit(self):
        await self.session.commit()

    async def rollback(self):
        await self.session.rollback()
