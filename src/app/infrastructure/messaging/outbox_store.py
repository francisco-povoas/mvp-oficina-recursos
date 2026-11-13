"""Adapter transacional do publisher sobre a Outbox PostgreSQL existente."""

from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlmodel.ext.asyncio.session import AsyncSession

from app.shared.infra.db.outbox_repository import PostgresOutboxRepository
from app.shared.outbox import ClaimedOutboxMessage


class PostgresPublisherOutboxStore:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory

    async def claim_pending(
        self,
        *,
        worker_id: str,
        batch_size: int,
        lease_duration: timedelta,
        max_attempts: int,
        now: datetime,
    ) -> list[ClaimedOutboxMessage]:
        async with self.session_factory() as session:
            claimed = await PostgresOutboxRepository(session).claim_pending(
                worker_id=worker_id,
                batch_size=batch_size,
                lease_duration=lease_duration,
                max_attempts=max_attempts,
                now=now,
            )
            await session.commit()
            return claimed

    async def mark_published(
        self,
        *,
        message_id: UUID,
        claim_id: UUID,
        published_at: datetime,
    ) -> None:
        async with self.session_factory() as session:
            await PostgresOutboxRepository(session).mark_published(
                message_id=message_id,
                claim_id=claim_id,
                published_at=published_at,
            )
            await session.commit()

    async def mark_failed(
        self,
        *,
        message_id: UUID,
        claim_id: UUID,
        error: str,
        next_attempt_at: datetime,
        failed_at: datetime,
    ) -> None:
        async with self.session_factory() as session:
            await PostgresOutboxRepository(session).mark_failed(
                message_id=message_id,
                claim_id=claim_id,
                error=error,
                next_attempt_at=next_attempt_at,
                failed_at=failed_at,
            )
            await session.commit()
