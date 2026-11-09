"""Persistencia concorrente da Outbox sobre PostgreSQL."""

import json
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import or_, update
from sqlmodel import select

from app.shared.infra.db import DBSession
from app.shared.infra.db.outbox_model import OutboxMessageModel
from app.shared.outbox import (
    ClaimedOutboxMessage,
    OutboxClaimConflictError,
    OutboxMessage,
)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _require_aware(value: datetime, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} deve possuir timezone")


class PostgresOutboxRepository:
    """Registra e reivindica mensagens sem controlar o commit do chamador."""

    def __init__(self, session: DBSession) -> None:
        self.session = session

    async def add(self, message: OutboxMessage) -> None:
        if not message.aggregate_type.strip():
            raise ValueError("aggregate_type nao pode ser vazio")
        if not message.event_type.strip():
            raise ValueError("event_type nao pode ser vazio")
        if message.event_version <= 0:
            raise ValueError("event_version deve ser positivo")
        _require_aware(message.occurred_at, "occurred_at")
        try:
            payload = json.dumps(
                message.payload,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("payload deve ser serializavel como JSON") from exc

        self.session.add(
            OutboxMessageModel(
                id=message.message_id,
                aggregate_type=message.aggregate_type,
                aggregate_id=message.aggregate_id,
                event_type=message.event_type,
                event_version=message.event_version,
                payload=payload,
                correlation_id=message.correlation_id,
                causation_id=message.causation_id,
                saga_id=message.saga_id,
                occurred_at=message.occurred_at,
                published_at=None,
                attempts=0,
                next_attempt_at=message.occurred_at,
            )
        )

    async def claim_pending(
        self,
        *,
        worker_id: str,
        batch_size: int,
        lease_duration: timedelta,
        max_attempts: int | None = None,
        now: datetime | None = None,
    ) -> list[ClaimedOutboxMessage]:
        if not worker_id.strip():
            raise ValueError("worker_id nao pode ser vazio")
        if batch_size <= 0:
            raise ValueError("batch_size deve ser positivo")
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration deve ser positivo")
        if max_attempts is not None and max_attempts <= 0:
            raise ValueError("max_attempts deve ser positivo")
        claimed_at = now or _utc_now()
        _require_aware(claimed_at, "now")
        claim_expires_at = claimed_at + lease_duration

        conditions = [
            OutboxMessageModel.published_at.is_(None),
            OutboxMessageModel.next_attempt_at <= claimed_at,
            or_(
                OutboxMessageModel.claim_id.is_(None),
                OutboxMessageModel.claim_expires_at <= claimed_at,
            ),
        ]
        if max_attempts is not None:
            conditions.append(OutboxMessageModel.attempts < max_attempts)

        statement = (
            select(OutboxMessageModel)
            .where(*conditions)
            .order_by(
                OutboxMessageModel.next_attempt_at,
                OutboxMessageModel.occurred_at,
                OutboxMessageModel.id,
            )
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
        rows = list((await self.session.exec(statement)).all())
        claimed: list[ClaimedOutboxMessage] = []
        for row in rows:
            claim_id = uuid4()
            row.claim_id = claim_id
            row.claimed_by = worker_id
            row.claimed_at = claimed_at
            row.claim_expires_at = claim_expires_at
            row.attempts += 1
            claimed.append(self._to_claimed(row))
        await self.session.flush()
        return claimed

    async def mark_published(
        self,
        *,
        message_id: UUID,
        claim_id: UUID,
        published_at: datetime | None = None,
    ) -> None:
        publication_time = published_at or _utc_now()
        _require_aware(publication_time, "published_at")
        statement = (
            update(OutboxMessageModel)
            .where(
                OutboxMessageModel.id == message_id,
                OutboxMessageModel.claim_id == claim_id,
                OutboxMessageModel.published_at.is_(None),
                OutboxMessageModel.claim_expires_at > publication_time,
            )
            .values(
                published_at=publication_time,
                claim_id=None,
                claimed_by=None,
                claimed_at=None,
                claim_expires_at=None,
            )
        )
        result = await self.session.exec(statement)
        if result.rowcount != 1:
            raise OutboxClaimConflictError("claim da mensagem nao esta mais ativo")

    async def mark_failed(
        self,
        *,
        message_id: UUID,
        claim_id: UUID,
        error: str,
        next_attempt_at: datetime,
        failed_at: datetime | None = None,
    ) -> None:
        failure_time = failed_at or _utc_now()
        _require_aware(failure_time, "failed_at")
        _require_aware(next_attempt_at, "next_attempt_at")
        if not error.strip():
            raise ValueError("error nao pode ser vazio")
        if next_attempt_at < failure_time:
            raise ValueError("next_attempt_at nao pode anteceder failed_at")
        statement = (
            update(OutboxMessageModel)
            .where(
                OutboxMessageModel.id == message_id,
                OutboxMessageModel.claim_id == claim_id,
                OutboxMessageModel.published_at.is_(None),
                OutboxMessageModel.claim_expires_at > failure_time,
            )
            .values(
                next_attempt_at=next_attempt_at,
                claim_id=None,
                claimed_by=None,
                claimed_at=None,
                claim_expires_at=None,
                last_error=error,
                last_failed_at=failure_time,
            )
        )
        result = await self.session.exec(statement)
        if result.rowcount != 1:
            raise OutboxClaimConflictError("claim da mensagem nao esta mais ativo")

    @staticmethod
    def _to_claimed(row: OutboxMessageModel) -> ClaimedOutboxMessage:
        assert row.claim_id is not None
        assert row.claimed_by is not None
        assert row.claimed_at is not None
        assert row.claim_expires_at is not None
        return ClaimedOutboxMessage(
            message_id=row.id,
            aggregate_type=row.aggregate_type,
            aggregate_id=row.aggregate_id,
            event_type=row.event_type,
            event_version=row.event_version,
            payload=json.loads(row.payload),
            correlation_id=row.correlation_id,
            causation_id=row.causation_id,
            saga_id=row.saga_id,
            occurred_at=row.occurred_at,
            attempts=row.attempts,
            claim_id=row.claim_id,
            claimed_by=row.claimed_by,
            claimed_at=row.claimed_at,
            claim_expires_at=row.claim_expires_at,
        )
