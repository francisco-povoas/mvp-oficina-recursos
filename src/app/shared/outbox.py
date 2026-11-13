"""Contrato local da Outbox transacional de Recursos."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Mapping, Protocol
from uuid import UUID


@dataclass(frozen=True, slots=True)
class OutboxMessage:
    """Mensagem a registrar na mesma transacao da mudanca de negocio."""

    message_id: UUID
    aggregate_type: str
    aggregate_id: UUID
    event_type: str
    event_version: int
    payload: Mapping[str, object]
    correlation_id: UUID
    occurred_at: datetime
    causation_id: UUID | None = None
    saga_id: UUID | None = None


@dataclass(frozen=True, slots=True)
class ClaimedOutboxMessage:
    """Mensagem reivindicada por um futuro publisher durante um lease."""

    message_id: UUID
    aggregate_type: str
    aggregate_id: UUID
    event_type: str
    event_version: int
    payload: dict[str, object]
    correlation_id: UUID
    causation_id: UUID | None
    saga_id: UUID | None
    occurred_at: datetime
    attempts: int
    claim_id: UUID
    claimed_by: str
    claimed_at: datetime
    claim_expires_at: datetime


class OutboxClaimConflictError(RuntimeError):
    """O lease informado nao e mais o lease ativo da mensagem."""


class OutboxRepository(Protocol):
    async def add(self, message: OutboxMessage) -> None: ...

    async def claim_pending(
        self,
        *,
        worker_id: str,
        batch_size: int,
        lease_duration: timedelta,
        max_attempts: int | None = None,
        now: datetime | None = None,
    ) -> list[ClaimedOutboxMessage]: ...

    async def mark_published(
        self,
        *,
        message_id: UUID,
        claim_id: UUID,
        published_at: datetime | None = None,
    ) -> None: ...

    async def mark_failed(
        self,
        *,
        message_id: UUID,
        claim_id: UUID,
        error: str,
        next_attempt_at: datetime,
        failed_at: datetime | None = None,
    ) -> None: ...
