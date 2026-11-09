"""Inbox PostgreSQL transacional e deduplicacao de efeitos de Recursos."""

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlmodel import Field, SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

from app.application.inbox_consumer import ProcessingOutcome


class InboxMessageModel(SQLModel, table=True):
    __tablename__ = "inbox_message"

    consumer_name: str = Field(primary_key=True, max_length=120)
    message_id: UUID = Field(primary_key=True)
    business_key: str = Field(max_length=220)
    payload_hash: str = Field(max_length=64)
    message_type: str = Field(max_length=120)
    message_version: int
    operation_id: UUID = Field(index=True)
    saga_id: UUID | None = None
    correlation_id: UUID
    causation_id: UUID | None = None
    status: str = Field(max_length=20)
    transport_receive_count: int
    received_at: datetime = Field(
        sa_column=sa.Column(sa.DateTime(timezone=True), nullable=False)
    )
    processed_at: datetime | None = Field(
        default=None, sa_column=sa.Column(sa.DateTime(timezone=True))
    )


class InboxEffectModel(SQLModel, table=True):
    __tablename__ = "inbox_effect"

    consumer_name: str = Field(primary_key=True, max_length=120)
    business_key: str = Field(primary_key=True, max_length=220)
    canonical_message_id: UUID
    payload_hash: str = Field(max_length=64)
    status: str = Field(max_length=20)
    created_at: datetime = Field(
        sa_column=sa.Column(sa.DateTime(timezone=True), nullable=False)
    )
    processed_at: datetime | None = Field(
        default=None, sa_column=sa.Column(sa.DateTime(timezone=True))
    )


class TransactionalMessageHandler(Protocol):
    async def handle(
        self, envelope: Mapping[str, object], session: AsyncSession
    ) -> None: ...


class RecursosBusinessKeyResolver:
    _PREFIXES = {
        "ReserveItem": "reserve",
        "ReleaseReservation": "release",
        "ReceiveBackorderedItem": "receive-backordered",
        "ConsumeReservation": "consume",
    }

    def resolve(self, envelope: Mapping[str, object]) -> str:
        message_type = str(envelope["message_type"])
        try:
            prefix = self._PREFIXES[message_type]
        except KeyError as exc:
            raise ValueError(f"tipo sem chave de idempotencia: {message_type}") from exc
        return f"{prefix}:{envelope['operation_id']}"


def semantic_payload_hash(envelope: Mapping[str, object]) -> str:
    semantic = {
        "message_type": envelope["message_type"],
        "message_version": envelope["message_version"],
        "operation_id": envelope["operation_id"],
        "saga_id": envelope["saga_id"],
        "payload": envelope["payload"],
    }
    encoded = json.dumps(
        semantic, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


class PostgresInboxProcessor:
    """Executa handler, efeito de idempotencia e Inbox na mesma transacao."""

    def __init__(
        self,
        session_factory: async_sessionmaker,
        *,
        consumer_name: str,
        handler: TransactionalMessageHandler,
        key_resolver: RecursosBusinessKeyResolver | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.consumer_name = consumer_name
        self.handler = handler
        self.key_resolver = key_resolver or RecursosBusinessKeyResolver()

    async def process(
        self, envelope: Mapping[str, object], *, receive_count: int
    ) -> ProcessingOutcome:
        now = datetime.now(UTC)
        message_id = UUID(str(envelope["message_id"]))
        business_key = self.key_resolver.resolve(envelope)
        payload_hash = semantic_payload_hash(envelope)
        async with self.session_factory() as session:
            async with session.begin():
                await session.exec(
                    insert(InboxMessageModel)
                    .values(
                        consumer_name=self.consumer_name,
                        message_id=message_id,
                        business_key=business_key,
                        payload_hash=payload_hash,
                        message_type=str(envelope["message_type"]),
                        message_version=int(envelope["message_version"]),
                        operation_id=UUID(str(envelope["operation_id"])),
                        saga_id=(UUID(str(envelope["saga_id"])) if envelope["saga_id"] else None),
                        correlation_id=UUID(str(envelope["correlation_id"])),
                        causation_id=(
                            UUID(str(envelope["causation_id"]))
                            if envelope["causation_id"]
                            else None
                        ),
                        status="PROCESSING",
                        transport_receive_count=receive_count,
                        received_at=now,
                    )
                    .on_conflict_do_nothing(index_elements=["consumer_name", "message_id"])
                )
                message = (
                    await session.exec(
                        sa.select(InboxMessageModel)
                        .where(
                            InboxMessageModel.consumer_name == self.consumer_name,
                            InboxMessageModel.message_id == message_id,
                        )
                        .with_for_update()
                    )
                ).scalar_one()
                if message.payload_hash != payload_hash or message.status == "CONFLICT":
                    return ProcessingOutcome.CONFLICT
                if message.status in {"PROCESSED", "DUPLICATE"}:
                    return ProcessingOutcome.DUPLICATE
                await session.exec(
                    insert(InboxEffectModel)
                    .values(
                        consumer_name=self.consumer_name,
                        business_key=business_key,
                        canonical_message_id=message_id,
                        payload_hash=payload_hash,
                        status="PROCESSING",
                        created_at=now,
                    )
                    .on_conflict_do_nothing(index_elements=["consumer_name", "business_key"])
                )
                effect = (
                    await session.exec(
                        sa.select(InboxEffectModel)
                        .where(
                            InboxEffectModel.consumer_name == self.consumer_name,
                            InboxEffectModel.business_key == business_key,
                        )
                        .with_for_update()
                    )
                ).scalar_one()
                if effect.payload_hash != payload_hash:
                    message.status = "CONFLICT"
                    message.processed_at = now
                    session.add(message)
                    return ProcessingOutcome.CONFLICT
                if effect.status == "PROCESSED":
                    message.status = "DUPLICATE"
                    message.processed_at = now
                    session.add(message)
                    return ProcessingOutcome.DUPLICATE
                await self.handler.handle(envelope, session)
                effect.status = "PROCESSED"
                effect.processed_at = now
                message.status = "PROCESSED"
                message.processed_at = now
                session.add(effect)
                session.add(message)
                return ProcessingOutcome.PROCESSED
