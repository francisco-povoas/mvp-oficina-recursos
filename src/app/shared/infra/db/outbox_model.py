"""Modelo PostgreSQL da Outbox transacional de Recursos."""

from datetime import datetime
from uuid import UUID

import sqlalchemy as sa
from sqlmodel import Field, SQLModel


class OutboxMessageModel(SQLModel, table=True):
    __tablename__ = "outbox_message"
    __table_args__ = (
        sa.CheckConstraint("event_version > 0", name="ck_outbox_event_version_positiva"),
        sa.CheckConstraint("attempts >= 0", name="ck_outbox_attempts_nao_negativo"),
        sa.CheckConstraint(
            "(claim_id IS NULL AND claimed_by IS NULL AND claimed_at IS NULL "
            "AND claim_expires_at IS NULL) OR "
            "(claim_id IS NOT NULL AND claimed_by IS NOT NULL AND claimed_at IS NOT NULL "
            "AND claim_expires_at IS NOT NULL)",
            name="ck_outbox_claim_completo",
        ),
        sa.CheckConstraint(
            "claim_expires_at IS NULL OR claim_expires_at > claimed_at",
            name="ck_outbox_claim_expiracao_valida",
        ),
        sa.CheckConstraint(
            "last_failed_at IS NULL OR last_error IS NOT NULL",
            name="ck_outbox_falha_com_erro",
        ),
    )

    id: UUID = Field(primary_key=True)
    aggregate_type: str = Field(sa_column=sa.Column(sa.String(80), nullable=False))
    aggregate_id: UUID = Field(sa_column=sa.Column(sa.Uuid, nullable=False))
    event_type: str = Field(sa_column=sa.Column(sa.String(120), nullable=False))
    event_version: int = Field(sa_column=sa.Column(sa.Integer, nullable=False))
    payload: str = Field(sa_column=sa.Column(sa.Text, nullable=False))
    correlation_id: UUID = Field(sa_column=sa.Column(sa.Uuid, nullable=False))
    causation_id: UUID | None = Field(default=None, sa_column=sa.Column(sa.Uuid))
    occurred_at: datetime = Field(
        sa_column=sa.Column(sa.DateTime(timezone=True), nullable=False)
    )
    published_at: datetime | None = Field(
        default=None,
        sa_column=sa.Column(sa.DateTime(timezone=True), index=True),
    )
    attempts: int = Field(
        default=0,
        sa_column=sa.Column(sa.Integer, nullable=False, server_default="0"),
    )
    next_attempt_at: datetime = Field(
        sa_column=sa.Column(
            sa.DateTime(timezone=True),
            nullable=False,
            index=True,
            server_default=sa.func.now(),
        )
    )
    claim_id: UUID | None = Field(default=None, sa_column=sa.Column(sa.Uuid))
    claimed_by: str | None = Field(
        default=None,
        sa_column=sa.Column(sa.String(120)),
    )
    claimed_at: datetime | None = Field(
        default=None,
        sa_column=sa.Column(sa.DateTime(timezone=True)),
    )
    claim_expires_at: datetime | None = Field(
        default=None,
        sa_column=sa.Column(sa.DateTime(timezone=True), index=True),
    )
    last_error: str | None = Field(default=None, sa_column=sa.Column(sa.Text))
    last_failed_at: datetime | None = Field(
        default=None,
        sa_column=sa.Column(sa.DateTime(timezone=True)),
    )
