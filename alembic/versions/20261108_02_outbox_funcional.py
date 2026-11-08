"""Cria a Outbox funcional local de Recursos."""

import sqlalchemy as sa

from alembic import op

revision = "20261108_02_outbox"
down_revision = "20261107_01_recursos"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "outbox_message",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("aggregate_type", sa.String(80), nullable=False),
        sa.Column("aggregate_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(120), nullable=False),
        sa.Column("event_version", sa.Integer(), nullable=False),
        sa.Column("payload", sa.Text(), nullable=False),
        sa.Column("correlation_id", sa.Uuid(), nullable=False),
        sa.Column("causation_id", sa.Uuid()),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("claim_id", sa.Uuid()),
        sa.Column("claimed_by", sa.String(120)),
        sa.Column("claimed_at", sa.DateTime(timezone=True)),
        sa.Column("claim_expires_at", sa.DateTime(timezone=True)),
        sa.Column("last_error", sa.Text()),
        sa.Column("last_failed_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint(
            "event_version > 0", name="ck_outbox_event_version_positiva"
        ),
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
    op.create_index("ix_outbox_pending", "outbox_message", ["published_at"])
    op.create_index(
        "ix_outbox_claimable",
        "outbox_message",
        ["next_attempt_at", "claim_expires_at", "occurred_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_table("outbox_message")
