"""Cria a Inbox persistente e a deduplicacao de efeitos de Recursos."""

import sqlalchemy as sa
from alembic import op

revision = "20261109_04_inbox"
down_revision = "20261108_03_outbox_saga"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "inbox_effect",
        sa.Column("consumer_name", sa.String(120), primary_key=True),
        sa.Column("business_key", sa.String(220), primary_key=True),
        sa.Column("canonical_message_id", sa.Uuid(), nullable=False),
        sa.Column("payload_hash", sa.String(64), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("status IN ('PROCESSING', 'PROCESSED')", name="ck_inbox_effect_status"),
        sa.CheckConstraint(
            "(status = 'PROCESSING' AND processed_at IS NULL) OR "
            "(status = 'PROCESSED' AND processed_at IS NOT NULL)",
            name="ck_inbox_effect_processing",
        ),
    )
    op.create_table(
        "inbox_message",
        sa.Column("consumer_name", sa.String(120), primary_key=True),
        sa.Column("message_id", sa.Uuid(), primary_key=True),
        sa.Column("business_key", sa.String(220), nullable=False),
        sa.Column("payload_hash", sa.String(64), nullable=False),
        sa.Column("message_type", sa.String(120), nullable=False),
        sa.Column("message_version", sa.Integer(), nullable=False),
        sa.Column("operation_id", sa.Uuid(), nullable=False),
        sa.Column("saga_id", sa.Uuid()),
        sa.Column("correlation_id", sa.Uuid(), nullable=False),
        sa.Column("causation_id", sa.Uuid()),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("transport_receive_count", sa.Integer(), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("message_version > 0", name="ck_inbox_message_version"),
        sa.CheckConstraint("transport_receive_count > 0", name="ck_inbox_receive_count_positivo"),
        sa.CheckConstraint(
            "status IN ('PROCESSING', 'PROCESSED', 'DUPLICATE', 'CONFLICT')",
            name="ck_inbox_message_status",
        ),
        sa.CheckConstraint(
            "(status = 'PROCESSING' AND processed_at IS NULL) OR "
            "(status <> 'PROCESSING' AND processed_at IS NOT NULL)",
            name="ck_inbox_message_processing",
        ),
    )
    op.create_index(
        "ix_inbox_message_business_key",
        "inbox_message",
        ["consumer_name", "business_key"],
    )
    op.create_index("ix_inbox_message_operation_id", "inbox_message", ["operation_id"])


def downgrade() -> None:
    op.drop_table("inbox_message")
    op.drop_table("inbox_effect")
