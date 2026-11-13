"""Preserva saga_id no envelope publicavel da Outbox.

Revision ID: 20261108_03_outbox_saga
Revises: 20261108_02_outbox
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20261108_03_outbox_saga"
down_revision: str | None = "20261108_02_outbox"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("outbox_message", sa.Column("saga_id", sa.Uuid(), nullable=True))


def downgrade() -> None:
    op.drop_column("outbox_message", "saga_id")
