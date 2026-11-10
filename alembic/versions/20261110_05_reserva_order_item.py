"""Identifica de forma duravel o item de Ordem associado a reserva."""

import sqlalchemy as sa
from alembic import op

revision = "20261110_05_reserva_order_item"
down_revision = "20261109_04_inbox"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "reserva_estoque",
        sa.Column("ordem_servico_item_id", sa.Uuid(), nullable=True),
    )
    op.execute(
        "UPDATE reserva_estoque "
        "SET ordem_servico_item_id = operation_id "
        "WHERE ordem_servico_item_id IS NULL"
    )
    op.alter_column("reserva_estoque", "ordem_servico_item_id", nullable=False)
    op.create_unique_constraint(
        "uq_reserva_ordem_servico_item",
        "reserva_estoque",
        ["ordem_servico_item_id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_reserva_ordem_servico_item",
        "reserva_estoque",
        type_="unique",
    )
    op.drop_column("reserva_estoque", "ordem_servico_item_id")
