"""Baseline independente de catálogo, estoque e reservas."""

import sqlalchemy as sa

from alembic import op

revision = "20261107_01_recursos"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "catalogo_servico",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("nome", sa.String(100), nullable=False, unique=True),
        sa.Column("descricao", sa.String(255)),
        sa.Column("valor_base", sa.Numeric(10, 2), nullable=False),
        sa.Column("tempo_medio_minutos", sa.Integer(), nullable=False),
        sa.Column("ativo", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("criado_em", sa.DateTime(timezone=True), nullable=False),
        sa.Column("atualizado_em", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "valor_base >= 0", name="ck_servico_valor_nao_negativo"
        ),
        sa.CheckConstraint(
            "tempo_medio_minutos > 0", name="ck_servico_tempo_medio_positivo"
        ),
    )
    op.create_table(
        "item_estoque",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("tipo", sa.String(10), nullable=False),
        sa.Column("nome", sa.String(100), nullable=False),
        sa.Column("descricao", sa.String(255)),
        sa.Column("codigo", sa.String(50), unique=True),
        sa.Column("quantidade_disponivel", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("quantidade_reservada", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("quantidade_minima", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("valor_unitario", sa.Numeric(10, 2), nullable=False),
        sa.Column("ativo", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("criado_em", sa.DateTime(timezone=True), nullable=False),
        sa.Column("atualizado_em", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("quantidade_disponivel >= 0", name="ck_item_saldo_disponivel"),
        sa.CheckConstraint("quantidade_reservada >= 0", name="ck_item_saldo_reservado"),
        sa.CheckConstraint("quantidade_minima >= 0", name="ck_item_quantidade_minima"),
        sa.CheckConstraint("valor_unitario >= 0", name="ck_item_valor_nao_negativo"),
    )
    op.create_table(
        "reserva_estoque",
        sa.Column("operation_id", sa.Uuid(), primary_key=True),
        sa.Column("ordem_servico_id", sa.Uuid(), nullable=False),
        sa.Column(
            "item_estoque_id",
            sa.Uuid(),
            sa.ForeignKey("item_estoque.id"),
            nullable=False,
        ),
        sa.Column("quantidade", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("criado_em", sa.DateTime(timezone=True), nullable=False),
        sa.Column("atualizado_em", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("quantidade > 0", name="ck_reserva_quantidade_positiva"),
        sa.CheckConstraint(
            "status IN ('PENDENTE', 'RESERVADA', 'LIBERADA', 'CONSUMIDA', 'SEM_ESTOQUE')",
            name="ck_reserva_status_valido",
        ),
    )
    op.create_index("ix_reserva_ordem", "reserva_estoque", ["ordem_servico_id"])
    op.create_index("ix_reserva_item", "reserva_estoque", ["item_estoque_id"])


def downgrade() -> None:
    op.drop_table("reserva_estoque")
    op.drop_table("item_estoque")
    op.drop_table("catalogo_servico")
