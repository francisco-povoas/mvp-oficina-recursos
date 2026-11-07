from datetime import datetime
from uuid import UUID

import sqlalchemy as sa
from sqlmodel import Field, SQLModel


class ReservaModel(SQLModel, table=True):
    __tablename__ = "reserva_estoque"
    __table_args__ = (
        sa.CheckConstraint("quantidade > 0", name="ck_reserva_quantidade_positiva"),
        sa.CheckConstraint(
            "status IN ('PENDENTE', 'RESERVADA', 'LIBERADA', 'CONSUMIDA', 'SEM_ESTOQUE')",
            name="ck_reserva_status_valido",
        ),
    )

    operation_id: UUID = Field(primary_key=True)
    ordem_servico_id: UUID = Field(index=True, nullable=False)
    item_estoque_id: UUID = Field(foreign_key="item_estoque.id", index=True, nullable=False)
    quantidade: int = Field(sa_column=sa.Column(sa.Integer, nullable=False))
    status: str = Field(sa_column=sa.Column(sa.String(20), nullable=False))
    criado_em: datetime = Field(sa_column=sa.Column(sa.DateTime(timezone=True), nullable=False))
    atualizado_em: datetime = Field(
        sa_column=sa.Column(sa.DateTime(timezone=True), nullable=False)
    )
