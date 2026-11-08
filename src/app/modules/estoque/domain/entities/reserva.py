from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID

from app.modules.estoque.domain.exceptions import ItemEstoqueInvalidoError


class StatusReserva(StrEnum):
    PENDENTE = "PENDENTE"
    RESERVADA = "RESERVADA"
    LIBERADA = "LIBERADA"
    CONSUMIDA = "CONSUMIDA"
    SEM_ESTOQUE = "SEM_ESTOQUE"


@dataclass(frozen=True, kw_only=True)
class Reserva:
    """Reserva idempotente pertencente a Recursos e identificada pela operação."""

    operation_id: UUID
    ordem_servico_id: UUID
    item_estoque_id: UUID
    quantidade: int
    status: StatusReserva
    criado_em: datetime
    atualizado_em: datetime

    def __post_init__(self) -> None:
        if self.quantidade <= 0:
            raise ItemEstoqueInvalidoError("A quantidade da reserva deve ser maior que zero.")

    def reservar(self, saldo_disponivel: int) -> "Reserva":
        if self.status != StatusReserva.PENDENTE:
            raise ItemEstoqueInvalidoError("Somente uma reserva pendente pode ser processada.")
        status = (
            StatusReserva.RESERVADA
            if saldo_disponivel >= self.quantidade
            else StatusReserva.SEM_ESTOQUE
        )
        return replace(self, status=status, atualizado_em=datetime.now(UTC))

    def liberar(self) -> "Reserva":
        if self.status != StatusReserva.RESERVADA:
            raise ItemEstoqueInvalidoError("Somente uma reserva confirmada pode ser liberada.")
        return replace(self, status=StatusReserva.LIBERADA, atualizado_em=datetime.now(UTC))

    def consumir(self) -> "Reserva":
        if self.status != StatusReserva.RESERVADA:
            raise ItemEstoqueInvalidoError("Somente uma reserva confirmada pode ser consumida.")
        return replace(self, status=StatusReserva.CONSUMIDA, atualizado_em=datetime.now(UTC))
