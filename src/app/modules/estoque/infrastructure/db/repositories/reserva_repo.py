from uuid import UUID

from sqlmodel import select

from app.modules.estoque.domain.entities.reserva import Reserva, StatusReserva
from app.modules.estoque.infrastructure.db.models.reserva import ReservaModel
from app.shared.infra.db import DBSession


class ReservaRepo:
    """Persistencia de reservas sem controlar a transacao do chamador."""

    def __init__(self, session: DBSession) -> None:
        self.session = session

    async def salvar(self, reserva: Reserva) -> None:
        self.session.add(self._to_model(reserva))

    async def atualizar(self, reserva: Reserva) -> None:
        model = await self.session.get(ReservaModel, reserva.operation_id)
        if model is None:
            raise LookupError("reserva nao encontrada durante atualizacao")
        model.ordem_servico_id = reserva.ordem_servico_id
        model.ordem_servico_item_id = reserva.ordem_servico_item_id
        model.item_estoque_id = reserva.item_estoque_id
        model.quantidade = reserva.quantidade
        model.status = reserva.status.value
        model.atualizado_em = reserva.atualizado_em
        self.session.add(model)

    async def obter_por_id_com_lock(self, reservation_id: UUID) -> Reserva | None:
        result = await self.session.exec(
            select(ReservaModel)
            .where(ReservaModel.operation_id == reservation_id)
            .with_for_update()
        )
        model = result.first()
        return self._to_entity(model) if model else None

    async def obter_por_item_da_ordem_com_lock(
        self,
        *,
        ordem_servico_id: UUID,
        ordem_servico_item_id: UUID,
        item_estoque_id: UUID,
    ) -> Reserva | None:
        result = await self.session.exec(
            select(ReservaModel)
            .where(
                ReservaModel.ordem_servico_id == ordem_servico_id,
                ReservaModel.ordem_servico_item_id == ordem_servico_item_id,
                ReservaModel.item_estoque_id == item_estoque_id,
            )
            .with_for_update()
        )
        model = result.first()
        return self._to_entity(model) if model else None

    @staticmethod
    def _to_model(reserva: Reserva) -> ReservaModel:
        return ReservaModel(
            operation_id=reserva.operation_id,
            ordem_servico_id=reserva.ordem_servico_id,
            ordem_servico_item_id=reserva.ordem_servico_item_id,
            item_estoque_id=reserva.item_estoque_id,
            quantidade=reserva.quantidade,
            status=reserva.status.value,
            criado_em=reserva.criado_em,
            atualizado_em=reserva.atualizado_em,
        )

    @staticmethod
    def _to_entity(model: ReservaModel) -> Reserva:
        return Reserva(
            operation_id=model.operation_id,
            ordem_servico_id=model.ordem_servico_id,
            ordem_servico_item_id=model.ordem_servico_item_id,
            item_estoque_id=model.item_estoque_id,
            quantidade=model.quantidade,
            status=StatusReserva(model.status),
            criado_em=model.criado_em,
            atualizado_em=model.atualizado_em,
        )
