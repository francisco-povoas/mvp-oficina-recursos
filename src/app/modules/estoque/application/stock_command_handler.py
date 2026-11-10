"""Handlers transacionais dos comandos de estoque recebidos de Ordens."""

from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from sqlmodel.ext.asyncio.session import AsyncSession

from app.modules.estoque.domain.entities.reserva import Reserva, StatusReserva
from app.modules.estoque.infrastructure.db.repositories.item_estoque_repo import (
    ItemEstoqueRepo,
)
from app.modules.estoque.infrastructure.db.repositories.reserva_repo import ReservaRepo
from app.shared.infra.db.outbox_repository import PostgresOutboxRepository
from app.shared.outbox import OutboxMessage
from app.shared.value_objects.id import ID

MAX_QUANTITY = 2_147_483_647


class StockCommandError(RuntimeError):
    """Comando valido no transporte, mas inaplicavel ao estado persistido."""


class StockCommandHandler:
    """Despacha comandos usando exclusivamente a sessao transacional da Inbox."""

    def __init__(
        self,
        *,
        clock: Callable[[], datetime] | None = None,
        message_id_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self.clock = clock or (lambda: datetime.now(UTC))
        self.message_id_factory = message_id_factory

    async def handle(
        self, envelope: Mapping[str, object], session: AsyncSession
    ) -> None:
        handlers = {
            "ReserveItem": self._reserve_item,
            "ReleaseReservation": self._release_reservation,
        }
        message_type = str(envelope["message_type"])
        handler = handlers.get(message_type)
        if handler is None:
            raise StockCommandError(f"handler funcional ausente para {message_type}")
        await handler(envelope, session)

    async def _reserve_item(
        self, envelope: Mapping[str, object], session: AsyncSession
    ) -> None:
        payload = self._payload(envelope)
        operation_id = self._uuid(envelope, "operation_id")
        resource_id = self._uuid(payload, "resource_id")
        quantity = self._quantity(payload)
        now = self.clock()
        outbox = PostgresOutboxRepository(session)

        if quantity < 1 or quantity > MAX_QUANTITY:
            await self._add_result(
                envelope,
                outbox,
                event_type="ReservationRejected.v1",
                payload={
                    "resource_id": str(resource_id),
                    "rejection_code": "INVALID_QUANTITY",
                    "detail": "Quantidade fora do intervalo suportado",
                    "rejected_at": self._timestamp(now),
                },
                occurred_at=now,
            )
            return

        item_repo = ItemEstoqueRepo(session)
        item = await item_repo.obter_por_id_com_lock(ID(resource_id))
        if item is None:
            await self._add_result(
                envelope,
                outbox,
                event_type="ReservationRejected.v1",
                payload={
                    "resource_id": str(resource_id),
                    "rejection_code": "RESOURCE_NOT_FOUND",
                    "detail": "Recurso nao encontrado",
                    "rejected_at": self._timestamp(now),
                },
                occurred_at=now,
            )
            return

        reservation = Reserva(
            operation_id=operation_id,
            ordem_servico_id=self._uuid(payload, "order_id"),
            ordem_servico_item_id=self._uuid(payload, "order_item_id"),
            item_estoque_id=resource_id,
            quantidade=quantity,
            status=StatusReserva.PENDENTE,
            criado_em=now,
            atualizado_em=now,
        ).reservar(item.quantidade_disponivel)
        await ReservaRepo(session).salvar(reservation)
        snapshot = {
            "name": item.nome,
            "description": item.descricao,
            "unit_price": self._money(item.valor_unitario),
        }

        if reservation.status is StatusReserva.RESERVADA:
            updated = replace(
                item,
                quantidade_disponivel=item.quantidade_disponivel - quantity,
                quantidade_reservada=item.quantidade_reservada + quantity,
                atualizado_em=now,
            )
            await item_repo.atualizar(updated)
            event_type = "ResourceReserved.v1"
            result_payload: Mapping[str, object] = {
                "reservation_id": str(reservation.operation_id),
                "resource_id": str(resource_id),
                "quantity": quantity,
                "reserved_at": self._timestamp(now),
                "resource_snapshot": snapshot,
            }
        else:
            event_type = "ItemBackordered.v1"
            result_payload = {
                "resource_id": str(resource_id),
                "requested_quantity": quantity,
                "available_quantity": item.quantidade_disponivel,
                "decided_at": self._timestamp(now),
                "resource_snapshot": snapshot,
            }

        await self._add_result(
            envelope,
            outbox,
            event_type=event_type,
            payload=result_payload,
            occurred_at=now,
        )

    async def _release_reservation(
        self, envelope: Mapping[str, object], session: AsyncSession
    ) -> None:
        payload = self._payload(envelope)
        operation_id = self._uuid(envelope, "operation_id")
        raw_reservation_id = payload.get("reservation_id")
        reservation_id = (
            UUID(str(raw_reservation_id)) if raw_reservation_id else operation_id
        )
        resource_id = self._uuid(payload, "resource_id")
        quantity = self._quantity(payload)
        reservation_repo = ReservaRepo(session)
        reservation = await reservation_repo.obter_por_id_com_lock(reservation_id)
        if reservation is None:
            raise StockCommandError("reserva a liberar nao encontrada")
        self._require_matching_reservation(
            reservation,
            resource_id=resource_id,
            quantity=quantity,
        )

        now = self.clock()
        if reservation.status is StatusReserva.RESERVADA:
            item_repo = ItemEstoqueRepo(session)
            item = await item_repo.obter_por_id_com_lock(ID(resource_id))
            if item is None:
                raise StockCommandError("item da reserva nao encontrado")
            if item.quantidade_reservada < quantity:
                raise StockCommandError("saldo reservado insuficiente para liberacao")
            await item_repo.atualizar(
                replace(
                    item,
                    quantidade_disponivel=item.quantidade_disponivel + quantity,
                    quantidade_reservada=item.quantidade_reservada - quantity,
                    atualizado_em=now,
                )
            )
            reservation = reservation.liberar()
            await reservation_repo.atualizar(reservation)
        elif reservation.status is not StatusReserva.LIBERADA:
            raise StockCommandError(
                f"reserva em estado {reservation.status.value} nao pode ser liberada"
            )

        await self._add_result(
            envelope,
            PostgresOutboxRepository(session),
            event_type="ReservationReleased.v1",
            payload={
                "reservation_id": str(reservation.operation_id),
                "resource_id": str(resource_id),
                "quantity": quantity,
                "released_at": self._timestamp(now),
            },
            occurred_at=now,
        )

    async def _add_result(
        self,
        envelope: Mapping[str, object],
        outbox: PostgresOutboxRepository,
        *,
        event_type: str,
        payload: Mapping[str, object],
        occurred_at: datetime,
    ) -> None:
        await outbox.add(
            OutboxMessage(
                message_id=self.message_id_factory(),
                aggregate_type="StockOperation",
                aggregate_id=self._uuid(envelope, "operation_id"),
                event_type=event_type,
                event_version=1,
                payload=payload,
                correlation_id=self._uuid(envelope, "correlation_id"),
                causation_id=self._uuid(envelope, "message_id"),
                saga_id=(
                    UUID(str(envelope["saga_id"])) if envelope.get("saga_id") else None
                ),
                occurred_at=occurred_at,
            )
        )

    @staticmethod
    def _payload(envelope: Mapping[str, object]) -> Mapping[str, object]:
        payload = envelope.get("payload")
        if not isinstance(payload, Mapping):
            raise StockCommandError("payload ausente ou invalido")
        return payload

    @staticmethod
    def _uuid(value: Mapping[str, object], field: str) -> UUID:
        try:
            return UUID(str(value[field]))
        except (KeyError, TypeError, ValueError) as exc:
            raise StockCommandError(f"{field} invalido") from exc

    @staticmethod
    def _quantity(payload: Mapping[str, object]) -> int:
        value = payload.get("quantity")
        if isinstance(value, bool) or not isinstance(value, int):
            raise StockCommandError("quantity deve ser inteiro")
        return value

    @staticmethod
    def _timestamp(value: datetime) -> str:
        return value.isoformat().replace("+00:00", "Z")

    @staticmethod
    def _money(value: Decimal) -> str:
        return f"{value:.2f}"

    @staticmethod
    def _require_matching_reservation(
        reservation: Reserva,
        *,
        resource_id: UUID,
        quantity: int,
    ) -> None:
        if reservation.item_estoque_id != resource_id:
            raise StockCommandError("resource_id diverge da reserva")
        if reservation.quantidade != quantity:
            raise StockCommandError("quantity diverge da reserva")
