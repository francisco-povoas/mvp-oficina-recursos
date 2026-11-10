"""Regras funcionais de reserva dentro da transacao Inbox/Outbox."""

import asyncio
import json
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.application.inbox_consumer import ProcessingOutcome
from app.infrastructure.messaging.contracts import JsonSchemaMessageValidator
from app.infrastructure.messaging.inbox import (
    InboxEffectModel,
    InboxMessageModel,
    PostgresInboxProcessor,
)
from app.modules.estoque.application.stock_command_handler import (
    MAX_QUANTITY,
    StockCommandHandler,
)
from app.modules.estoque.infrastructure.db.models.item_estoque import ItemEstoqueModel
from app.modules.estoque.infrastructure.db.models.reserva import ReservaModel
from app.shared.infra.db.outbox_model import OutboxMessageModel

CONSUMER = "recursos.commands.v1"
SCHEMA = "contracts/vendor/ordens/saga-stock.v1.schema.json"


def factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def save_item(engine: AsyncEngine, *, available: int, reserved: int = 0) -> UUID:
    resource_id = uuid4()
    now = datetime.now(UTC)
    async with factory(engine)() as session:
        session.add(
            ItemEstoqueModel(
                id=resource_id,
                tipo="PECA",
                nome="Filtro autoritativo",
                descricao="Persistido em Recursos",
                codigo=f"FT-{resource_id.hex[:8]}",
                quantidade_disponivel=available,
                quantidade_reservada=reserved,
                quantidade_minima=1,
                valor_unitario=Decimal("42.90"),
                ativo=True,
                criado_em=now,
                atualizado_em=now,
            )
        )
        await session.commit()
    return resource_id


def reserve_envelope(
    resource_id: UUID,
    *,
    quantity: int = 2,
    operation_id: UUID | None = None,
    order_id: UUID | None = None,
    order_item_id: UUID | None = None,
) -> dict[str, object]:
    return {
        "message_id": str(uuid4()),
        "message_type": "ReserveItem",
        "message_version": 1,
        "occurred_at": "2026-11-10T12:00:00Z",
        "correlation_id": str(uuid4()),
        "causation_id": None,
        "operation_id": str(operation_id or uuid4()),
        "saga_id": str(uuid4()),
        "payload": {
            "order_id": str(order_id or uuid4()),
            "order_item_id": str(order_item_id or uuid4()),
            "resource_id": str(resource_id),
            "quantity": quantity,
            "resource_snapshot": {
                "name": "Snapshot de Ordens",
                "description": None,
                "unit_price": "1.00",
            },
        },
    }


def release_envelope(
    reservation_id: UUID,
    resource_id: UUID,
    *,
    quantity: int = 2,
    operation_id: UUID | None = None,
) -> dict[str, object]:
    return {
        "message_id": str(uuid4()),
        "message_type": "ReleaseReservation",
        "message_version": 1,
        "occurred_at": "2026-11-10T13:00:00Z",
        "correlation_id": str(uuid4()),
        "causation_id": str(uuid4()),
        "operation_id": str(operation_id or uuid4()),
        "saga_id": str(uuid4()),
        "payload": {
            "reservation_id": str(reservation_id),
            "resource_id": str(resource_id),
            "quantity": quantity,
            "reason": "PENDING_CANCELLATION",
        },
    }


def processor(engine: AsyncEngine, handler=None) -> PostgresInboxProcessor:
    return PostgresInboxProcessor(
        factory(engine),
        consumer_name=CONSUMER,
        handler=handler or StockCommandHandler(),
    )


async def load_state(engine: AsyncEngine, resource_id: UUID, operation_id: UUID):
    async with factory(engine)() as session:
        item = await session.get(ItemEstoqueModel, resource_id)
        reservation = await session.get(ReservaModel, operation_id)
        outboxes = list((await session.exec(select(OutboxMessageModel))).all())
        inboxes = list((await session.exec(select(InboxMessageModel))).all())
        effects = list((await session.exec(select(InboxEffectModel))).all())
    return item, reservation, outboxes, inboxes, effects


def validate_outbox(record: OutboxMessageModel) -> dict[str, object]:
    message_type = record.event_type.removesuffix(".v1")
    envelope = {
        "message_id": str(record.id),
        "message_type": message_type,
        "message_version": record.event_version,
        "occurred_at": record.occurred_at.isoformat(),
        "correlation_id": str(record.correlation_id),
        "causation_id": str(record.causation_id) if record.causation_id else None,
        "operation_id": str(record.aggregate_id),
        "saga_id": str(record.saga_id) if record.saga_id else None,
        "payload": json.loads(record.payload),
    }
    JsonSchemaMessageValidator(SCHEMA).validate(envelope)
    return envelope


async def test_reserva_suficiente_move_saldo_e_grava_inbox_outbox_atomicas(
    db_engine: AsyncEngine,
) -> None:
    resource_id = await save_item(db_engine, available=5, reserved=1)
    value = reserve_envelope(resource_id, quantity=2)

    outcome = await processor(db_engine).process(value, receive_count=1)

    assert outcome is ProcessingOutcome.PROCESSED
    item, reservation, outboxes, inboxes, effects = await load_state(
        db_engine, resource_id, UUID(str(value["operation_id"]))
    )
    assert (item.quantidade_disponivel, item.quantidade_reservada) == (3, 3)
    assert reservation.status == "RESERVADA"
    assert reservation.ordem_servico_item_id == UUID(str(value["payload"]["order_item_id"]))
    assert len(outboxes) == len(inboxes) == len(effects) == 1
    assert inboxes[0].status == effects[0].status == "PROCESSED"
    published = validate_outbox(outboxes[0])
    assert published["message_type"] == "ResourceReserved"
    assert published["payload"]["reservation_id"] == value["operation_id"]
    assert published["payload"]["resource_snapshot"]["name"] == "Filtro autoritativo"


async def test_saldo_insuficiente_cria_backorder_sem_alterar_saldos(
    db_engine: AsyncEngine,
) -> None:
    resource_id = await save_item(db_engine, available=1, reserved=4)
    value = reserve_envelope(resource_id, quantity=2)

    assert await processor(db_engine).process(value, receive_count=1) is ProcessingOutcome.PROCESSED

    item, reservation, outboxes, *_ = await load_state(
        db_engine, resource_id, UUID(str(value["operation_id"]))
    )
    assert (item.quantidade_disponivel, item.quantidade_reservada) == (1, 4)
    assert reservation.status == "SEM_ESTOQUE"
    result = validate_outbox(outboxes[0])
    assert result["message_type"] == "ItemBackordered"
    assert result["payload"]["available_quantity"] == 1


async def test_recurso_inexistente_produz_rejeicao_sem_reserva(
    db_engine: AsyncEngine,
) -> None:
    resource_id = uuid4()
    value = reserve_envelope(resource_id)

    await processor(db_engine).process(value, receive_count=1)

    _, reservation, outboxes, inboxes, effects = await load_state(
        db_engine, resource_id, UUID(str(value["operation_id"]))
    )
    assert reservation is None
    assert inboxes[0].status == effects[0].status == "PROCESSED"
    result = validate_outbox(outboxes[0])
    assert result["payload"]["rejection_code"] == "RESOURCE_NOT_FOUND"


async def test_quantidade_fora_do_integer_postgresql_produz_rejeicao_contratual(
    db_engine: AsyncEngine,
) -> None:
    resource_id = await save_item(db_engine, available=10)
    value = reserve_envelope(resource_id, quantity=MAX_QUANTITY + 1)
    JsonSchemaMessageValidator(SCHEMA).validate(value)

    await processor(db_engine).process(value, receive_count=1)

    item, reservation, outboxes, *_ = await load_state(
        db_engine, resource_id, UUID(str(value["operation_id"]))
    )
    assert (item.quantidade_disponivel, item.quantidade_reservada) == (10, 0)
    assert reservation is None
    result = validate_outbox(outboxes[0])
    assert result["payload"]["rejection_code"] == "INVALID_QUANTITY"


async def test_reentrega_e_nova_emissao_iguais_nao_repetem_reserva(
    db_engine: AsyncEngine,
) -> None:
    resource_id = await save_item(db_engine, available=5)
    first = reserve_envelope(resource_id, quantity=3)
    second = deepcopy(first)
    second["message_id"] = str(uuid4())
    second["correlation_id"] = str(uuid4())
    command_processor = processor(db_engine)

    assert await command_processor.process(first, receive_count=1) is ProcessingOutcome.PROCESSED
    assert await command_processor.process(first, receive_count=2) is ProcessingOutcome.DUPLICATE
    assert await command_processor.process(second, receive_count=1) is ProcessingOutcome.DUPLICATE

    item, reservation, outboxes, inboxes, effects = await load_state(
        db_engine, resource_id, UUID(str(first["operation_id"]))
    )
    assert reservation.status == "RESERVADA"
    assert (item.quantidade_disponivel, item.quantidade_reservada) == (2, 3)
    assert len(outboxes) == len(effects) == 1
    assert sorted(message.status for message in inboxes) == ["DUPLICATE", "PROCESSED"]


async def test_mesma_operacao_com_payload_divergente_preserva_primeiro_resultado(
    db_engine: AsyncEngine,
) -> None:
    resource_id = await save_item(db_engine, available=8)
    first = reserve_envelope(resource_id, quantity=2)
    divergent = deepcopy(first)
    divergent["message_id"] = str(uuid4())
    divergent["payload"]["quantity"] = 3
    command_processor = processor(db_engine)

    await command_processor.process(first, receive_count=1)
    assert (
        await command_processor.process(divergent, receive_count=1)
        is ProcessingOutcome.CONFLICT
    )

    item, reservation, outboxes, inboxes, _ = await load_state(
        db_engine, resource_id, UUID(str(first["operation_id"]))
    )
    assert reservation.quantidade == 2
    assert (item.quantidade_disponivel, item.quantidade_reservada) == (6, 2)
    assert len(outboxes) == 1
    assert {message.status for message in inboxes} == {"PROCESSED", "CONFLICT"}


async def test_reservas_concorrentes_serializam_saldo_do_mesmo_item(
    db_engine: AsyncEngine,
) -> None:
    resource_id = await save_item(db_engine, available=5)
    first = reserve_envelope(resource_id, quantity=4)
    second = reserve_envelope(resource_id, quantity=4)

    outcomes = await asyncio.gather(
        processor(db_engine).process(first, receive_count=1),
        processor(db_engine).process(second, receive_count=1),
    )

    assert outcomes == [ProcessingOutcome.PROCESSED, ProcessingOutcome.PROCESSED]
    async with factory(db_engine)() as session:
        item = await session.get(ItemEstoqueModel, resource_id)
        reservations = list((await session.exec(select(ReservaModel))).all())
        outboxes = list((await session.exec(select(OutboxMessageModel))).all())
    assert (item.quantidade_disponivel, item.quantidade_reservada) == (1, 4)
    assert {reservation.status for reservation in reservations} == {"RESERVADA", "SEM_ESTOQUE"}
    assert {record.event_type for record in outboxes} == {
        "ResourceReserved.v1",
        "ItemBackordered.v1",
    }


class FailAfterStockEffect:
    async def handle(self, envelope, session) -> None:
        await StockCommandHandler().handle(envelope, session)
        raise RuntimeError("falha injetada antes do commit")


async def test_falha_reverte_estoque_reserva_outbox_inbox_e_retry_recupera(
    db_engine: AsyncEngine,
) -> None:
    resource_id = await save_item(db_engine, available=5)
    value = reserve_envelope(resource_id, quantity=2)

    try:
        await processor(db_engine, FailAfterStockEffect()).process(value, receive_count=1)
    except RuntimeError as exc:
        assert str(exc) == "falha injetada antes do commit"
    else:  # pragma: no cover
        raise AssertionError("a falha injetada deveria escapar do processor")

    item, reservation, outboxes, inboxes, effects = await load_state(
        db_engine, resource_id, UUID(str(value["operation_id"]))
    )
    assert (item.quantidade_disponivel, item.quantidade_reservada) == (5, 0)
    assert reservation is None
    assert outboxes == inboxes == effects == []

    assert await processor(db_engine).process(value, receive_count=2) is ProcessingOutcome.PROCESSED
    async with factory(db_engine)() as session:
        assert await session.scalar(sa.select(sa.func.count()).select_from(OutboxMessageModel)) == 1


async def test_liberacao_normal_devolve_saldo_e_publica_resultado_valido(
    db_engine: AsyncEngine,
) -> None:
    resource_id = await save_item(db_engine, available=5)
    reserve = reserve_envelope(resource_id, quantity=2)
    await processor(db_engine).process(reserve, receive_count=1)
    release = release_envelope(
        UUID(str(reserve["operation_id"])),
        resource_id,
        quantity=2,
    )
    JsonSchemaMessageValidator(SCHEMA).validate(release)

    assert await processor(db_engine).process(release, receive_count=1) is ProcessingOutcome.PROCESSED

    async with factory(db_engine)() as session:
        item = await session.get(ItemEstoqueModel, resource_id)
        reservation = await session.get(ReservaModel, UUID(str(reserve["operation_id"])))
        outboxes = list(
            (
                await session.exec(
                    select(OutboxMessageModel).order_by(OutboxMessageModel.occurred_at)
                )
            ).all()
        )
    assert (item.quantidade_disponivel, item.quantidade_reservada) == (5, 0)
    assert reservation.status == "LIBERADA"
    assert [record.event_type for record in outboxes] == [
        "ResourceReserved.v1",
        "ReservationReleased.v1",
    ]
    result = validate_outbox(outboxes[-1])
    assert result["operation_id"] == release["operation_id"]
    assert result["payload"]["reservation_id"] == reserve["operation_id"]


async def test_liberacao_repetida_com_mesma_operacao_nao_incrementa_saldo_novamente(
    db_engine: AsyncEngine,
) -> None:
    resource_id = await save_item(db_engine, available=4, reserved=0)
    reserve = reserve_envelope(resource_id, quantity=3)
    await processor(db_engine).process(reserve, receive_count=1)
    release = release_envelope(UUID(str(reserve["operation_id"])), resource_id, quantity=3)
    redelivery = deepcopy(release)
    redelivery["message_id"] = str(uuid4())
    redelivery["correlation_id"] = str(uuid4())
    command_processor = processor(db_engine)

    assert await command_processor.process(release, receive_count=1) is ProcessingOutcome.PROCESSED
    assert await command_processor.process(redelivery, receive_count=2) is ProcessingOutcome.DUPLICATE

    async with factory(db_engine)() as session:
        item = await session.get(ItemEstoqueModel, resource_id)
        release_results = list(
            (
                await session.exec(
                    select(OutboxMessageModel).where(
                        OutboxMessageModel.event_type == "ReservationReleased.v1"
                    )
                )
            ).all()
        )
    assert (item.quantidade_disponivel, item.quantidade_reservada) == (4, 0)
    assert len(release_results) == 1


async def test_nova_operacao_sobre_reserva_ja_liberada_retorna_resultado_sem_efeito(
    db_engine: AsyncEngine,
) -> None:
    resource_id = await save_item(db_engine, available=6)
    reserve = reserve_envelope(resource_id, quantity=2)
    await processor(db_engine).process(reserve, receive_count=1)
    first = release_envelope(UUID(str(reserve["operation_id"])), resource_id, quantity=2)
    second = release_envelope(UUID(str(reserve["operation_id"])), resource_id, quantity=2)

    await processor(db_engine).process(first, receive_count=1)
    await processor(db_engine).process(second, receive_count=1)

    async with factory(db_engine)() as session:
        item = await session.get(ItemEstoqueModel, resource_id)
        results = list(
            (
                await session.exec(
                    select(OutboxMessageModel).where(
                        OutboxMessageModel.event_type == "ReservationReleased.v1"
                    )
                )
            ).all()
        )
    assert (item.quantidade_disponivel, item.quantidade_reservada) == (6, 0)
    assert len(results) == 2


async def test_backorder_nao_e_convertido_em_liberacao_fisica(
    db_engine: AsyncEngine,
) -> None:
    resource_id = await save_item(db_engine, available=1)
    reserve = reserve_envelope(resource_id, quantity=2)
    await processor(db_engine).process(reserve, receive_count=1)
    release = release_envelope(UUID(str(reserve["operation_id"])), resource_id, quantity=2)

    try:
        await processor(db_engine).process(release, receive_count=1)
    except RuntimeError as exc:
        assert "SEM_ESTOQUE" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("backorder nao deve ser confirmado como liberacao")

    async with factory(db_engine)() as session:
        item = await session.get(ItemEstoqueModel, resource_id)
        reservation = await session.get(ReservaModel, UUID(str(reserve["operation_id"])))
        release_count = await session.scalar(
            sa.select(sa.func.count())
            .select_from(OutboxMessageModel)
            .where(OutboxMessageModel.event_type == "ReservationReleased.v1")
        )
        release_inbox = await session.get(InboxMessageModel, (CONSUMER, release["message_id"]))
    assert (item.quantidade_disponivel, item.quantidade_reservada) == (1, 0)
    assert reservation.status == "SEM_ESTOQUE"
    assert release_count == 0 and release_inbox is None
