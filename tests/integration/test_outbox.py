"""Outbox funcional de Recursos validada contra PostgreSQL real."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlmodel.ext.asyncio.session import AsyncSession

from app.modules.estoque.domain.entities.item_estoque import ItemEstoque, TipoItemEstoque
from app.modules.estoque.infrastructure.db.repositories.item_estoque_repo import ItemEstoqueRepo
from app.modules.estoque.infrastructure.db.uow.item_estoque_uow import ItemEstoqueUnitOfWork
from app.shared.infra.db.outbox_model import OutboxMessageModel
from app.shared.infra.db.outbox_repository import PostgresOutboxRepository
from app.shared.outbox import OutboxClaimConflictError, OutboxMessage
from app.shared.value_objects.id import ID


def _factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _item() -> ItemEstoque:
    now = datetime.now(UTC)
    return ItemEstoque(
        id=ID.generate(),
        tipo=TipoItemEstoque.PECA,
        nome="Filtro da Outbox",
        descricao="Item para validar transacao local",
        codigo=f"OUT-{uuid4().hex[:8]}",
        quantidade_disponivel=10,
        quantidade_reservada=0,
        quantidade_minima=1,
        valor_unitario=Decimal("42.90"),
        ativo=True,
        criado_em=now,
        atualizado_em=now,
    )


def _message(*, aggregate_id, occurred_at: datetime, message_id=None) -> OutboxMessage:
    return OutboxMessage(
        message_id=message_id or uuid4(),
        aggregate_type="ReservaEstoque",
        aggregate_id=aggregate_id,
        event_type="ResourceReserved.v1",
        event_version=1,
        payload={"quantity": 1, "reservation_id": str(uuid4())},
        correlation_id=uuid4(),
        causation_id=uuid4(),
        occurred_at=occurred_at,
    )


async def _persist_messages(
    engine: AsyncEngine,
    *,
    count: int,
    occurred_at: datetime,
) -> list[OutboxMessage]:
    messages = [
        _message(aggregate_id=uuid4(), occurred_at=occurred_at) for _ in range(count)
    ]
    factory = _factory(engine)
    async with factory() as session:
        repository = PostgresOutboxRepository(session)
        for message in messages:
            await repository.add(message)
        await session.commit()
    return messages


async def test_item_e_evento_confirmam_na_mesma_transacao(
    db_engine: AsyncEngine,
) -> None:
    factory = _factory(db_engine)
    item = _item()
    now = datetime.now(UTC)
    message = _message(aggregate_id=item.id.value, occurred_at=now)

    async with ItemEstoqueUnitOfWork(factory()) as uow:
        await uow.item_estoque_repo.salvar(item)
        await uow.outbox.add(message)

    async with factory() as session:
        assert await ItemEstoqueRepo(session).obter_por_id(item.id) == item
        stored = await session.get(OutboxMessageModel, message.message_id)

    assert stored is not None
    assert stored.event_type == "ResourceReserved.v1"
    assert stored.next_attempt_at == now


async def test_excecao_reverte_item_e_evento(
    db_engine: AsyncEngine,
) -> None:
    factory = _factory(db_engine)
    item = _item()
    message = _message(aggregate_id=item.id.value, occurred_at=datetime.now(UTC))

    with pytest.raises(RuntimeError, match="falha injetada"):
        async with ItemEstoqueUnitOfWork(factory()) as uow:
            await uow.item_estoque_repo.salvar(item)
            await uow.outbox.add(message)
            raise RuntimeError("falha injetada")

    async with factory() as session:
        assert await ItemEstoqueRepo(session).obter_por_id(item.id) is None
        assert await session.get(OutboxMessageModel, message.message_id) is None


async def test_falha_da_outbox_no_commit_nao_persiste_item(
    db_engine: AsyncEngine,
) -> None:
    factory = _factory(db_engine)
    duplicate_id = uuid4()
    now = datetime.now(UTC)
    async with factory() as session:
        await PostgresOutboxRepository(session).add(
            _message(aggregate_id=uuid4(), occurred_at=now, message_id=duplicate_id)
        )
        await session.commit()

    item = _item()
    with pytest.raises(IntegrityError):
        async with ItemEstoqueUnitOfWork(factory()) as uow:
            await uow.item_estoque_repo.salvar(item)
            await uow.outbox.add(
                _message(
                    aggregate_id=item.id.value,
                    occurred_at=now,
                    message_id=duplicate_id,
                )
            )

    async with factory() as session:
        assert await ItemEstoqueRepo(session).obter_por_id(item.id) is None


async def test_claimers_concorrentes_recebem_lotes_disjuntos(
    db_engine: AsyncEngine,
) -> None:
    now = datetime.now(UTC)
    messages = await _persist_messages(db_engine, count=4, occurred_at=now)
    factory = _factory(db_engine)

    async with factory() as session_a, factory() as session_b:
        claims_a = await PostgresOutboxRepository(session_a).claim_pending(
            worker_id="recursos-a",
            batch_size=2,
            lease_duration=timedelta(seconds=30),
            now=now,
        )
        claims_b = await PostgresOutboxRepository(session_b).claim_pending(
            worker_id="recursos-b",
            batch_size=4,
            lease_duration=timedelta(seconds=30),
            now=now,
        )
        await session_b.commit()
        await session_a.commit()

    ids_a = {claim.message_id for claim in claims_a}
    ids_b = {claim.message_id for claim in claims_b}
    assert len(ids_a) == len(ids_b) == 2
    assert ids_a.isdisjoint(ids_b)
    assert ids_a | ids_b == {message.message_id for message in messages}


async def test_claim_expirado_pode_ser_recuperado(
    db_engine: AsyncEngine,
) -> None:
    now = datetime.now(UTC)
    [message] = await _persist_messages(db_engine, count=1, occurred_at=now)
    factory = _factory(db_engine)

    async with factory() as session:
        repository = PostgresOutboxRepository(session)
        [old_claim] = await repository.claim_pending(
            worker_id="recursos-antigo",
            batch_size=1,
            lease_duration=timedelta(seconds=10),
            now=now,
        )
        await session.commit()

    recovered_at = now + timedelta(seconds=11)
    async with factory() as session:
        repository = PostgresOutboxRepository(session)
        [new_claim] = await repository.claim_pending(
            worker_id="recursos-recuperador",
            batch_size=1,
            lease_duration=timedelta(seconds=10),
            now=recovered_at,
        )
        await session.commit()

    assert new_claim.claim_id != old_claim.claim_id
    assert new_claim.attempts == 2
    async with factory() as session:
        with pytest.raises(OutboxClaimConflictError):
            await PostgresOutboxRepository(session).mark_published(
                message_id=message.message_id,
                claim_id=old_claim.claim_id,
                published_at=recovered_at + timedelta(seconds=1),
            )
        await session.rollback()


async def test_falha_retry_e_confirmacao_preservam_auditoria(
    db_engine: AsyncEngine,
) -> None:
    now = datetime.now(UTC)
    [message] = await _persist_messages(db_engine, count=1, occurred_at=now)
    factory = _factory(db_engine)
    retry_at = now + timedelta(minutes=1)

    async with factory() as session:
        repository = PostgresOutboxRepository(session)
        [claim] = await repository.claim_pending(
            worker_id="recursos",
            batch_size=1,
            lease_duration=timedelta(seconds=30),
            now=now,
        )
        await repository.mark_failed(
            message_id=message.message_id,
            claim_id=claim.claim_id,
            error="SQS indisponivel",
            failed_at=now + timedelta(seconds=1),
            next_attempt_at=retry_at,
        )
        await session.commit()

    async with factory() as session:
        repository = PostgresOutboxRepository(session)
        assert not await repository.claim_pending(
            worker_id="recursos-cedo",
            batch_size=1,
            lease_duration=timedelta(seconds=30),
            now=retry_at - timedelta(microseconds=1),
        )
        await session.rollback()

    published_at = retry_at + timedelta(seconds=1)
    async with factory() as session:
        repository = PostgresOutboxRepository(session)
        [retry_claim] = await repository.claim_pending(
            worker_id="recursos-retry",
            batch_size=1,
            lease_duration=timedelta(seconds=30),
            now=retry_at,
        )
        await repository.mark_published(
            message_id=message.message_id,
            claim_id=retry_claim.claim_id,
            published_at=published_at,
        )
        await session.commit()

    async with factory() as session:
        stored = await session.get(OutboxMessageModel, message.message_id)
    assert stored is not None
    assert stored.attempts == 2
    assert stored.published_at == published_at
    assert stored.last_error == "SQS indisponivel"
    assert stored.last_failed_at == now + timedelta(seconds=1)
    assert stored.claim_id is None


async def test_claim_respeita_limite_de_tentativas(db_engine: AsyncEngine) -> None:
    now = datetime.now(UTC)
    [message] = await _persist_messages(db_engine, count=1, occurred_at=now)
    factory = _factory(db_engine)
    async with factory() as session:
        stored = await session.get(OutboxMessageModel, message.message_id)
        assert stored is not None
        stored.attempts = 8
        await session.commit()
    async with factory() as session:
        assert not await PostgresOutboxRepository(session).claim_pending(
            worker_id="recursos-limitado",
            batch_size=1,
            lease_duration=timedelta(seconds=30),
            max_attempts=8,
            now=now + timedelta(minutes=1),
        )
