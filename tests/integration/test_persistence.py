import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlmodel.ext.asyncio.session import AsyncSession

from app.modules.estoque.domain.entities.item_estoque import ItemEstoque, TipoItemEstoque
from app.modules.estoque.infrastructure.db.models.item_estoque import ItemEstoqueModel
from app.modules.estoque.infrastructure.db.models.reserva import ReservaModel
from app.modules.estoque.infrastructure.db.repositories.item_estoque_repo import ItemEstoqueRepo
from app.modules.estoque.infrastructure.db.uow.item_estoque_uow import ItemEstoqueUnitOfWork
from app.modules.servicos.domain.entities.servico import Servico
from app.modules.servicos.infrastructure.db.models.servico import ServicoModel
from app.modules.servicos.infrastructure.db.repositories.servico_repo import ServicoRepo
from app.shared.value_objects.id import ID


def _factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _item(*, disponivel: int = 10, reservada: int = 2) -> ItemEstoque:
    now = datetime.now(UTC)
    return ItemEstoque(
        id=ID.generate(),
        tipo=TipoItemEstoque.PECA,
        nome="Filtro de óleo",
        descricao="Filtro premium",
        codigo=f"FO-{uuid4().hex[:8]}",
        quantidade_disponivel=disponivel,
        quantidade_reservada=reservada,
        quantidade_minima=3,
        valor_unitario=Decimal("42.90"),
        ativo=True,
        criado_em=now,
        atualizado_em=now,
    )


async def test_persiste_e_recupera_catalogo_e_item_de_estoque(
    db_engine: AsyncEngine,
) -> None:
    now = datetime.now(UTC)
    servico = Servico(
        id=ID.generate(),
        nome="Troca de óleo",
        descricao="Serviço completo",
        valor_base=Decimal("150.00"),
        tempo_medio_minutos=40,
        ativo=True,
        criado_em=now,
        atualizado_em=now,
    )
    item = _item()
    factory = _factory(db_engine)
    async with factory() as session:
        await ServicoRepo(session).salvar(servico)
        await ItemEstoqueRepo(session).salvar(item)
        await session.commit()

    async with factory() as session:
        servico_recuperado = await ServicoRepo(session).obter_por_nome(servico.nome)
        item_recuperado = await ItemEstoqueRepo(session).obter_por_codigo(item.codigo)

    assert servico_recuperado == servico
    assert item_recuperado == item


@pytest.mark.parametrize(
    ("disponivel", "reservada"),
    [(-1, 0), (0, -1)],
)
async def test_banco_rejeita_saldos_negativos(
    db_engine: AsyncEngine,
    disponivel: int,
    reservada: int,
) -> None:
    now = datetime.now(UTC)
    model = ItemEstoqueModel(
        id=uuid4(),
        tipo="PECA",
        nome="Saldo inválido",
        descricao=None,
        codigo=uuid4().hex,
        quantidade_disponivel=disponivel,
        quantidade_reservada=reservada,
        quantidade_minima=0,
        valor_unitario=Decimal("1.00"),
        ativo=True,
        criado_em=now,
        atualizado_em=now,
    )
    factory = _factory(db_engine)
    async with factory() as session:
        session.add(model)
        with pytest.raises(IntegrityError):
            await session.commit()


async def test_reserva_e_unica_por_operation_id_e_preserva_payload(
    db_engine: AsyncEngine,
) -> None:
    item = _item()
    operation_id = uuid4()
    order_id = uuid4()
    now = datetime.now(UTC)
    factory = _factory(db_engine)
    async with factory() as session:
        await ItemEstoqueRepo(session).salvar(item)
        session.add(
            ReservaModel(
                operation_id=operation_id,
                ordem_servico_id=order_id,
                ordem_servico_item_id=uuid4(),
                item_estoque_id=item.id.value,
                quantidade=3,
                status="RESERVADA",
                criado_em=now,
                atualizado_em=now,
            )
        )
        await session.commit()

    async with factory() as session:
        recovered = await session.get(ReservaModel, operation_id)
        assert recovered is not None
        assert recovered.ordem_servico_id == order_id
        assert recovered.item_estoque_id == item.id.value
        assert recovered.quantidade == 3

    async with factory() as session:
        session.add(
            ReservaModel(
                operation_id=operation_id,
                ordem_servico_id=order_id,
                ordem_servico_item_id=uuid4(),
                item_estoque_id=item.id.value,
                quantidade=3,
                status="RESERVADA",
                criado_em=now,
                atualizado_em=now,
            )
        )
        with pytest.raises(IntegrityError):
            await session.commit()


@pytest.mark.parametrize(
    ("quantidade", "status"),
    [(0, "PENDENTE"), (1, "INVALIDA")],
)
async def test_reserva_respeita_constraints_de_quantidade_e_estado(
    db_engine: AsyncEngine,
    quantidade: int,
    status: str,
) -> None:
    item = _item()
    now = datetime.now(UTC)
    factory = _factory(db_engine)
    async with factory() as session:
        await ItemEstoqueRepo(session).salvar(item)
        await session.flush()
        session.add(
            ReservaModel(
                operation_id=uuid4(),
                ordem_servico_id=uuid4(),
                ordem_servico_item_id=uuid4(),
                item_estoque_id=item.id.value,
                quantidade=quantidade,
                status=status,
                criado_em=now,
                atualizado_em=now,
            )
        )
        with pytest.raises(IntegrityError):
            await session.commit()


async def test_lock_pessimista_bloqueia_transacao_concorrente(
    db_engine: AsyncEngine,
) -> None:
    item = _item()
    factory = _factory(db_engine)
    async with factory() as setup:
        await ItemEstoqueRepo(setup).salvar(item)
        await setup.commit()

    async with factory() as first, factory() as second:
        locked = await ItemEstoqueRepo(first).obter_por_id_com_lock(item.id)
        assert locked == item

        async def competing_lock() -> None:
            await second.exec(text("SET LOCAL lock_timeout = '200ms'"))
            await ItemEstoqueRepo(second).obter_por_id_com_lock(item.id)

        task = asyncio.create_task(competing_lock())
        with pytest.raises(DBAPIError):
            await task
        await second.rollback()
        await first.commit()

    async with factory() as third:
        assert await ItemEstoqueRepo(third).obter_por_id_com_lock(item.id) == item
        await third.rollback()


async def test_transacao_local_faz_rollback_sem_efeito_parcial(
    db_engine: AsyncEngine,
) -> None:
    item = _item()
    factory = _factory(db_engine)

    with pytest.raises(RuntimeError, match="forçar rollback"):
        async with ItemEstoqueUnitOfWork(factory()) as uow:
            await uow.item_estoque_repo.salvar(item)
            raise RuntimeError("forçar rollback")

    async with factory() as session:
        assert await ItemEstoqueRepo(session).obter_por_id(item.id) is None


async def test_atualizacao_de_saldos_persiste_sem_violar_invariantes(
    db_engine: AsyncEngine,
) -> None:
    item = _item(disponivel=8, reservada=0)
    factory = _factory(db_engine)
    async with factory() as session:
        repo = ItemEstoqueRepo(session)
        await repo.salvar(item)
        await session.commit()
        atualizado = replace(
            item,
            quantidade_disponivel=5,
            quantidade_reservada=3,
            atualizado_em=datetime.now(UTC),
        )
        await repo.atualizar(atualizado)
        await session.commit()

    async with factory() as session:
        recovered = await ItemEstoqueRepo(session).obter_por_id(item.id)

    assert recovered == atualizado
    assert recovered.quantidade_disponivel >= 0
    assert recovered.quantidade_reservada >= 0


async def test_isolamento_inicia_cada_teste_sem_dados(db_session: AsyncSession) -> None:
    services = await db_session.scalar(select(func.count()).select_from(ServicoModel))
    items = await db_session.scalar(select(func.count()).select_from(ItemEstoqueModel))
    reservations = await db_session.scalar(select(func.count()).select_from(ReservaModel))
    assert (services, items, reservations) == (0, 0, 0)
