import asyncio
from copy import deepcopy
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlmodel.ext.asyncio.session import AsyncSession

from app.application.inbox_consumer import ProcessingOutcome
from app.infrastructure.messaging.inbox import (
    InboxEffectModel,
    InboxMessageModel,
    PostgresInboxProcessor,
)

CONSUMER = "recursos.commands.v1"


def factory(engine: AsyncEngine):
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def envelope(*, operation_id=None, quantity=2):
    return {
        "message_id": str(uuid4()),
        "message_type": "ReserveItem",
        "message_version": 1,
        "occurred_at": "2026-11-09T12:00:00Z",
        "correlation_id": str(uuid4()),
        "causation_id": None,
        "operation_id": str(operation_id or uuid4()),
        "saga_id": str(uuid4()),
        "payload": {
            "order_id": str(uuid4()),
            "order_item_id": str(uuid4()),
            "resource_id": str(uuid4()),
            "quantity": quantity,
            "resource_snapshot": {"name": "Filtro", "description": None, "unit_price": "20.00"},
        },
    }


@pytest.fixture
async def probe_table(db_engine):
    async with db_engine.begin() as connection:
        await connection.execute(
            sa.text("CREATE TABLE consumer_effect_probe (business_key VARCHAR(220) PRIMARY KEY)")
        )
    yield
    async with db_engine.begin() as connection:
        await connection.execute(sa.text("DROP TABLE consumer_effect_probe"))


class ProbeHandler:
    def __init__(self, *, fail=False, delay=0):
        self.fail = fail
        self.delay = delay
        self.calls = 0

    async def handle(self, value, session):
        self.calls += 1
        if self.delay:
            await asyncio.sleep(self.delay)
        await session.exec(
            sa.text("INSERT INTO consumer_effect_probe (business_key) VALUES (:key)"),
            params={"key": f"reserve:{value['operation_id']}"},
        )
        if self.fail:
            raise RuntimeError("falha injetada depois do efeito")


async def count_rows(engine, table):
    async with factory(engine)() as session:
        return (await session.exec(sa.text(f"SELECT count(*) FROM {table}"))).scalar_one()


async def test_processa_efeito_e_inbox_na_mesma_transacao(db_engine, probe_table):
    handler, value = ProbeHandler(), envelope()
    outcome = await PostgresInboxProcessor(
        factory(db_engine), consumer_name=CONSUMER, handler=handler
    ).process(value, receive_count=1)
    assert outcome is ProcessingOutcome.PROCESSED and handler.calls == 1
    async with factory(db_engine)() as session:
        message = await session.get(InboxMessageModel, (CONSUMER, value["message_id"]))
        effect = await session.get(InboxEffectModel, (CONSUMER, f"reserve:{value['operation_id']}"))
    assert message.status == effect.status == "PROCESSED"


async def test_reentrega_mesmo_message_id_nao_repete_efeito(db_engine, probe_table):
    handler, value = ProbeHandler(), envelope()
    processor = PostgresInboxProcessor(factory(db_engine), consumer_name=CONSUMER, handler=handler)
    assert await processor.process(value, receive_count=1) is ProcessingOutcome.PROCESSED
    assert await processor.process(value, receive_count=2) is ProcessingOutcome.DUPLICATE
    assert handler.calls == 1


async def test_novo_message_id_mesma_operacao_e_payload_deduplica_negocio(db_engine, probe_table):
    handler, first = ProbeHandler(), envelope()
    second = deepcopy(first)
    second["message_id"], second["correlation_id"] = str(uuid4()), str(uuid4())
    processor = PostgresInboxProcessor(factory(db_engine), consumer_name=CONSUMER, handler=handler)
    assert await processor.process(first, receive_count=1) is ProcessingOutcome.PROCESSED
    assert await processor.process(second, receive_count=1) is ProcessingOutcome.DUPLICATE
    assert handler.calls == 1 and await count_rows(db_engine, "inbox_message") == 2


async def test_mesma_operacao_payload_divergente_e_conflito(db_engine, probe_table):
    handler, first = ProbeHandler(), envelope()
    second = deepcopy(first)
    second["message_id"], second["payload"]["quantity"] = str(uuid4()), 3
    processor = PostgresInboxProcessor(factory(db_engine), consumer_name=CONSUMER, handler=handler)
    await processor.process(first, receive_count=1)
    assert await processor.process(second, receive_count=1) is ProcessingOutcome.CONFLICT
    assert handler.calls == 1


async def test_falha_reverte_efeito_e_inbox_e_retry_recupera(db_engine, probe_table):
    value = envelope()
    processor = PostgresInboxProcessor(
        factory(db_engine), consumer_name=CONSUMER, handler=ProbeHandler(fail=True)
    )
    with pytest.raises(RuntimeError, match="falha injetada"):
        await processor.process(value, receive_count=1)
    assert await count_rows(db_engine, "consumer_effect_probe") == 0
    assert await count_rows(db_engine, "inbox_message") == 0
    recovered = ProbeHandler()
    restarted = PostgresInboxProcessor(factory(db_engine), consumer_name=CONSUMER, handler=recovered)
    assert await restarted.process(value, receive_count=2) is ProcessingOutcome.PROCESSED


async def test_consumers_concorrentes_produzem_um_efeito(db_engine, probe_table):
    first = envelope(operation_id=uuid4())
    second = deepcopy(first)
    second["message_id"] = str(uuid4())
    handler = ProbeHandler(delay=0.05)
    a = PostgresInboxProcessor(factory(db_engine), consumer_name=CONSUMER, handler=handler)
    b = PostgresInboxProcessor(factory(db_engine), consumer_name=CONSUMER, handler=handler)
    outcomes = await asyncio.gather(
        a.process(first, receive_count=1), b.process(second, receive_count=1)
    )
    assert set(outcomes) == {ProcessingOutcome.PROCESSED, ProcessingOutcome.DUPLICATE}
    assert handler.calls == 1
