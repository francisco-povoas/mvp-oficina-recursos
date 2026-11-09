import asyncio
import json
import time
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from app.application.messaging import (
    MessageContractError,
    MessagePublicationError,
    MessagePublicationIndeterminateError,
)
from app.application.outbox_publisher import (
    OutboxPublisher,
    PublisherPolicy,
    StaticRecursosMessageRouter,
    build_envelope,
)
from app.infrastructure.messaging.contracts import JsonSchemaMessageValidator
from app.infrastructure.messaging.sqs import SqsFifoTransport
from app.shared.outbox import ClaimedOutboxMessage

NOW = datetime(2026, 11, 8, 19, 0, tzinfo=UTC)
SCHEMA = "contracts/vendor/ordens/saga-stock.v1.schema.json"


def claimed(
    *,
    event_type: str = "ResourceReserved.v1",
    attempts: int = 1,
    payload: dict | None = None,
) -> ClaimedOutboxMessage:
    return ClaimedOutboxMessage(
        message_id=uuid4(),
        aggregate_type="StockOperation",
        aggregate_id=uuid4(),
        event_type=event_type,
        event_version=1,
        payload=payload
        or {
            "reservation_id": str(uuid4()),
            "resource_id": str(uuid4()),
            "quantity": 2,
            "reserved_at": "2026-11-08T19:00:00Z",
            "resource_snapshot": {
                "name": "Filtro",
                "description": None,
                "unit_price": "42.90",
            },
        },
        correlation_id=uuid4(),
        causation_id=uuid4(),
        saga_id=uuid4(),
        occurred_at=NOW,
        attempts=attempts,
        claim_id=uuid4(),
        claimed_by="worker-test",
        claimed_at=NOW,
        claim_expires_at=NOW + timedelta(seconds=30),
    )


class FakeStore:
    def __init__(self, messages):
        self.messages = messages
        self.claim_args = None
        self.published = []
        self.failed = []
        self.confirm_error = None

    async def claim_pending(self, **kwargs):
        self.claim_args = kwargs
        messages, self.messages = self.messages, []
        return messages

    async def mark_published(self, **kwargs):
        if self.confirm_error:
            raise self.confirm_error
        self.published.append(kwargs)

    async def mark_failed(self, **kwargs):
        self.failed.append(kwargs)


class FakeTransport:
    def __init__(self, error=None):
        self.error = error
        self.sent = []

    async def publish(self, **kwargs):
        if self.error:
            raise self.error
        self.sent.append(kwargs)
        return "elasticmq-id"

    async def close(self):
        return None


class PassValidator:
    def validate(self, _envelope):
        return None


def publisher(store, transport, *, validator=None) -> OutboxPublisher:
    return OutboxPublisher(
        worker_id="worker-test",
        store=store,
        router=StaticRecursosMessageRouter("queue.fifo"),
        validator=validator or PassValidator(),
        transport=transport,
        policy=PublisherPolicy(poll_interval_seconds=0.01),
        clock=lambda: NOW,
        jitter=lambda _start, cap: cap,
    )


async def test_publica_envelope_metadados_e_atributos_fifo() -> None:
    message = claimed()
    store = FakeStore([message])
    transport = FakeTransport()
    assert await publisher(store, transport).run_once() == 1
    sent = transport.sent[0]
    body = json.loads(sent["body"])
    assert body["correlation_id"] == str(message.correlation_id)
    assert body["causation_id"] == str(message.causation_id)
    assert body["operation_id"] == str(message.aggregate_id)
    assert body["saga_id"] == str(message.saga_id)
    assert sent["deduplication_id"] == str(message.message_id)
    assert sent["route"].group_id == str(message.aggregate_id)
    assert store.published and not store.failed
    assert store.claim_args["max_attempts"] == 8


async def test_schema_invalido_e_rota_desconhecida_permanecem_pendentes() -> None:
    invalid = claimed(payload={"quantity": 0})
    store = FakeStore([invalid])
    transport = FakeTransport()
    await publisher(store, transport, validator=JsonSchemaMessageValidator(SCHEMA)).run_once()
    assert not transport.sent and not store.published
    assert store.failed[0]["error"].startswith("MessageContractError:")

    unknown_store = FakeStore([claimed(event_type="ReserveItem.v1")])
    await publisher(unknown_store, transport).run_once()
    assert unknown_store.failed[0]["error"].startswith("UnknownMessageRouteError:")


@pytest.mark.parametrize(
    "event_type",
    [
        "ResourceReserved.v1",
        "ItemBackordered.v1",
        "ReservationRejected.v1",
        "ReservationReleased.v1",
        "BackorderedItemReceived.v1",
        "ReservationConsumed.v1",
    ],
)
def test_todos_os_seis_eventos_possuem_a_mesma_rota_fifo(event_type: str) -> None:
    message = claimed(event_type=event_type)
    route = StaticRecursosMessageRouter("recursos-to-ordens.fifo").resolve(message)
    assert route.queue_url == "recursos-to-ordens.fifo"
    assert route.group_id == str(message.aggregate_id)


async def test_falha_agenda_backoff_e_confirmacao_falha_nao_reclassifica_envio() -> None:
    message = claimed(attempts=4)
    failed_store = FakeStore([message])
    await publisher(
        failed_store,
        FakeTransport(MessagePublicationError("indisponivel")),
    ).run_once()
    assert failed_store.failed[0]["next_attempt_at"] == NOW + timedelta(seconds=8)

    confirmation_store = FakeStore([claimed()])
    confirmation_store.confirm_error = RuntimeError("postgres")
    transport = FakeTransport()
    await publisher(confirmation_store, transport).run_once()
    assert transport.sent and not confirmation_store.failed


async def test_loop_encerra_quando_sinalizado() -> None:
    store = FakeStore([])
    stop = asyncio.Event()
    task = asyncio.create_task(publisher(store, FakeTransport()).run(stop))
    await asyncio.sleep(0)
    stop.set()
    await asyncio.wait_for(task, timeout=0.2)


def test_envelope_rejeita_tipo_versao_incoerentes() -> None:
    with pytest.raises(MessageContractError):
        build_envelope(claimed(event_type="ResourceReserved.v2"))


class SlowClient:
    def send_message(self, **_kwargs):
        time.sleep(0.05)
        return {"MessageId": "late", "ResponseMetadata": {"HTTPStatusCode": 200}}


class UnknownClient:
    def send_message(self, **_kwargs):
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}


async def test_timeout_e_resposta_indeterminada_sao_tipados() -> None:
    route = StaticRecursosMessageRouter("queue.fifo").resolve(claimed())
    with pytest.raises(MessagePublicationIndeterminateError, match="timeout"):
        await SqsFifoTransport(SlowClient(), send_timeout_seconds=0.001).publish(
            route=route, body="{}", deduplication_id=str(uuid4())
        )
    with pytest.raises(MessagePublicationIndeterminateError, match="confirmacao"):
        await SqsFifoTransport(UnknownClient(), send_timeout_seconds=1).publish(
            route=route, body="{}", deduplication_id=str(uuid4())
        )
