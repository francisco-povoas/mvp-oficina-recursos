import asyncio
import json
from uuid import uuid4

import pytest

from app.application.inbox_consumer import (
    ConsumerPolicy,
    InboxConsumer,
    ProcessingOutcome,
    ReceivedMessage,
)


def envelope(message_type="ReserveItem"):
    return {
        "message_id": str(uuid4()),
        "message_type": message_type,
        "message_version": 1,
        "occurred_at": "2026-11-09T12:00:00Z",
        "correlation_id": str(uuid4()),
        "causation_id": None,
        "operation_id": str(uuid4()),
        "saga_id": str(uuid4()),
        "payload": {},
    }


class Validator:
    def __init__(self, error=None):
        self.error = error

    def validate(self, _value):
        if self.error:
            raise self.error


class Processor:
    def __init__(self, outcome=ProcessingOutcome.PROCESSED, error=None, delay=0):
        self.outcome = outcome
        self.error = error
        self.delay = delay
        self.calls = []

    async def process(self, value, *, receive_count):
        self.calls.append((value, receive_count))
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.error:
            raise self.error
        return self.outcome


class Transport:
    def __init__(self, messages=None, delete_error=None):
        self.messages = list(messages or [])
        self.delete_error = delete_error
        self.deleted = []
        self.extended = []

    async def receive(self, **_kwargs):
        messages, self.messages = self.messages, []
        return messages

    async def delete(self, receipt_handle):
        if self.delete_error:
            raise self.delete_error
        self.deleted.append(receipt_handle)

    async def extend_visibility(self, receipt_handle, **_kwargs):
        self.extended.append(receipt_handle)

    async def close(self):
        pass


def received(value=None):
    return ReceivedMessage("receipt", json.dumps(value or envelope()), 3, "sqs-id")


def consumer(transport, processor, validator=None, heartbeat=0.5):
    return InboxConsumer(
        consumer_name="recursos.commands.v1",
        allowed_message_types=frozenset({"ReserveItem"}),
        validator=validator or Validator(),
        processor=processor,
        transport=transport,
        policy=ConsumerPolicy(
            batch_size=10,
            concurrency=2,
            wait_time_seconds=0,
            visibility_timeout_seconds=1,
            heartbeat_seconds=heartbeat,
        ),
    )


async def test_ack_somente_depois_do_processamento_bem_sucedido():
    transport = Transport([received()])
    processor = Processor()
    assert await consumer(transport, processor).run_once() == 1
    assert processor.calls[0][1] == 3
    assert transport.deleted == ["receipt"]


@pytest.mark.parametrize(
    ("processor", "validator"),
    [
        (Processor(error=RuntimeError("db")), Validator()),
        (Processor(), Validator(ValueError("schema"))),
        (Processor(ProcessingOutcome.CONFLICT), Validator()),
    ],
)
async def test_falha_conflito_ou_contrato_invalido_nao_faz_ack(processor, validator):
    transport = Transport([received()])
    await consumer(transport, processor, validator).run_once()
    assert transport.deleted == []


async def test_tipo_de_outra_fila_nao_e_processado():
    processor = Processor()
    transport = Transport([received(envelope("ResourceReserved"))])
    await consumer(transport, processor).run_once()
    assert processor.calls == [] and transport.deleted == []


async def test_falha_de_ack_deixa_reentrega_para_inbox_deduplicar():
    transport = Transport([received()], delete_error=RuntimeError("indeterminado"))
    await consumer(transport, Processor(ProcessingOutcome.DUPLICATE)).run_once()
    assert transport.deleted == []


async def test_renova_visibilidade_durante_processamento_longo():
    transport = Transport([received()])
    await consumer(transport, Processor(delay=0.04), heartbeat=0.01).run_once()
    assert transport.extended and transport.deleted == ["receipt"]


def test_politica_rejeita_heartbeat_sem_margem():
    with pytest.raises(ValueError, match="heartbeat"):
        ConsumerPolicy(visibility_timeout_seconds=10, heartbeat_seconds=10)
