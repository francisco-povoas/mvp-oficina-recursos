"""Consumer contra PostgreSQL 16 e ElasticMQ reais."""

import asyncio
import json
import os
from uuid import uuid4

import boto3
import pytest
import sqlalchemy as sa
from botocore.config import Config
from botocore.exceptions import ClientError
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlmodel.ext.asyncio.session import AsyncSession

from app.application.inbox_consumer import ConsumerPolicy, InboxConsumer
from app.consumer import ALLOWED_MESSAGE_TYPES, CONSUMER_NAME
from app.infrastructure.messaging.contracts import JsonSchemaMessageValidator
from app.infrastructure.messaging.inbox import PostgresInboxProcessor
from app.infrastructure.messaging.sqs_consumer import SqsConsumerTransport

ENDPOINT = os.environ["SQS_ENDPOINT_URL"]
QUEUE_URL = os.environ["SQS_ORDENS_TO_RECURSOS_URL"]
QUEUE_NAME = "mvp-oficina-ordens-to-recursos-local.fifo"
DLQ_NAME = "mvp-oficina-ordens-to-recursos-local-dlq.fifo"


def envelope():
    operation_id = str(uuid4())
    return {
        "message_id": str(uuid4()),
        "message_type": "ReserveItem",
        "message_version": 1,
        "occurred_at": "2026-11-09T12:00:00Z",
        "correlation_id": str(uuid4()),
        "causation_id": str(uuid4()),
        "operation_id": operation_id,
        "saga_id": str(uuid4()),
        "payload": {
            "order_id": str(uuid4()),
            "order_item_id": str(uuid4()),
            "resource_id": str(uuid4()),
            "quantity": 2,
            "resource_snapshot": {"name": "Filtro", "description": None, "unit_price": "20.00"},
        },
    }


def ensure_queue(client, name, attributes):
    try:
        return client.get_queue_url(QueueName=name)["QueueUrl"]
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") not in {
            "AWS.SimpleQueueService.NonExistentQueue",
            "QueueDoesNotExist",
        }:
            raise
    return client.create_queue(QueueName=name, Attributes=attributes)["QueueUrl"]


def drain(client, queue_url):
    while True:
        messages = client.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=10, VisibilityTimeout=0
        ).get("Messages", [])
        if not messages:
            return
        for message in messages:
            client.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])


@pytest.fixture
def command_queue():
    client = boto3.client(
        "sqs", endpoint_url=ENDPOINT, region_name="us-east-1", config=Config(retries={"max_attempts": 1})
    )
    dlq_url = ensure_queue(
        client, DLQ_NAME, {"FifoQueue": "true", "ContentBasedDeduplication": "false"}
    )
    dlq_arn = client.get_queue_attributes(
        QueueUrl=dlq_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    queue_url = ensure_queue(
        client,
        QUEUE_NAME,
        {
            "FifoQueue": "true",
            "ContentBasedDeduplication": "false",
            "VisibilityTimeout": "1",
            "RedrivePolicy": json.dumps(
                {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "5"}
            ),
        },
    )
    drain(client, queue_url)
    drain(client, dlq_url)
    yield client, queue_url, dlq_url
    drain(client, queue_url)
    drain(client, dlq_url)
    client.close()


@pytest.fixture
async def transport_probe(db_engine):
    async with db_engine.begin() as connection:
        await connection.execute(sa.text("CREATE TABLE consumer_transport_probe (id UUID PRIMARY KEY)"))
    yield
    async with db_engine.begin() as connection:
        await connection.execute(sa.text("DROP TABLE consumer_transport_probe"))


class Handler:
    def __init__(self):
        self.calls = 0

    async def handle(self, value, session):
        self.calls += 1
        await session.exec(
            sa.text("INSERT INTO consumer_transport_probe (id) VALUES (:id)"),
            params={"id": value["operation_id"]},
        )


def build_consumer(db_engine, client, handler):
    return InboxConsumer(
        consumer_name=CONSUMER_NAME,
        allowed_message_types=ALLOWED_MESSAGE_TYPES,
        validator=JsonSchemaMessageValidator("contracts/vendor/ordens/saga-stock.v1.schema.json"),
        processor=PostgresInboxProcessor(
            async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False),
            consumer_name=CONSUMER_NAME,
            handler=handler,
        ),
        transport=SqsConsumerTransport(client, queue_url=QUEUE_URL),
        policy=ConsumerPolicy(
            batch_size=10,
            concurrency=2,
            wait_time_seconds=0,
            visibility_timeout_seconds=1,
            heartbeat_seconds=0.2,
        ),
    )


async def test_recebe_commit_ack_e_reentrega_nao_repete_efeito(
    db_engine, transport_probe, command_queue
):
    client, queue_url, _ = command_queue
    value, handler = envelope(), Handler()
    client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(value),
        MessageGroupId=value["operation_id"],
        MessageDeduplicationId=value["message_id"],
    )
    consumer = build_consumer(db_engine, client, handler)
    # Commit local sem ACK simula resultado indeterminado da remocao.
    assert (await consumer.processor.process(value, receive_count=1)).value == "PROCESSED"
    assert await consumer.run_once() == 1
    assert handler.calls == 1
    assert not client.receive_message(QueueUrl=queue_url).get("Messages")


async def test_contrato_invalido_nao_faz_ack_e_redrive_para_dlq(
    db_engine, transport_probe, command_queue
):
    client, queue_url, dlq_url = command_queue
    body = '{"message_type":"ReserveItem"}'
    client.send_message(
        QueueUrl=queue_url,
        MessageBody=body,
        MessageGroupId=str(uuid4()),
        MessageDeduplicationId=str(uuid4()),
    )
    consumer = build_consumer(db_engine, client, Handler())
    assert await consumer.run_once() == 1
    await asyncio.sleep(1.1)
    for _ in range(10):
        messages = client.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=1, VisibilityTimeout=0
        ).get("Messages", [])
        if not messages:
            break
    dead_letters = client.receive_message(
        QueueUrl=dlq_url, MaxNumberOfMessages=1, VisibilityTimeout=0
    ).get("Messages", [])
    assert dead_letters and dead_letters[0]["Body"] == body
