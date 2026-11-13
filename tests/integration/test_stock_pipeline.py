"""Fluxo funcional real: ElasticMQ -> Inbox/estoque/Outbox -> ElasticMQ."""

import json
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import boto3
import pytest
from botocore.config import Config
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlmodel.ext.asyncio.session import AsyncSession

from app.application.inbox_consumer import ConsumerPolicy, InboxConsumer
from app.application.outbox_publisher import (
    OutboxPublisher,
    PublisherPolicy,
    StaticRecursosMessageRouter,
)
from app.consumer import ALLOWED_MESSAGE_TYPES, CONSUMER_NAME
from app.infrastructure.messaging.contracts import JsonSchemaMessageValidator
from app.infrastructure.messaging.inbox import InboxMessageModel, PostgresInboxProcessor
from app.infrastructure.messaging.outbox_store import PostgresPublisherOutboxStore
from app.infrastructure.messaging.sqs import SqsFifoTransport
from app.infrastructure.messaging.sqs_consumer import SqsConsumerTransport
from app.modules.estoque.application.stock_command_handler import StockCommandHandler
from app.modules.estoque.infrastructure.db.models.item_estoque import ItemEstoqueModel
from app.modules.estoque.infrastructure.db.models.reserva import ReservaModel
from app.shared.infra.db.outbox_model import OutboxMessageModel

ENDPOINT = os.environ["SQS_ENDPOINT_URL"]
COMMAND_QUEUE = os.environ["SQS_ORDENS_TO_RECURSOS_URL"]
RESULT_QUEUE = os.environ["SQS_RECURSOS_TO_ORDENS_URL"]
SCHEMA = "contracts/vendor/ordens/saga-stock.v1.schema.json"


def factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def drain(client, queue_url: str) -> None:
    while True:
        messages = client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            VisibilityTimeout=0,
        ).get("Messages", [])
        if not messages:
            return
        for message in messages:
            client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message["ReceiptHandle"],
            )


@pytest.fixture
def pipeline_sqs_client():
    client = boto3.client(
        "sqs",
        endpoint_url=ENDPOINT,
        region_name="us-east-1",
        config=Config(retries={"max_attempts": 1}),
    )
    drain(client, COMMAND_QUEUE)
    drain(client, RESULT_QUEUE)
    yield client
    drain(client, COMMAND_QUEUE)
    drain(client, RESULT_QUEUE)
    client.close()


async def test_reserva_e_publica_resultado_com_postgresql_e_elasticmq_reais(
    db_engine: AsyncEngine,
    pipeline_sqs_client,
) -> None:
    resource_id = uuid4()
    operation_id = uuid4()
    message_id = uuid4()
    now = datetime.now(UTC)
    async with factory(db_engine)() as session:
        session.add(
            ItemEstoqueModel(
                id=resource_id,
                tipo="PECA",
                nome="Pastilha de freio",
                descricao="Eixo dianteiro",
                codigo=f"PF-{resource_id.hex[:8]}",
                quantidade_disponivel=5,
                quantidade_reservada=0,
                quantidade_minima=1,
                valor_unitario=Decimal("120.00"),
                ativo=True,
                criado_em=now,
                atualizado_em=now,
            )
        )
        await session.commit()

    command = {
        "message_id": str(message_id),
        "message_type": "ReserveItem",
        "message_version": 1,
        "occurred_at": now.isoformat(),
        "correlation_id": str(uuid4()),
        "causation_id": None,
        "operation_id": str(operation_id),
        "saga_id": str(uuid4()),
        "payload": {
            "order_id": str(uuid4()),
            "order_item_id": str(uuid4()),
            "resource_id": str(resource_id),
            "quantity": 2,
            "resource_snapshot": {
                "name": "Snapshot anterior",
                "description": None,
                "unit_price": "1.00",
            },
        },
    }
    pipeline_sqs_client.send_message(
        QueueUrl=COMMAND_QUEUE,
        MessageBody=json.dumps(command),
        MessageGroupId=str(operation_id),
        MessageDeduplicationId=str(message_id),
    )

    consumer_transport = SqsConsumerTransport(
        pipeline_sqs_client,
        queue_url=COMMAND_QUEUE,
    )
    consumer = InboxConsumer(
        consumer_name=CONSUMER_NAME,
        allowed_message_types=ALLOWED_MESSAGE_TYPES,
        validator=JsonSchemaMessageValidator(SCHEMA),
        processor=PostgresInboxProcessor(
            factory(db_engine),
            consumer_name=CONSUMER_NAME,
            handler=StockCommandHandler(),
        ),
        transport=consumer_transport,
        policy=ConsumerPolicy(
            batch_size=1,
            concurrency=1,
            wait_time_seconds=1,
            visibility_timeout_seconds=5,
            heartbeat_seconds=1,
        ),
    )
    assert await consumer.run_once() == 1

    publisher = OutboxPublisher(
        worker_id="stock-pipeline-test",
        store=PostgresPublisherOutboxStore(factory(db_engine)),
        router=StaticRecursosMessageRouter(RESULT_QUEUE),
        validator=JsonSchemaMessageValidator(SCHEMA),
        transport=SqsFifoTransport(pipeline_sqs_client, send_timeout_seconds=2),
        policy=PublisherPolicy(
            batch_size=1,
            lease_seconds=5,
            poll_interval_seconds=0.1,
            max_attempts=2,
            initial_backoff_seconds=0.1,
            max_backoff_seconds=1,
        ),
    )
    assert await publisher.run_once() == 1

    [transport_result] = pipeline_sqs_client.receive_message(
        QueueUrl=RESULT_QUEUE,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=1,
        AttributeNames=["All"],
    )["Messages"]
    result = json.loads(transport_result["Body"])
    JsonSchemaMessageValidator(SCHEMA).validate(result)
    assert result["message_type"] == "ResourceReserved"
    assert result["operation_id"] == str(operation_id)
    assert result["causation_id"] == str(message_id)
    assert result["payload"]["resource_snapshot"]["name"] == "Pastilha de freio"
    assert transport_result["Attributes"]["MessageGroupId"] == str(operation_id)

    async with factory(db_engine)() as session:
        item = await session.get(ItemEstoqueModel, resource_id)
        reservation = await session.get(ReservaModel, operation_id)
        inbox = await session.get(InboxMessageModel, (CONSUMER_NAME, message_id))
        outbox = await session.get(OutboxMessageModel, UUID(result["message_id"]))
    assert (item.quantidade_disponivel, item.quantidade_reservada) == (3, 2)
    assert reservation.status == "RESERVADA"
    assert inbox.status == "PROCESSED"
    assert outbox.published_at is not None
    assert outbox.published_at <= datetime.now(UTC) + timedelta(seconds=1)
