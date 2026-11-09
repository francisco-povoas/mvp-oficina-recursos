"""Composition root do consumer independente de comandos de Ordens."""

import asyncio
import logging
import signal

from app.application.inbox_consumer import ConsumerPolicy, InboxConsumer
from app.config import Settings, get_settings
from app.infrastructure.messaging.contracts import JsonSchemaMessageValidator
from app.infrastructure.messaging.inbox import PostgresInboxProcessor, TransactionalMessageHandler
from app.infrastructure.messaging.sqs import create_sqs_client
from app.infrastructure.messaging.sqs_consumer import SqsConsumerTransport
from app.shared.infra.db import dispose_engine, get_session_factory

CONSUMER_NAME = "recursos.commands.v1"
ALLOWED_MESSAGE_TYPES = frozenset(
    {"ReserveItem", "ReleaseReservation", "ReceiveBackorderedItem", "ConsumeReservation"}
)


async def run_consumer(
    settings: Settings | None = None,
    *,
    handler: TransactionalMessageHandler | None = None,
) -> None:
    resolved = settings or get_settings()
    resolved.validate_consumer_runtime()
    logging.basicConfig(
        level=getattr(logging, resolved.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger = logging.getLogger("app.consumer")
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_shutdown() -> None:
        logger.info("inbox_consumer_shutdown_requested")
        stop_event.set()

    for signal_number in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(signal_number, request_shutdown)
        except NotImplementedError:  # pragma: no cover
            signal.signal(signal_number, lambda *_: request_shutdown())

    # O Checkpoint D entrega o worker e o port transacional, mas nao antecipa handlers
    # do Checkpoint E. Sem handler, o processo permanece saudavel e nao drena a fila.
    if handler is None:
        logger.warning("inbox_consumer_paused_business_handlers_not_installed")
        await stop_event.wait()
        await dispose_engine()
        return

    client = create_sqs_client(
        region=resolved.aws_region,
        endpoint_url=resolved.sqs_endpoint_url,
        connect_timeout_seconds=resolved.sqs_connect_timeout_seconds,
        read_timeout_seconds=resolved.sqs_read_timeout_seconds,
    )
    transport = SqsConsumerTransport(client, queue_url=resolved.sqs_ordens_to_recursos_url)
    consumer = InboxConsumer(
        consumer_name=CONSUMER_NAME,
        allowed_message_types=ALLOWED_MESSAGE_TYPES,
        validator=JsonSchemaMessageValidator(resolved.consumer_contract_schema_path),
        processor=PostgresInboxProcessor(
            get_session_factory(),
            consumer_name=CONSUMER_NAME,
            handler=handler,
        ),
        transport=transport,
        policy=ConsumerPolicy(
            batch_size=resolved.consumer_batch_size,
            concurrency=resolved.consumer_concurrency,
            wait_time_seconds=resolved.consumer_wait_time_seconds,
            visibility_timeout_seconds=resolved.consumer_visibility_timeout_seconds,
            heartbeat_seconds=resolved.consumer_heartbeat_seconds,
        ),
        logger=logger,
    )
    try:
        await consumer.run(stop_event)
    finally:
        await transport.close()
        await dispose_engine()


def main() -> None:
    asyncio.run(run_consumer())


if __name__ == "__main__":
    main()
