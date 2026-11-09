"""Composition root do processo independente de publicação da Outbox."""

import asyncio
import logging
import os
import signal
import socket
from uuid import uuid4

from app.application.outbox_publisher import (
    OutboxPublisher,
    PublisherPolicy,
    StaticRecursosMessageRouter,
)
from app.config import Settings, get_settings
from app.infrastructure.messaging.contracts import JsonSchemaMessageValidator
from app.infrastructure.messaging.outbox_store import PostgresPublisherOutboxStore
from app.infrastructure.messaging.sqs import SqsFifoTransport, create_sqs_client
from app.shared.infra.db import dispose_engine, get_session_factory


def _worker_id(settings: Settings) -> str:
    if settings.publisher_worker_id.strip():
        return settings.publisher_worker_id.strip()
    return f"{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:8]}"


async def run_publisher(settings: Settings | None = None) -> None:
    resolved = settings or get_settings()
    resolved.validate_publisher_runtime()
    logging.basicConfig(
        level=getattr(logging, resolved.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger = logging.getLogger("app.publisher")
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_shutdown() -> None:
        logger.info("outbox_publisher_shutdown_requested")
        stop_event.set()

    for signal_number in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(signal_number, request_shutdown)
        except NotImplementedError:  # pragma: no cover
            signal.signal(signal_number, lambda *_: request_shutdown())

    client = create_sqs_client(
        region=resolved.aws_region,
        endpoint_url=resolved.sqs_endpoint_url,
        connect_timeout_seconds=resolved.sqs_connect_timeout_seconds,
        read_timeout_seconds=resolved.sqs_read_timeout_seconds,
    )
    transport = SqsFifoTransport(client, send_timeout_seconds=resolved.sqs_send_timeout_seconds)
    publisher = OutboxPublisher(
        worker_id=_worker_id(resolved),
        store=PostgresPublisherOutboxStore(get_session_factory()),
        router=StaticRecursosMessageRouter(resolved.sqs_recursos_to_ordens_url),
        validator=JsonSchemaMessageValidator(resolved.publisher_contract_schema_path),
        transport=transport,
        policy=PublisherPolicy(
            batch_size=resolved.publisher_batch_size,
            lease_seconds=resolved.publisher_lease_seconds,
            poll_interval_seconds=resolved.publisher_poll_interval_seconds,
            max_attempts=resolved.publisher_max_attempts,
            initial_backoff_seconds=resolved.publisher_initial_backoff_seconds,
            max_backoff_seconds=resolved.publisher_max_backoff_seconds,
        ),
        logger=logger,
    )
    try:
        await publisher.run(stop_event)
    finally:
        await transport.close()
        await dispose_engine()


def main() -> None:
    asyncio.run(run_publisher())


if __name__ == "__main__":
    main()
