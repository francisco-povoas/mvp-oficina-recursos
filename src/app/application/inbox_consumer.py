"""Consumer SQS tecnico com ACK somente depois do commit local."""

import asyncio
import json
import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import Mapping, Protocol


class MessageContractError(ValueError):
    """A mensagem nao atende ao contrato versionado da fila."""


class BusinessHandlerUnavailableError(RuntimeError):
    """O handler funcional pertence ao Checkpoint E."""


class ProcessingOutcome(StrEnum):
    PROCESSED = "PROCESSED"
    DUPLICATE = "DUPLICATE"
    CONFLICT = "CONFLICT"


@dataclass(frozen=True, slots=True)
class ReceivedMessage:
    receipt_handle: str
    body: str
    receive_count: int
    transport_message_id: str | None = None


class MessageContractValidator(Protocol):
    def validate(self, envelope: Mapping[str, object]) -> None: ...


class IncomingMessageProcessor(Protocol):
    async def process(
        self, envelope: Mapping[str, object], *, receive_count: int
    ) -> ProcessingOutcome: ...


class ConsumerTransport(Protocol):
    async def receive(
        self,
        *,
        max_messages: int,
        wait_time_seconds: int,
        visibility_timeout_seconds: int,
    ) -> list[ReceivedMessage]: ...

    async def delete(self, receipt_handle: str) -> None: ...

    async def extend_visibility(
        self, receipt_handle: str, *, visibility_timeout_seconds: int
    ) -> None: ...

    async def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class ConsumerPolicy:
    batch_size: int = 10
    concurrency: int = 4
    wait_time_seconds: int = 20
    visibility_timeout_seconds: int = 60
    heartbeat_seconds: float = 20.0

    def __post_init__(self) -> None:
        if not 1 <= self.batch_size <= 10:
            raise ValueError("consumer batch_size deve estar entre 1 e 10")
        if self.concurrency <= 0:
            raise ValueError("consumer concurrency deve ser positiva")
        if not 0 <= self.wait_time_seconds <= 20:
            raise ValueError("consumer wait_time_seconds deve estar entre 0 e 20")
        if self.visibility_timeout_seconds <= 0 or self.heartbeat_seconds <= 0:
            raise ValueError("visibility timeout e heartbeat devem ser positivos")
        if self.heartbeat_seconds >= self.visibility_timeout_seconds:
            raise ValueError("heartbeat deve ser menor que o visibility timeout")


class InboxConsumer:
    def __init__(
        self,
        *,
        consumer_name: str,
        allowed_message_types: frozenset[str],
        validator: MessageContractValidator,
        processor: IncomingMessageProcessor,
        transport: ConsumerTransport,
        policy: ConsumerPolicy,
        logger: logging.Logger | None = None,
    ) -> None:
        if not consumer_name.strip() or not allowed_message_types:
            raise ValueError("consumer e tipos permitidos devem ser informados")
        self.consumer_name = consumer_name
        self.allowed_message_types = allowed_message_types
        self.validator = validator
        self.processor = processor
        self.transport = transport
        self.policy = policy
        self.logger = logger or logging.getLogger(__name__)

    async def run_once(self) -> int:
        messages = await self.transport.receive(
            max_messages=self.policy.batch_size,
            wait_time_seconds=self.policy.wait_time_seconds,
            visibility_timeout_seconds=self.policy.visibility_timeout_seconds,
        )
        semaphore = asyncio.Semaphore(self.policy.concurrency)

        async def bounded(message: ReceivedMessage) -> None:
            async with semaphore:
                await self._consume_one(message)

        await asyncio.gather(*(bounded(message) for message in messages))
        return len(messages)

    async def run(self, stop_event: asyncio.Event) -> None:
        self.logger.info("inbox_consumer_started consumer_name=%s", self.consumer_name)
        try:
            while not stop_event.is_set():
                try:
                    await self.run_once()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.logger.error(
                        "inbox_receive_failed consumer_name=%s error_type=%s",
                        self.consumer_name,
                        type(exc).__name__,
                    )
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=1.0)
                    except TimeoutError:
                        pass
        finally:
            self.logger.info("inbox_consumer_stopped consumer_name=%s", self.consumer_name)

    async def _consume_one(self, message: ReceivedMessage) -> None:
        try:
            envelope = self._decode(message.body)
        except MessageContractError as exc:
            self.logger.warning(
                "inbox_message_invalid consumer_name=%s transport_message_id=%s "
                "receive_count=%s error=%s",
                self.consumer_name,
                message.transport_message_id,
                message.receive_count,
                str(exc),
            )
            return
        heartbeat_stop = asyncio.Event()
        heartbeat = asyncio.create_task(self._heartbeat(message, heartbeat_stop))
        try:
            outcome = await self.processor.process(envelope, receive_count=message.receive_count)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.logger.warning(
                "inbox_processing_failed consumer_name=%s message_id=%s correlation_id=%s "
                "operation_id=%s receive_count=%s error_type=%s",
                self.consumer_name,
                envelope["message_id"],
                envelope["correlation_id"],
                envelope["operation_id"],
                message.receive_count,
                type(exc).__name__,
            )
            return
        finally:
            heartbeat_stop.set()
            await heartbeat
        if outcome is ProcessingOutcome.CONFLICT:
            self.logger.error(
                "inbox_idempotency_conflict consumer_name=%s message_id=%s "
                "correlation_id=%s operation_id=%s",
                self.consumer_name,
                envelope["message_id"],
                envelope["correlation_id"],
                envelope["operation_id"],
            )
            return
        try:
            await self.transport.delete(message.receipt_handle)
        except Exception as exc:
            self.logger.warning(
                "inbox_ack_failed consumer_name=%s message_id=%s correlation_id=%s "
                "operation_id=%s error_type=%s",
                self.consumer_name,
                envelope["message_id"],
                envelope["correlation_id"],
                envelope["operation_id"],
                type(exc).__name__,
            )
            return
        self.logger.info(
            "inbox_message_acknowledged consumer_name=%s message_id=%s correlation_id=%s "
            "operation_id=%s outcome=%s",
            self.consumer_name,
            envelope["message_id"],
            envelope["correlation_id"],
            envelope["operation_id"],
            outcome.value,
        )

    def _decode(self, body: str) -> Mapping[str, object]:
        try:
            value = json.loads(body)
        except (json.JSONDecodeError, TypeError) as exc:
            raise MessageContractError("corpo nao e JSON valido") from exc
        if not isinstance(value, dict):
            raise MessageContractError("envelope deve ser um objeto JSON")
        try:
            self.validator.validate(value)
        except Exception as exc:
            raise MessageContractError("JSON Schema rejeitou a mensagem") from exc
        if value.get("message_type") not in self.allowed_message_types:
            raise MessageContractError("tipo de mensagem nao pertence a esta fila")
        return value

    async def _heartbeat(self, message: ReceivedMessage, stop_event: asyncio.Event) -> None:
        while True:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.policy.heartbeat_seconds)
                return
            except TimeoutError:
                pass
            try:
                await self.transport.extend_visibility(
                    message.receipt_handle,
                    visibility_timeout_seconds=self.policy.visibility_timeout_seconds,
                )
            except Exception as exc:
                self.logger.warning(
                    "inbox_visibility_extension_failed consumer_name=%s "
                    "transport_message_id=%s error_type=%s",
                    self.consumer_name,
                    message.transport_message_id,
                    type(exc).__name__,
                )
