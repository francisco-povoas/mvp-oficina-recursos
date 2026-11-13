"""Caso de uso independente que publica a Outbox com semantica at-least-once."""

import asyncio
import json
import logging
import random
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from app.application.messaging import (
    MessageContractError,
    MessageContractValidator,
    MessagePublicationError,
    MessageRoute,
    MessageRouter,
    MessageTransport,
    OutboxStore,
    UnknownMessageRouteError,
)
from app.shared.outbox import ClaimedOutboxMessage, OutboxClaimConflictError


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class PublisherPolicy:
    batch_size: int = 10
    lease_seconds: float = 30.0
    poll_interval_seconds: float = 1.0
    max_attempts: int = 8
    initial_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 60.0

    def __post_init__(self) -> None:
        if self.batch_size <= 0:
            raise ValueError("publisher batch_size deve ser positivo")
        if self.lease_seconds <= 0:
            raise ValueError("publisher lease_seconds deve ser positivo")
        if self.poll_interval_seconds <= 0:
            raise ValueError("publisher poll_interval_seconds deve ser positivo")
        if self.max_attempts <= 0:
            raise ValueError("publisher max_attempts deve ser positivo")
        if self.initial_backoff_seconds <= 0:
            raise ValueError("publisher initial_backoff_seconds deve ser positivo")
        if self.max_backoff_seconds < self.initial_backoff_seconds:
            raise ValueError("publisher max_backoff_seconds deve ser >= initial_backoff_seconds")


def build_envelope(message: ClaimedOutboxMessage) -> dict[str, object]:
    suffix = f".v{message.event_version}"
    if not message.event_type.endswith(suffix):
        raise MessageContractError("tipo e versao persistidos sao incompatíveis")
    message_type = message.event_type[: -len(suffix)]
    if not message_type:
        raise MessageContractError("tipo de mensagem vazio")
    return {
        "message_id": str(message.message_id),
        "message_type": message_type,
        "message_version": message.event_version,
        "occurred_at": message.occurred_at.isoformat().replace("+00:00", "Z"),
        "correlation_id": str(message.correlation_id),
        "causation_id": str(message.causation_id) if message.causation_id else None,
        "operation_id": str(message.aggregate_id),
        "saga_id": str(message.saga_id) if message.saga_id else None,
        "payload": message.payload,
    }


class OutboxPublisher:
    def __init__(
        self,
        *,
        worker_id: str,
        store: OutboxStore,
        router: MessageRouter,
        validator: MessageContractValidator,
        transport: MessageTransport,
        policy: PublisherPolicy,
        clock: Callable[[], datetime] = utc_now,
        jitter: Callable[[float, float], float] = random.uniform,
        logger: logging.Logger | None = None,
    ) -> None:
        if not worker_id.strip():
            raise ValueError("worker_id nao pode ser vazio")
        self.worker_id = worker_id
        self.store = store
        self.router = router
        self.validator = validator
        self.transport = transport
        self.policy = policy
        self.clock = clock
        self.jitter = jitter
        self.logger = logger or logging.getLogger(__name__)

    async def run_once(self) -> int:
        claimed_at = self.clock()
        messages = await self.store.claim_pending(
            worker_id=self.worker_id,
            batch_size=self.policy.batch_size,
            lease_duration=timedelta(seconds=self.policy.lease_seconds),
            max_attempts=self.policy.max_attempts,
            now=claimed_at,
        )
        for message in messages:
            await self._publish_one(message)
        return len(messages)

    async def run(self, stop_event: asyncio.Event) -> None:
        self.logger.info("outbox_publisher_started worker_id=%s", self.worker_id)
        try:
            while not stop_event.is_set():
                try:
                    claimed = await self.run_once()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.logger.error(
                        "outbox_claim_failed worker_id=%s error_type=%s",
                        self.worker_id,
                        type(exc).__name__,
                    )
                    claimed = 0
                if claimed == 0:
                    try:
                        await asyncio.wait_for(
                            stop_event.wait(),
                            timeout=self.policy.poll_interval_seconds,
                        )
                    except TimeoutError:
                        pass
        finally:
            self.logger.info("outbox_publisher_stopped worker_id=%s", self.worker_id)

    async def _publish_one(self, message: ClaimedOutboxMessage) -> None:
        try:
            envelope = build_envelope(message)
            route = self.router.resolve(message)
            self.validator.validate(envelope)
            body = json.dumps(
                envelope,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            transport_message_id = await self.transport.publish(
                route=route,
                body=body,
                deduplication_id=str(message.message_id),
            )
        except asyncio.CancelledError:
            raise
        except (MessageContractError, UnknownMessageRouteError, MessagePublicationError) as exc:
            await self._record_failure(message, exc)
            return
        except Exception as exc:
            await self._record_failure(message, exc)
            return

        try:
            await self.store.mark_published(
                message_id=message.message_id,
                claim_id=message.claim_id,
                published_at=self.clock(),
            )
        except Exception as exc:
            self.logger.error(
                "outbox_confirmation_failed message_id=%s correlation_id=%s "
                "operation_id=%s claim_id=%s error_type=%s",
                message.message_id,
                message.correlation_id,
                message.aggregate_id,
                message.claim_id,
                type(exc).__name__,
            )
            return

        self.logger.info(
            "outbox_published message_id=%s correlation_id=%s operation_id=%s "
            "transport_message_id=%s attempt=%s",
            message.message_id,
            message.correlation_id,
            message.aggregate_id,
            transport_message_id,
            message.attempts,
        )

    async def _record_failure(self, message: ClaimedOutboxMessage, exc: Exception) -> None:
        failed_at = self.clock()
        next_attempt_at = failed_at + timedelta(seconds=self._retry_delay(message.attempts))
        safe_error = self._safe_error(exc)
        try:
            await self.store.mark_failed(
                message_id=message.message_id,
                claim_id=message.claim_id,
                error=safe_error,
                next_attempt_at=next_attempt_at,
                failed_at=failed_at,
            )
        except OutboxClaimConflictError:
            self.logger.warning(
                "outbox_failure_not_recorded_stale_claim message_id=%s claim_id=%s",
                message.message_id,
                message.claim_id,
            )
            return
        except Exception as store_exc:
            self.logger.error(
                "outbox_failure_persistence_failed message_id=%s claim_id=%s error_type=%s",
                message.message_id,
                message.claim_id,
                type(store_exc).__name__,
            )
            return
        self.logger.warning(
            "outbox_publish_failed message_id=%s correlation_id=%s operation_id=%s "
            "attempt=%s exhausted=%s retry_at=%s error=%s",
            message.message_id,
            message.correlation_id,
            message.aggregate_id,
            message.attempts,
            message.attempts >= self.policy.max_attempts,
            next_attempt_at.isoformat(),
            safe_error,
        )

    def _retry_delay(self, attempts: int) -> float:
        cap = min(
            self.policy.max_backoff_seconds,
            self.policy.initial_backoff_seconds * (2 ** max(attempts - 1, 0)),
        )
        return self.jitter(0.0, cap)

    @staticmethod
    def _safe_error(exc: Exception) -> str:
        if isinstance(
            exc,
            (MessageContractError, UnknownMessageRouteError, MessagePublicationError),
        ):
            return f"{type(exc).__name__}: {str(exc).strip()}"[:1000]
        return type(exc).__name__


class StaticRecursosMessageRouter:
    _ROUTED_TYPES = frozenset(
        {
            "ResourceReserved.v1",
            "ItemBackordered.v1",
            "ReservationRejected.v1",
            "ReservationReleased.v1",
            "BackorderedItemReceived.v1",
            "ReservationConsumed.v1",
        }
    )

    def __init__(self, queue_url: str) -> None:
        if not queue_url.strip():
            raise ValueError("SQS_RECURSOS_TO_ORDENS_URL deve ser informado")
        self.queue_url = queue_url

    def resolve(self, message: ClaimedOutboxMessage) -> MessageRoute:
        if message.event_type not in self._ROUTED_TYPES or message.event_version != 1:
            raise UnknownMessageRouteError(
                f"rota ausente para {message.event_type} (versao {message.event_version})"
            )
        return MessageRoute(queue_url=self.queue_url, group_id=str(message.aggregate_id))
