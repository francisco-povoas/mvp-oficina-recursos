"""Adapter boto3 para publicação em SQS FIFO ou ElasticMQ."""

import asyncio
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from app.application.messaging import (
    MessagePublicationError,
    MessagePublicationIndeterminateError,
    MessageRoute,
)


def create_sqs_client(
    *,
    region: str,
    endpoint_url: str | None,
    connect_timeout_seconds: float,
    read_timeout_seconds: float,
) -> Any:
    return boto3.client(
        "sqs",
        region_name=region,
        endpoint_url=endpoint_url,
        config=Config(
            connect_timeout=connect_timeout_seconds,
            read_timeout=read_timeout_seconds,
            retries={"max_attempts": 1, "mode": "standard"},
        ),
    )


class SqsFifoTransport:
    def __init__(self, client: Any, *, send_timeout_seconds: float) -> None:
        if send_timeout_seconds <= 0:
            raise ValueError("SQS_SEND_TIMEOUT_SECONDS deve ser positivo")
        self.client = client
        self.send_timeout_seconds = send_timeout_seconds

    async def publish(
        self,
        *,
        route: MessageRoute,
        body: str,
        deduplication_id: str,
    ) -> str:
        try:
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    self.client.send_message,
                    QueueUrl=route.queue_url,
                    MessageBody=body,
                    MessageGroupId=route.group_id,
                    MessageDeduplicationId=deduplication_id,
                ),
                timeout=self.send_timeout_seconds,
            )
        except TimeoutError as exc:
            raise MessagePublicationIndeterminateError("timeout sem resultado confirmado") from exc
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "ClientError")
            raise MessagePublicationError(f"SQS rejeitou a chamada ({code})") from exc
        except BotoCoreError as exc:
            raise MessagePublicationError(type(exc).__name__) from exc
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        transport_id = response.get("MessageId")
        if status != 200 or not isinstance(transport_id, str) or not transport_id:
            raise MessagePublicationIndeterminateError("resposta SQS sem confirmacao positiva")
        return transport_id

    async def close(self) -> None:
        await asyncio.to_thread(self.client.close)
