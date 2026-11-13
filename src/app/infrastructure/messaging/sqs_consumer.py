"""Adapter boto3 de consumo SQS/ElasticMQ."""

import asyncio
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from app.application.inbox_consumer import ReceivedMessage


class SqsConsumerTransportError(RuntimeError):
    pass


class SqsConsumerTransport:
    def __init__(self, client: Any, *, queue_url: str) -> None:
        if not queue_url.strip():
            raise ValueError("queue_url deve ser informada")
        self.client = client
        self.queue_url = queue_url

    async def receive(
        self,
        *,
        max_messages: int,
        wait_time_seconds: int,
        visibility_timeout_seconds: int,
    ) -> list[ReceivedMessage]:
        try:
            response = await asyncio.to_thread(
                self.client.receive_message,
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time_seconds,
                VisibilityTimeout=visibility_timeout_seconds,
                AttributeNames=["ApproximateReceiveCount"],
            )
        except (BotoCoreError, ClientError) as exc:
            raise SqsConsumerTransportError(type(exc).__name__) from exc
        return [
            ReceivedMessage(
                receipt_handle=item["ReceiptHandle"],
                body=item["Body"],
                receive_count=int(
                    item.get("Attributes", {}).get("ApproximateReceiveCount", "1")
                ),
                transport_message_id=item.get("MessageId"),
            )
            for item in response.get("Messages", [])
        ]

    async def delete(self, receipt_handle: str) -> None:
        try:
            response = await asyncio.to_thread(
                self.client.delete_message,
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle,
            )
        except (BotoCoreError, ClientError) as exc:
            raise SqsConsumerTransportError(type(exc).__name__) from exc
        if response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 200:
            raise SqsConsumerTransportError("delete sem confirmacao positiva")

    async def extend_visibility(
        self, receipt_handle: str, *, visibility_timeout_seconds: int
    ) -> None:
        try:
            response = await asyncio.to_thread(
                self.client.change_message_visibility,
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=visibility_timeout_seconds,
            )
        except (BotoCoreError, ClientError) as exc:
            raise SqsConsumerTransportError(type(exc).__name__) from exc
        if response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 200:
            raise SqsConsumerTransportError("visibility sem confirmacao positiva")

    async def close(self) -> None:
        await asyncio.to_thread(self.client.close)
