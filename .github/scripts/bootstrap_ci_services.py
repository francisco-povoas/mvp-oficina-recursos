"""Aguarda o ElasticMQ e cria, de forma idempotente, as filas usadas no CI."""

from __future__ import annotations

import json
import time

import boto3
from botocore.exceptions import BotoCoreError, ClientError

QUEUE_PAIRS = (
    (
        "mvp-oficina-ordens-to-recursos-local.fifo",
        "mvp-oficina-ordens-to-recursos-local-dlq.fifo",
    ),
    (
        "mvp-oficina-recursos-to-ordens-local.fifo",
        "mvp-oficina-recursos-to-ordens-local-dlq.fifo",
    ),
)


def main() -> None:
    client = boto3.client(
        "sqs", region_name="us-east-1", endpoint_url="http://127.0.0.1:9324"
    )
    last_error: Exception | None = None
    for _ in range(30):
        try:
            client.list_queues()
            break
        except (BotoCoreError, ClientError) as exc:
            last_error = exc
            time.sleep(1)
    else:
        raise RuntimeError("ElasticMQ não ficou saudável") from last_error

    expected: set[str] = set()
    for source_name, dlq_name in QUEUE_PAIRS:
        expected.update((source_name, dlq_name))
        dlq_url = client.create_queue(
            QueueName=dlq_name,
            Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "false"},
        )["QueueUrl"]
        dlq_arn = client.get_queue_attributes(
            QueueUrl=dlq_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        client.create_queue(
            QueueName=source_name,
            Attributes={
                "FifoQueue": "true",
                "ContentBasedDeduplication": "false",
                "VisibilityTimeout": "60",
                "RedrivePolicy": json.dumps(
                    {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "5"}
                ),
            },
        )

    observed = {
        url.rsplit("/", 1)[-1] for url in client.list_queues().get("QueueUrls", [])
    }
    missing = expected - observed
    if missing:
        raise RuntimeError(f"Filas ausentes após bootstrap: {sorted(missing)}")
    print(f"ElasticMQ saudável; {len(expected)} filas inicializadas")


if __name__ == "__main__":
    main()
