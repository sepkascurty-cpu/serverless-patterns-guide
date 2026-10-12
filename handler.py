"""
Idempotent SQS-triggered Lambda: order payment processor.

Patterns demonstrated:
  1. Idempotency via DynamoDB conditional write (safe under at-least-once delivery)
  2. Partial batch responses so one poison message never nukes the batch
  3. Structured logging with the request/message context attached

Deploy: zip this file, set env vars TABLE_NAME and AWS region,
wire the Lambda to the SQS queue with ReportBatchItemFailures enabled.
"""

import json
import logging
import os
import time

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ.get("TABLE_NAME", "order-idempotency")
table = dynamodb.Table(TABLE_NAME)


def claim_idempotency(order_id: str, ttl_hours: int = 24) -> bool:
    """Return True if this is the first time we see order_id, else False.

    Uses a conditional PutItem: the write succeeds only when the key
    does not already exist, which makes the claim atomic even when two
    concurrent invocations race on a redelivered message.
    """
    try:
        table.put_item(
            Item={
                "order_id": order_id,
                "claimed_at": int(time.time()),
                # auto-expire the record so the table never grows unbounded
                "ttl": int(time.time()) + ttl_hours * 3600,
            },
            ConditionExpression="attribute_not_exists(order_id)",
        )
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.info("duplicate delivery skipped", extra={"order_id": order_id})
            return False
        raise


def process_order(order: dict) -> None:
    """Business logic goes here: charge payment, emit order.paid, etc."""
    logger.info(
        "processing order",
        extra={"order_id": order["order_id"], "amount": order.get("amount")},
    )
    # ... call payment provider, publish to EventBridge ...


def lambda_handler(event, context):  # noqa: ANN001, ANN201 - Lambda signature
    """SQS event source mapping entrypoint with partial batch responses."""
    batch_failures = []

    for record in event.get("Records", []):
        message_id = record.get("messageId", "unknown")
        try:
            order = json.loads(record["body"])
            order_id = order["order_id"]
        except (json.JSONDecodeError, KeyError) as exc:
            # Malformed payload: never retry, it will never parse.
            # Send to DLQ via the queue's redrive policy instead of failing.
            logger.error("unparseable message", extra={"message_id": message_id})
            raise exc

        if not claim_idempotency(order_id):
            continue  # duplicate delivery, already processed

        try:
            process_order(order)
        except Exception:
            # Transient failure: report ONLY this message so SQS retries it
            # while the rest of the batch is deleted as successfully processed.
            logger.exception("order failed, will retry", extra={"order_id": order_id})
            batch_failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": batch_failures}
