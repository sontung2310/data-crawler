"""AWS SQS publisher (only constructed when fully configured)."""
from __future__ import annotations

import json
import logging
from typing import Any, Dict

import boto3

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_REGION,
    AWS_SECRET_ACCESS_KEY,
    AWS_SQS_QUEUE_URL,
)

logger = logging.getLogger(__name__)


class SqsPublisher:
    def __init__(self) -> None:
        self._client = boto3.client(
            "sqs",
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        self._queue_url = AWS_SQS_QUEUE_URL

    def _send(self, event: Dict[str, Any]) -> None:
        self._client.send_message(
            QueueUrl=self._queue_url,
            MessageBody=json.dumps(event, default=str),
        )

    def publish_post(self, event: Dict[str, Any]) -> None:
        self._send(event)

    def publish_comment(self, event: Dict[str, Any]) -> None:
        self._send(event)

    def publish_influencer(self, event: Dict[str, Any]) -> None:
        self._send(event)
