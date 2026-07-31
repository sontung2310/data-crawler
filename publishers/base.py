"""Event publisher factory."""
from __future__ import annotations

from typing import Any, Dict, Protocol

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_REGION,
    AWS_SECRET_ACCESS_KEY,
    AWS_SQS_QUEUE_URL,
)


class EventPublisher(Protocol):
    def publish_post(self, event: Dict[str, Any]) -> None: ...

    def publish_comment(self, event: Dict[str, Any]) -> None: ...


def sqs_fully_configured() -> bool:
    return bool(
        AWS_SQS_QUEUE_URL
        and AWS_ACCESS_KEY_ID
        and AWS_SECRET_ACCESS_KEY
        and AWS_REGION
    )


def get_publisher() -> EventPublisher:
    if sqs_fully_configured():
        from publishers.sqs import SqsPublisher

        return SqsPublisher()
    from publishers.null import NullPublisher

    return NullPublisher()
