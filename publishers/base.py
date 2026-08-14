"""Event publisher factory."""
from __future__ import annotations

import threading
from typing import Any, Dict, Optional, Protocol

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_REGION,
    AWS_SECRET_ACCESS_KEY,
    AWS_SQS_QUEUE_URL,
)


class EventPublisher(Protocol):
    def publish_post(self, event: Dict[str, Any]) -> None: ...

    def publish_comment(self, event: Dict[str, Any]) -> None: ...

    def publish_influencer(self, event: Dict[str, Any]) -> None: ...


_publisher: Optional[EventPublisher] = None
_lock = threading.Lock()


def sqs_fully_configured() -> bool:
    return bool(
        AWS_SQS_QUEUE_URL
        and AWS_ACCESS_KEY_ID
        and AWS_SECRET_ACCESS_KEY
        and AWS_REGION
    )


def get_publisher() -> EventPublisher:
    """Return a process-wide publisher (one boto3 client when SQS is configured)."""
    global _publisher
    if _publisher is not None:
        return _publisher
    with _lock:
        if _publisher is None:
            if sqs_fully_configured():
                from publishers.sqs import SqsPublisher

                _publisher = SqsPublisher()
            else:
                from publishers.null import NullPublisher

                _publisher = NullPublisher()
        return _publisher


def reset_publisher_for_tests() -> None:
    """Clear cached publisher (unit tests only)."""
    global _publisher
    with _lock:
        _publisher = None
