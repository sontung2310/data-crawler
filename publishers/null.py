"""No-op publisher used until SQS is fully configured."""
from __future__ import annotations

from typing import Any, Dict


class NullPublisher:
    def publish_post(self, event: Dict[str, Any]) -> None:
        return None

    def publish_comment(self, event: Dict[str, Any]) -> None:
        return None

    def publish_influencer(self, event: Dict[str, Any]) -> None:
        return None
