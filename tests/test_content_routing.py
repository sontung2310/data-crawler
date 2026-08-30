from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

import orchestrator


class _Task:
    task_id = "content-task-1"
    query = "brand monitoring"
    _done = False

    def __init__(self) -> None:
        self.bumped = []
        self.warnings = []
        self.errors = []

    def cancelled(self) -> bool:
        return False

    def bump(self, source: str, kind: str) -> None:
        self.bumped.append((source, kind))

    def add_warning(self, source: str, code: str, message: str) -> None:
        self.warnings.append((source, code, message))

    def add_error(self, source: str, code: str, message: str) -> None:
        self.errors.append((source, code, message))


class ContentRoutingTests(unittest.TestCase):
    def test_content_item_keeps_existing_raw_collected_response_contract(self) -> None:
        publisher = MagicMock()
        task = _Task()
        row = {
            "source": "x_playwright",
            "post_id": "x_playwright:post-1",
            "url": "https://x.com/example/status/1",
            "title": "A post",
            "text": "content",
        }

        with (
            patch.object(orchestrator, "get_publisher", return_value=publisher),
            patch.object(orchestrator, "persist_raw_posts"),
        ):
            on_item = orchestrator._make_on_item(task, "x_playwright")
            on_item("post", row)

        publisher.publish_post.assert_called_once()
        event = publisher.publish_post.call_args.args[0]
        self.assertEqual(event["event_type"], "raw_collected")
        self.assertEqual(event["content_type"], "post")
        self.assertEqual(event["external_id"], "post-1")
        self.assertEqual(task.bumped, [("x_playwright", "post")])
        self.assertEqual(task.errors, [])


if __name__ == "__main__":
    unittest.main()
