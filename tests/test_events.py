#!/usr/bin/env python3
"""Unit tests for event envelope helpers."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from events import (  # noqa: E402
    comment_event_from_row,
    post_event_from_row,
    validate_event,
)


class TestValidateEvent(unittest.TestCase):
    def test_valid_post(self) -> None:
        ok, reason = validate_event(
            {
                "schema_version": 1,
                "content_type": "post",
                "source": "x_playwright",
                "external_id": "abc",
                "payload": {},
            }
        )
        self.assertTrue(ok)
        self.assertEqual(reason, "")

    def test_missing_source(self) -> None:
        ok, reason = validate_event(
            {
                "schema_version": 1,
                "content_type": "post",
                "source": "",
                "external_id": "abc",
            }
        )
        self.assertFalse(ok)
        self.assertIn("source", reason)

    def test_bad_schema(self) -> None:
        ok, _ = validate_event({"schema_version": 99, "content_type": "post", "source": "x", "external_id": "1"})
        self.assertFalse(ok)


class TestEventFromRow(unittest.TestCase):
    def test_post_event(self) -> None:
        event = post_event_from_row(
            {"source": "youtube_api", "post_id": "youtube_api:vid1", "url": "https://youtu.be/vid1", "title": "t"},
            "task-1",
        )
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event["event_type"], "raw_collected")
        self.assertEqual(event["content_type"], "post")
        self.assertEqual(event["history_id"], "task-1")
        ok, _ = validate_event(event)
        self.assertTrue(ok)

    def test_comment_event(self) -> None:
        event = comment_event_from_row(
            {
                "source": "x_playwright",
                "comment_id": "c1",
                "parent_content_id": "p1",
                "text": "hi",
            },
            "task-2",
        )
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event["content_type"], "comment")
        self.assertEqual(event["parent_content_id"], "p1")
        ok, _ = validate_event(event)
        self.assertTrue(ok)


if __name__ == "__main__":
    unittest.main()
