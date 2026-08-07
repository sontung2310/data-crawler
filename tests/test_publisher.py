#!/usr/bin/env python3
"""Unit tests for SQS publisher factory."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from publishers import base as publisher_base  # noqa: E402
from publishers.null import NullPublisher  # noqa: E402


class TestGetPublisher(unittest.TestCase):
    def setUp(self) -> None:
        publisher_base.reset_publisher_for_tests()

    def tearDown(self) -> None:
        publisher_base.reset_publisher_for_tests()

    def test_null_when_not_configured(self) -> None:
        with patch.object(publisher_base, "sqs_fully_configured", return_value=False):
            pub = publisher_base.get_publisher()
        self.assertIsInstance(pub, NullPublisher)

    def test_singleton(self) -> None:
        with patch.object(publisher_base, "sqs_fully_configured", return_value=False):
            a = publisher_base.get_publisher()
            b = publisher_base.get_publisher()
        self.assertIs(a, b)

    def test_sqs_when_configured(self) -> None:
        fake = MagicMock(name="SqsPublisherInstance")
        with (
            patch.object(publisher_base, "sqs_fully_configured", return_value=True),
            patch("publishers.sqs.SqsPublisher", return_value=fake) as ctor,
        ):
            pub = publisher_base.get_publisher()
        self.assertIs(pub, fake)
        ctor.assert_called_once()


class TestSqsPublisherSend(unittest.TestCase):
    def test_send_message_called(self) -> None:
        with (
            patch("publishers.sqs.boto3") as boto3,
            patch("publishers.sqs.AWS_SQS_QUEUE_URL", "https://sqs.example/q"),
            patch("publishers.sqs.AWS_REGION", "ap-southeast-2"),
            patch("publishers.sqs.AWS_ACCESS_KEY_ID", "ak"),
            patch("publishers.sqs.AWS_SECRET_ACCESS_KEY", "sk"),
        ):
            client = MagicMock()
            boto3.client.return_value = client
            from publishers.sqs import SqsPublisher

            pub = SqsPublisher()
            pub.publish_post({"event_id": "1", "event_type": "raw_collected"})
            client.send_message.assert_called_once()
            kwargs = client.send_message.call_args.kwargs
            self.assertEqual(kwargs["QueueUrl"], "https://sqs.example/q")
            self.assertIn("raw_collected", kwargs["MessageBody"])


if __name__ == "__main__":
    unittest.main()
