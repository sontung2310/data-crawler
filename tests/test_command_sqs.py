#!/usr/bin/env python3
"""Unit tests for SQS command consumer."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from consumers import command_sqs  # noqa: E402


class TestParseCommand(unittest.TestCase):
    def test_valid(self) -> None:
        cmd = command_sqs._parse_command(
            {"query": " AI ", "source": "youtube", "time_delta": "day", "limit": 5, "job_id": "j1"}
        )
        self.assertEqual(cmd["query"], "AI")
        self.assertEqual(cmd["job_id"], "j1")
        self.assertEqual(cmd["limit"], 5)

    def test_missing_query(self) -> None:
        with self.assertRaises(ValueError):
            command_sqs._parse_command({"query": "  "})

    def test_limit_clamped(self) -> None:
        cmd = command_sqs._parse_command({"query": "q", "limit": 999})
        self.assertEqual(cmd["limit"], 100)

    def test_bad_limit(self) -> None:
        with self.assertRaises(ValueError):
            command_sqs._parse_command({"query": "q", "limit": "nope"})

    def test_job_id_generated(self) -> None:
        cmd = command_sqs._parse_command({"query": "q"})
        self.assertTrue(cmd["job_id"])


class TestHandleCommandMessage(unittest.TestCase):
    def test_new_job_submits(self) -> None:
        with (
            patch.object(command_sqs, "get_crawl_task", return_value=None),
            patch.object(command_sqs, "create_accepted_task") as create,
            patch.object(command_sqs, "submit_crawl") as submit,
        ):
            tid = command_sqs.handle_command_message(
                {"query": "hello", "source": "youtube", "job_id": "job-1", "limit": 2}
            )
        self.assertEqual(tid, "job-1")
        create.assert_called_once()
        submit.assert_called_once()

    def test_duplicate_job_skips(self) -> None:
        with (
            patch.object(command_sqs, "get_crawl_task", return_value={"task_id": "job-1", "status": "running"}),
            patch.object(command_sqs, "create_accepted_task") as create,
            patch.object(command_sqs, "submit_crawl") as submit,
        ):
            tid = command_sqs.handle_command_message(
                {"query": "hello", "source": "youtube", "job_id": "job-1"}
            )
        self.assertEqual(tid, "job-1")
        create.assert_not_called()
        submit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
