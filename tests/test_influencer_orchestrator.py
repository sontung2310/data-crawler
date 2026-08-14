from __future__ import annotations

import unittest
from contextlib import nullcontext
from unittest.mock import MagicMock, patch

import influencer_orchestrator as module


class _Task:
    instances = []

    def __init__(self, *args, **kwargs):
        self.status = kwargs.get("status")
        self.errors = []
        self.warnings = []
        self.influencers = 0
        _Task.instances.append(self)

    def set_source_status(self, source, status):
        self.source_status = status

    def bump(self, source, kind):
        assert kind == "influencer"
        self.influencers += 1

    def add_warning(self, source, code, message):
        self.warnings.append((code, message))

    def add_error(self, source, code, message):
        self.errors.append((code, message))

    def finish(self, status):
        self.status = status

    def snapshot(self):
        return {"progress": {"influencers_written": self.influencers}}


class TestInfluencerOrchestrator(unittest.TestCase):
    def setUp(self):
        _Task.instances.clear()

    def test_missing_x_session_fails_instead_of_completing_empty(self):
        with (
            patch.object(module, "TaskState", _Task),
            patch.object(module, "X_AUTH_TOKEN", ""),
            patch.object(module, "X_CT0", ""),
            patch.object(module, "notify_session_expired") as notify,
            patch.object(module, "run") as run,
        ):
            module.run_influencer_task("task-1", "Marketing", 10)

        task = _Task.instances[-1]
        self.assertEqual(task.status, "failed")
        self.assertEqual(task.errors[0][0], "session_expired")
        notify.assert_called_once()
        run.assert_not_called()

    def test_success_persists_and_publishes_minimal_profile(self):
        publisher = MagicMock()
        output = {
            "results": [
                {
                    "name": "Jane Doe",
                    "handle": "JaneDoe",
                    "bio": "Marketing educator",
                    "profile_img_url": "https://example.com/jane.jpg",
                    "followers": {"estimated": 1200},
                    "following": {"estimated": 100},
                }
            ]
        }
        with (
            patch.object(module, "TaskState", _Task),
            patch.object(module, "X_AUTH_TOKEN", "token"),
            patch.object(module, "X_CT0", "ct0"),
            patch.object(
                module, "x_session_slot", return_value=nullcontext()
            ) as x_slot,
            patch.object(module, "run", return_value=output) as run,
            patch.object(module, "persist_influencer") as persist,
            patch.object(module, "get_publisher", return_value=publisher),
        ):
            module.run_influencer_task("task-2", "Marketing", 10)

        task = _Task.instances[-1]
        self.assertEqual(task.status, "completed")
        self.assertEqual(task.influencers, 1)
        persist.assert_called_once()
        self.assertIs(
            run.call_args.kwargs["x_slot_factory"], x_slot
        )
        event = publisher.publish_influencer.call_args.args[0]
        self.assertEqual(event["content_type"], "influencer")
        self.assertEqual(
            set(event["payload"]),
            {
                "topic",
                "name",
                "handle",
                "bio",
                "profile_img_url",
                "followers_count",
                "following_count",
            },
        )


if __name__ == "__main__":
    unittest.main()
