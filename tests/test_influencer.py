from __future__ import annotations

import unittest
from contextlib import nullcontext
from unittest.mock import MagicMock, patch

import influencer_orchestrator as orchestrator
import persist
from x_influencer_discovery.config import Settings
from x_influencer_discovery.extractors import extract_x_profile_from_html
from x_influencer_discovery.pipeline import run


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


class TestInfluencerPipeline(unittest.TestCase):
    def test_rejects_empty_topic_before_network_work(self) -> None:
        with self.assertRaisesRegex(ValueError, "query must not be empty"):
            run("  ", 10, Settings(), persist_output=False)

    def test_extracts_required_profile_fields(self) -> None:
        html = (
            '<title>Jane Doe (@janedoe) / X</title>'
            '<meta property="og:description" content="Marketing educator">'
            '<meta property="og:image" content="https://pbs.twimg.com/profile.jpg">'
        )
        profile = extract_x_profile_from_html("janedoe", html)
        self.assertEqual(profile.name, "Jane Doe")
        self.assertEqual(profile.bio, "Marketing educator")
        self.assertEqual(
            profile.profile_img_url, "https://pbs.twimg.com/profile.jpg"
        )


class TestInfluencerOrchestrator(unittest.TestCase):
    def setUp(self):
        _Task.instances.clear()

    def test_missing_x_session_fails_instead_of_completing_empty(self):
        with (
            patch.object(orchestrator, "TaskState", _Task),
            patch.object(orchestrator, "X_AUTH_TOKEN", ""),
            patch.object(orchestrator, "X_CT0", ""),
            patch.object(orchestrator, "notify_session_expired") as notify,
            patch.object(orchestrator, "run") as pipeline_run,
        ):
            orchestrator.run_influencer_task("task-1", "Marketing", 10)

        task = _Task.instances[-1]
        self.assertEqual(task.status, "failed")
        self.assertEqual(task.errors[0][0], "session_expired")
        notify.assert_called_once()
        pipeline_run.assert_not_called()

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
            patch.object(orchestrator, "TaskState", _Task),
            patch.object(orchestrator, "X_AUTH_TOKEN", "token"),
            patch.object(orchestrator, "X_CT0", "ct0"),
            patch.object(
                orchestrator, "x_session_slot", return_value=nullcontext()
            ) as x_slot,
            patch.object(orchestrator, "run", return_value=output) as pipeline_run,
            patch.object(orchestrator, "persist_influencer") as persist_influencer,
            patch.object(orchestrator, "get_publisher", return_value=publisher),
        ):
            orchestrator.run_influencer_task("task-2", "Marketing", 10)

        task = _Task.instances[-1]
        self.assertEqual(task.status, "completed")
        self.assertEqual(task.influencers, 1)
        persist_influencer.assert_called_once()
        self.assertIs(pipeline_run.call_args.kwargs["x_slot_factory"], x_slot)
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


class TestInfluencerPersistence(unittest.TestCase):
    def test_upserts_one_minimal_influencer_document(self) -> None:
        collection = MagicMock()
        collection.update_one.return_value.upserted_id = "new-id"
        db = {"influencers": collection}
        row = {
            "source": "x_influencer_discovery",
            "topic": "Marketing",
            "name": "Example Person",
            "handle": "@Example_Handle",
            "bio": "Marketing educator",
            "profile_img_url": "https://example.com/profile.jpg",
            "followers_count": 1200,
            "following_count": 100,
        }

        with (
            patch.object(persist, "ensure_raw_indexes"),
            patch.object(persist, "get_mongo_db", return_value=db),
        ):
            created, updated = persist.persist_influencer(row)

        self.assertEqual((created, updated), (1, 0))
        update = collection.update_one.call_args.args
        self.assertEqual(update[0], {"_id": "x:example_handle"})
        stored = update[1]["$set"]
        self.assertEqual(stored["platform"], "x")
        self.assertEqual(stored["followers_count"], 1200)
        self.assertEqual(update[1]["$addToSet"], {"topics": "Marketing"})
        self.assertNotIn("score", stored)
        self.assertNotIn("confidence", stored)


if __name__ == "__main__":
    unittest.main()
