from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import influencer_orchestrator as orchestrator
import persist
from x_influencer_discovery.config import Settings
from x_influencer_discovery.extractors import extract_x_profile_from_html
from x_influencer_discovery.models import CandidateEvidence, CountValue, XProfile
from x_influencer_discovery.evaluation import evaluate_candidate
from x_influencer_discovery.pipeline import produce_x_discovery, run
from x_influencer_discovery.x_search import PostAuthor


class _Task:
    instances = []

    def __init__(self, *args, **kwargs):
        self.status = kwargs.get("status")
        self.errors = []
        self.warnings = []
        self.influencers = 0
        self.progress = {}
        self.status_history = [self.status]
        _Task.instances.append(self)

    def set_status(self, status):
        self.status = status
        self.status_history.append(status)

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
        self.status_history.append(status)

    def set_progress(self, **values):
        self.progress.update(values)

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
            '<div class="font-bold">12K</div><div>Followers</div>'
        )
        profile = extract_x_profile_from_html("janedoe", html)
        self.assertEqual(profile.name, "Jane Doe")
        self.assertEqual(profile.bio, "Marketing educator")
        self.assertEqual(
            profile.profile_img_url, "https://pbs.twimg.com/profile.jpg"
        )

    def test_rejects_companies_missing_followers_and_no_current_year_activity(self) -> None:
        current_year = datetime.now(timezone.utc).year
        profiles = [
            XProfile(
                name="Individual Without Metrics",
                handle="individual",
                profile_url="https://x.com/individual",
                bio="Unrelated bio",
                followers=CountValue(estimated=None),
                following=CountValue(estimated=10),
            ),
            XProfile(
                name="Active Individual",
                handle="active",
                profile_url="https://x.com/active",
                bio="Unrelated bio",
                followers=CountValue(estimated=100),
            ),
            XProfile(
                name="Inactive Individual",
                handle="inactive",
                profile_url="https://x.com/inactive",
                bio="Unrelated bio",
                followers=CountValue(estimated=100),
            ),
            XProfile(
                name="A Company",
                handle="company",
                profile_url="https://x.com/company",
                bio="Marketing platform",
                followers=CountValue(estimated=1000),
            ),
            XProfile(
                name=None,
                handle="unavailable",
                profile_url="https://x.com/unavailable",
                bio=None,
                source_status="blocked",
            ),
        ]
        posts = {
            "individual": [],
            "active": [{"created_at": f"{current_year}-01-02T00:00:00Z", "likes": 0}],
            "inactive": [{"created_at": f"{current_year - 1}-12-31T00:00:00Z", "likes": 100}],
            "company": [],
            "unavailable": [],
        }
        labels = {
            "individual": "person_name",
            "active": "person_name",
            "inactive": "person_name",
            "company": "company_name",
            "unavailable": "person_name",
        }
        evaluations = {
            profile.handle: evaluate_candidate(
                profile,
                posts[profile.handle],
                related_terms=["Marketing"],
                company_summary="Marketing agency",
                minimum_followers=100,
                minimum_relevance_score=0.0,
                company_name="Marketing Eye",
                company_domain="marketingeye.com.au",
                account_label=labels[profile.handle],
                semantic_similarity=1.0,
                current_year=current_year,
            )
            for profile in profiles
        }

        self.assertEqual(
            [handle for handle, result in evaluations.items() if result.eligible],
            ["active"],
        )
        self.assertEqual(evaluations["company"].reason, "company_or_organisation")
        self.assertEqual(evaluations["unavailable"].reason, "profile_fetch_failed")
        self.assertEqual(evaluations["individual"].reason, "followers_unavailable")
        self.assertEqual(evaluations["inactive"].reason, "no_current_year_post")

    def test_x_discovery_persists_evidence_before_queue_submission(self) -> None:
        class Queue:
            def __init__(self):
                self.urls = []

            def send(self, url, **_kwargs):
                self.urls.append(url)
                return True

        class Searcher:
            failed_terms = set()

            async def search(self, _terms, **_options):
                return {
                    "Marketing": [
                        PostAuthor(
                            "janedoe",
                            "Jane Doe",
                            CandidateEvidence(type="x_post", query="Marketing"),
                        )
                    ]
                }

        class EvidenceStore:
            def __init__(self):
                self.rows = []

            def record(self, **kwargs):
                self.rows.append(kwargs)

        queue = Queue()
        evidence = EvidenceStore()
        sent, degraded = asyncio.run(
            produce_x_discovery(
                queue,
                Searcher(),
                ["Marketing"],
                company_id="company-1",
                platform="x",
                language="en",
                authors_per_query=20,
                max_scrolls_per_query=8,
                evidence_store=evidence,
                run_id="task-1",
            )
        )
        self.assertEqual((sent, degraded), (1, False))
        self.assertEqual(queue.urls, ["https://x.com/janedoe"])
        self.assertEqual(evidence.rows[0]["task_id"], "task-1")
        self.assertEqual(evidence.rows[0]["profile_url"], "https://x.com/janedoe")
        self.assertEqual(evidence.rows[0]["evidence"]["type"], "x_post")


class TestInfluencerOrchestrator(unittest.TestCase):
    @staticmethod
    def request(**overrides):
        request = {
            "company_id": "company-1",
            "company_name": "Marketing Eye",
            "company_domain": "marketingeye.com.au",
            "company_summary": "A marketing agency helping brands grow.",
            "field": "Marketing",
            "related_terms": ["SEO", "marketing", "content marketing"],
            "platform": "x",
            "limit": 10,
            "resume": False,
        }
        request.update(overrides)
        return request

    def setUp(self):
        _Task.instances.clear()

    def test_sqs_settings_match_standalone_ranking_defaults(self):
        settings = orchestrator._settings()
        self.assertEqual(settings.classification_backend, "local")
        self.assertTrue(settings.embedding_enabled)

    def test_browser_session_setting_is_forwarded_to_reference(self):
        with patch.object(orchestrator.config, "X_BROWSER_SESSION", "/tmp/x-state.json"):
            settings = orchestrator._settings()
        self.assertEqual(settings.x_session, "/tmp/x-state.json")

    def test_topic_brief_normalizes_deduplicates_and_puts_field_first(self):
        normalized = orchestrator._normalize_request(**self.request())
        self.assertEqual(normalized["field"], "Marketing")
        self.assertEqual(normalized["related_terms"], ["SEO", "content marketing"])
        self.assertEqual(
            normalized["topic_terms"],
            ["Marketing", "SEO", "content marketing"],
        )

    def test_x_discovery_limits_are_configuration_driven(self):
        with (
            patch.object(orchestrator.config, "INFLUENCER_X_AUTHORS_PER_QUERY", 37),
            patch.object(orchestrator.config, "INFLUENCER_X_MAX_SCROLLS_PER_QUERY", 11),
        ):
            settings = orchestrator._settings()
        self.assertEqual(settings.x_authors_per_query, 37)
        self.assertEqual(settings.x_max_scrolls_per_query, 11)

    def test_snowball_setting_follows_reference_configuration(self):
        with patch.object(orchestrator.config, "INFLUENCER_ENABLE_SNOWBALL", True):
            settings = orchestrator._settings()
        self.assertTrue(settings.enable_snowball)

    def test_invalid_influencer_retry_setting_does_not_silently_change_policy(self):
        with (
            patch.object(orchestrator.config, "INFLUENCER_SQS_MAX_RECEIVE_COUNT", 2),
            self.assertRaisesRegex(ValueError, "must be exactly 1"),
        ):
            orchestrator._settings()

    def test_invalid_influencer_setting_finishes_task_as_failed(self):
        with (
            patch.object(orchestrator, "TaskState", _Task),
            patch.object(orchestrator.config, "INFLUENCER_SQS_MAX_RECEIVE_COUNT", 2),
        ):
            orchestrator.run_influencer_task("task-invalid-settings", **self.request())

        task = _Task.instances[-1]
        self.assertEqual(task.status, "failed")
        self.assertEqual(task.errors[0][0], "influencer_error")

    def test_missing_x_session_matches_reference_degraded_lane_behavior(self):
        candidate_collection = MagicMock()
        output = {"status": "completed", "results": [], "metrics": {}}
        with (
            patch.object(orchestrator, "TaskState", _Task),
            patch.object(orchestrator.config, "X_AUTH_TOKEN", ""),
            patch.object(orchestrator.config, "X_CT0", ""),
            patch.object(orchestrator.config, "X_BROWSER_SESSION", ""),
            patch.object(orchestrator, "notify_session_expired") as notify,
            patch.object(orchestrator, "get_mongo_db", return_value={"influencer_candidates": candidate_collection}),
            patch.object(orchestrator.DurableProfileQueue, "from_settings", return_value=MagicMock()),
            patch.object(orchestrator, "run", return_value=output) as pipeline_run,
        ):
            orchestrator.run_influencer_task("task-1", **self.request())

        task = _Task.instances[-1]
        self.assertEqual(task.status, "completed_with_no_results")
        notify.assert_not_called()
        pipeline_run.assert_called_once()
        self.assertIsNone(pipeline_run.call_args.args[2].x_session)

    def test_normalizes_topic_brief_and_passes_full_request_to_pipeline(self):
        output = {
            "status": "completed",
            "results": [{"account": {"handle": "janedoe"}}],
            "metrics": {
                "sources": {"x_post": 3, "public": 2},
                "queue": {"retried": 1, "dead_letter_depth": 2},
                "processing": {"fetched": 4, "evaluated": 4},
                "eligibility": {"passed": 1, "rejected": 3},
                "leaderboard": {"inserted": 1, "updated": 0},
            },
        }
        candidate_collection = MagicMock()
        queue = MagicMock()
        with (
            patch.object(orchestrator, "TaskState", _Task),
            patch.object(orchestrator.config, "X_AUTH_TOKEN", "token"),
            patch.object(orchestrator.config, "X_CT0", "ct0"),
            patch.object(orchestrator, "get_mongo_db", return_value={"influencer_candidates": candidate_collection}),
            patch.object(orchestrator.DurableProfileQueue, "from_settings", return_value=queue),
            patch.object(orchestrator, "run", return_value=output) as pipeline_run,
        ):
            orchestrator.run_influencer_task("task-2", **self.request())

        task = _Task.instances[-1]
        self.assertEqual(task.status, "completed")
        self.assertEqual(task.status_history, ["validating", "discovering", "processing", "completed"])
        self.assertEqual(task.progress["discovery_submitted"], 5)
        self.assertEqual(task.progress["profiles_fetched"], 4)
        self.assertEqual(task.progress["candidates_evaluated"], 4)
        self.assertEqual(task.progress["eligible"], 1)
        self.assertEqual(task.progress["rejected"], 3)
        self.assertEqual(task.progress["retried"], 1)
        self.assertEqual(task.progress["persisted"], 1)
        self.assertEqual(task.progress["dead_lettered"], 2)
        pipeline_kwargs = pipeline_run.call_args.kwargs
        self.assertEqual(pipeline_kwargs["company_id"], "company-1")
        self.assertEqual(pipeline_kwargs["company_name"], "Marketing Eye")
        self.assertEqual(pipeline_kwargs["company_domain"], "marketingeye.com.au")
        self.assertEqual(pipeline_kwargs["related_terms"], ["Marketing", "SEO", "content marketing"])
        self.assertEqual(pipeline_kwargs["run_id"], "task-2")
        self.assertIsInstance(pipeline_kwargs["candidate_store"], orchestrator.CandidateStore)
        self.assertIs(pipeline_kwargs["candidate_store"].collection, candidate_collection)
        self.assertIs(pipeline_kwargs["queue"], queue)
        self.assertTrue(pipeline_kwargs["produce_discovery"])
        self.assertNotIn("publisher", pipeline_kwargs)

    def test_resume_drains_existing_queue_without_running_discovery(self):
        candidate_collection = MagicMock()
        with (
            patch.object(orchestrator, "TaskState", _Task),
            patch.object(orchestrator.config, "X_AUTH_TOKEN", "token"),
            patch.object(orchestrator.config, "X_CT0", "ct0"),
            patch.object(orchestrator, "get_mongo_db", return_value={"influencer_candidates": candidate_collection}),
            patch.object(orchestrator.DurableProfileQueue, "from_settings", return_value=MagicMock()),
            patch.object(orchestrator, "run", return_value={"status": "completed", "results": [], "metrics": {}}) as pipeline_run,
        ):
            orchestrator.run_influencer_task("task-resume", **self.request(resume=True))

        self.assertFalse(pipeline_run.call_args.kwargs["produce_discovery"])

    def test_empty_results_complete_without_response_event(self):
        with (
            patch.object(orchestrator, "TaskState", _Task),
            patch.object(orchestrator.config, "X_AUTH_TOKEN", "token"),
            patch.object(orchestrator.config, "X_CT0", "ct0"),
            patch.object(orchestrator, "get_mongo_db", return_value={"influencer_candidates": MagicMock()}),
            patch.object(orchestrator.DurableProfileQueue, "from_settings", return_value=MagicMock()),
            patch.object(orchestrator, "run", return_value={"status": "completed", "results": [], "metrics": {}}),
        ):
            orchestrator.run_influencer_task("task-empty", **self.request())

        task = _Task.instances[-1]
        self.assertEqual(task.status, "completed_with_no_results")


class TestInfluencerPersistence(unittest.TestCase):
    def test_saves_ranked_run_with_diagnostics(self) -> None:
        collection = MagicMock()
        db = {"x_influencer_runs": collection}
        output = {
            "query": "Marketing",
            "platform": "X",
            "generated_at": "2026-08-19T00:00:00+00:00",
            "results": [{"handle": "janedoe", "rank": 1}],
            "diagnostics": {"requested_limit": 20, "final_results": 1},
        }
        with (
            patch.object(persist, "ensure_raw_indexes"),
            patch.object(persist, "get_mongo_db", return_value=db),
        ):
            persist.save_influencer_run("task-1", "MarketingEye.com.au", output)

        collection.update_one.assert_called_once()
        filter_doc, update_doc = collection.update_one.call_args.args[:2]
        self.assertEqual(filter_doc, {"task_id": "task-1"})
        stored = update_doc["$set"]
        self.assertEqual(stored["company_domain_id"], "marketingeye.com.au")
        self.assertEqual(stored["results"], output["results"])
        self.assertEqual(stored["diagnostics"], output["diagnostics"])

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

    def test_evidence_ledger_is_keyed_by_task_company_platform_and_account(self) -> None:
        collection = MagicMock()
        with (
            patch.object(persist, "ensure_raw_indexes"),
            patch.object(persist, "get_mongo_db", return_value={"influencer_candidate_evidence": collection}),
        ):
            persist.upsert_influencer_candidate_evidence(
                task_id="task-1",
                company_id="company-1",
                platform="x",
                account_id="x:janedoe",
                handle="janedoe",
                evidence=[{"type": "x_post", "query": "Marketing"}],
            )

        selector, update = collection.update_one.call_args.args[:2]
        self.assertEqual(
            selector,
            {
                "task_id": "task-1",
                "company_id": "company-1",
                "platform": "x",
                "account_id": "x:janedoe",
            },
        )
        self.assertEqual(update["$addToSet"]["evidence"]["$each"], [{"type": "x_post", "query": "Marketing"}])


if __name__ == "__main__":
    unittest.main()
