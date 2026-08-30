from __future__ import annotations

import unittest
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from x_influencer_discovery.dashboard_export import (
    DashboardExportError,
    map_candidates_to_dashboard_rows,
    publish_dashboard_influencers,
)


class DashboardExportTests(unittest.TestCase):
    @staticmethod
    def candidate(handle: str = "janedoe") -> dict:
        return {
            "account": {"handle": handle},
            "profile": {
                "avatar_url": "https://example.com/jane.jpg",
                "followers": 1234,
            },
        }

    def test_maps_candidates_and_preserves_existing_flags(self):
        rows = map_candidates_to_dashboard_rows(
            [self.candidate("JaneDoe"), self.candidate("new_creator")],
            existing_rows=[
                {
                    "name": "janedoe",
                    "status": "contacted",
                    "relevancy": "relevant",
                }
            ],
        )

        self.assertEqual(rows[0]["name"], "janedoe")
        self.assertEqual(rows[0]["status"], "contacted")
        self.assertEqual(rows[0]["relevancy"], "relevant")
        self.assertEqual(rows[1]["status"], "uncontacted")
        self.assertEqual(rows[1]["relevancy"], "")

    def test_empty_source_results_are_rejected(self):
        with self.assertRaisesRegex(DashboardExportError, "no fresh leaderboard"):
            publish_dashboard_influencers("mongodb://unused/db", "example.com", [])

    def test_dry_run_reads_flags_but_does_not_update_dashboard(self):
        collection = MagicMock()
        collection.find_one.return_value = {
            "influencers": [
                {
                    "data": {
                        "influencers": [
                            {
                                "name": "janedoe",
                                "status": "contacted",
                                "relevancy": "irrelevant",
                            }
                        ]
                    }
                }
            ]
        }

        @contextmanager
        def opened_collection(_url):
            yield collection

        with patch(
            "x_influencer_discovery.dashboard_export.open_dashboard_collection",
            opened_collection,
        ):
            report = publish_dashboard_influencers(
                "mongodb://unused/db",
                "example.com",
                [self.candidate()],
                dry_run=True,
            )

        self.assertTrue(report.dry_run)
        self.assertEqual(report.rows_written, 1)
        self.assertEqual(report.rows[0]["status"], "contacted")
        self.assertEqual(report.rows[0]["relevancy"], "irrelevant")
        collection.update_one.assert_not_called()

    def test_write_replaces_only_selected_dashboard_array(self):
        collection = MagicMock()
        collection.find_one.return_value = {"influencers": []}
        result = MagicMock(matched_count=1, modified_count=1)
        collection.update_one.return_value = result

        @contextmanager
        def opened_collection(_url):
            yield collection

        with patch(
            "x_influencer_discovery.dashboard_export.open_dashboard_collection",
            opened_collection,
        ):
            report = publish_dashboard_influencers(
                "mongodb://unused/db",
                "example.com",
                [self.candidate()],
            )

        self.assertFalse(report.dry_run)
        self.assertEqual(report.modified_count, 1)
        selector, update = collection.update_one.call_args.args[:2]
        self.assertEqual(selector, {"company_domain_id": "example.com"})
        self.assertIn("influencers.0.data.influencers", update["$set"])


if __name__ == "__main__":
    unittest.main()
