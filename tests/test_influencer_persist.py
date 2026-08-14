from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

import persist


class TestPersistInfluencer(unittest.TestCase):
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
