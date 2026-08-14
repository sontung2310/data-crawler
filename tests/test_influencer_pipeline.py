from __future__ import annotations

import unittest

from x_influencer_discovery.config import Settings
from x_influencer_discovery.extractors import extract_x_profile_from_html
from x_influencer_discovery.pipeline import run


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


if __name__ == "__main__":
    unittest.main()
