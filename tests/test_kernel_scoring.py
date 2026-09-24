"""Kernel ranking and appearance-count contracts from influencer_discovery ver2."""

from x_influencer_discovery.evaluation import score_candidate
from x_influencer_discovery.models import CountValue, RecentActivity, XProfile
from x_influencer_discovery.queueing import DurableProfileQueue


def _profile() -> XProfile:
    return XProfile(
        name="Jane Doe",
        handle="janedoe",
        profile_url="https://x.com/janedoe",
        bio="Marketing automation strategist",
        followers=CountValue(estimated=10_000_000),
        recent_activity=RecentActivity(
            status="active",
            latest_visible_post_at="2026-09-01T00:00:00+00:00",
            evidence="Latest 1 non-pinned posts average age 1.0 days.",
        ),
    )


def test_score_candidate_uses_frequency_and_capped_components():
    scored = score_candidate(
        _profile(),
        "marketing",
        source_count=2,
        recent_posts=[{"text": "Marketing automation", "likes": 10, "reposts": 1, "replies": 1}],
        topic_terms=["marketing", "marketing automation"],
    )
    parts = scored.score_breakdown

    assert parts.frequently_appeared == 4
    assert parts.topic_relevance <= 40
    assert parts.followers <= 20
    assert parts.recent_activity <= 15
    assert parts.engagement <= 15
    assert parts.followers == 20
    assert parts.recent_activity == 15
    assert parts.total == (
        parts.topic_relevance
        + parts.frequently_appeared
        + parts.followers
        + parts.recent_activity
        + parts.engagement
    )


def test_direct_sends_count_appearances_and_snowball_sends_do_not():
    class _Client:
        def send_message(self, **_kwargs):
            return {"MessageId": "1"}

    queue = DurableProfileQueue(_Client(), queue_url="https://example.invalid/queue", company_id="company-1")
    assert queue.send("https://x.com/JaneDoe", company_id="company-1") is True
    assert queue.send("https://twitter.com/janedoe", company_id="company-1") is False
    assert queue.appearance_count("https://x.com/janedoe") == 2
    assert queue.send_snowball("https://x.com/janedoe", company_id="company-1") is False
    assert queue.appearance_count("https://x.com/janedoe") == 2
