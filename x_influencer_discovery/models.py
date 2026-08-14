from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any


@dataclass
class CountValue:
    raw: str | None = None
    estimated: int | None = None


@dataclass
class RecentActivity:
    status: str
    latest_visible_post_at: str | None = None
    evidence: str | None = None


@dataclass
class ScoreBreakdown:
    topic_relevance: int
    authority: int
    followers: int
    recent_activity: int
    engagement: int
    total: int


@dataclass
class XProfile:
    name: str | None
    handle: str
    profile_url: str
    bio: str | None
    profile_img_url: str | None = None
    followers: CountValue = field(default_factory=CountValue)
    following: CountValue = field(default_factory=CountValue)
    source_status: str = "ok"
    title: str | None = None
    recent_activity: RecentActivity = field(default_factory=lambda: RecentActivity(status="unknown_public_html_limited", evidence="Static public X HTML did not expose reliable recent posts."))
    discovery_sources: list[str] = field(default_factory=list)
    error: str | None = None


@dataclass
class ScoredInfluencer:
    rank: int
    name: str | None
    handle: str
    profile_url: str
    profile_img_url: str | None
    account_type: str
    bio: str | None
    followers: CountValue
    following: CountValue
    niche: list[str]
    relevance_evidence: list[str]
    recent_activity: RecentActivity
    recent_posts: list[dict[str, Any]] | None
    engagement_metrics: dict[str, Any] | None
    discovery_sources: list[str]
    score_breakdown: ScoreBreakdown
    confidence: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

