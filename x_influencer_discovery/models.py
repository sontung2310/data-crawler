from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any


@dataclass(frozen=True)
class TopicBrief:
    field: str
    core_terms: list[str]
    related_terms: list[str]
    language: str = "en"

    @classmethod
    def build(cls, field_name: str, related_terms: list[str] | None = None) -> "TopicBrief":
        field_name = " ".join(field_name.split())
        if not field_name:
            raise ValueError("field must not be empty")
        related = []
        seen = {field_name.casefold()}
        for value in related_terms or []:
            value = " ".join(str(value).split())
            if value and value.casefold() not in seen:
                related.append(value)
                seen.add(value.casefold())
        return cls(field=field_name, core_terms=[field_name.casefold()], related_terms=related)

    def topic_terms(self, count: int = 4) -> list[str]:
        """Return exactly the configured discovery lanes where possible."""
        terms = [self.field, *self.related_terms]
        return terms[:count]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CandidateEvidence:
    type: str
    query: str | None = None
    source_url: str | None = None
    source_domain: str | None = None
    source_rank: int | None = None
    source_name: str | None = None
    source_bio: str | None = None
    source_title: str | None = None
    source_date: str | None = None
    context: str | None = None
    post_url: str | None = None
    posted_at: str | None = None
    text: str | None = None
    likes: int | None = None
    reposts: int | None = None
    replies: int | None = None
    views: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if value is not None}


@dataclass
class CountValue:
    raw: str | None = None
    estimated: int | None = None


@dataclass(frozen=True)
class AccountRef:
    """Platform-neutral identity and current public locator for an account."""

    account_id: str
    handle: str | None = None
    profile_url: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            key: value
            for key, value in {
                "account_id": self.account_id,
                "handle": self.handle,
                "profile_url": self.profile_url,
            }.items()
            if value is not None
        }


@dataclass(frozen=True)
class RenderedProfileSurface:
    """Public metadata read from X's authenticated, rendered profile DOM.

    X's Open Graph tags are generic or absent for many authenticated profile
    pages. Keeping this transient browser result separate from ``XProfile``
    lets the pure HTML extractor retain its public/fallback behaviour while
    preferring the rendered profile fields when they are available.
    """

    bio: str | None = None
    profile_img_url: str | None = None
    canonical_url: str | None = None


@dataclass
class RecentActivity:
    status: str
    latest_visible_post_at: str | None = None
    evidence: str | None = None


@dataclass
class ScoreBreakdown:
    topic_relevance: int
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
    platform: str = "x"
    account_id: str | None = None


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
