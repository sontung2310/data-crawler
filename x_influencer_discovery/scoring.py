from __future__ import annotations

import math
import re

from .models import ScoreBreakdown, ScoredInfluencer, XProfile
from .querying import query_expansions, query_terms, term_matches_text
from .x_profile_posts import build_engagement_metrics, engagement_score, recent_activity_score

def follower_score(count: int | None) -> int:
    if not count:
        return 0
    # Log-scaled authority: 100K ~= 18, 1M ~= 24, 5M+ ~= 29–30.
    return min(30, max(0, round((math.log10(count) - 4) / 3 * 18 + 12)))


def classify_account_label(profile: XProfile, llm_label: str | None = None) -> str:
    """Return only the two downstream labels requested by the user.

    `llm_label` is expected to come from gpt-5-nano. If it is unavailable or
    invalid, default to person_name so the pipeline does not discard candidates
    from deterministic/company-handle heuristics.
    """
    return llm_label if llm_label in {"person_name", "company_name"} else "person_name"


def classify_account(profile: XProfile, llm_label: str | None = None) -> str:
    return classify_account_label(profile, llm_label=llm_label)


def infer_niche(profile: XProfile, query: str) -> list[str]:
    query_norm = query.strip()
    niches: list[str] = []
    if query_norm:
        niches.append(query_norm)
    deduped = []
    for niche in niches:
        if niche and niche not in deduped:
            deduped.append(niche)
    return deduped[:4] or [query_norm]


def topic_relevance_score(
    profile: XProfile,
    query: str,
    embedding_similarity: float | None = None,
) -> tuple[int, list[str]]:
    # Page titles mostly mirror mutable display names. Ground relevance in the
    # account's self-described bio, rather than a label such as "AI expert".
    text = (profile.bio or "").lower()
    variants = query_expansions(query)
    terms = query_terms(query)
    if re.fullmatch(r"[A-Za-z]\s*[&/]\s*[A-Za-z]", query.strip()):
        # A literal two-initial match is too ambiguous (for example, names or
        # fandom tags that happen to contain F&B). Require a semantic expansion.
        terms.discard(query.strip().lower())
    hits = sorted({
        term for term in terms
        if term and term_matches_text(term, text)
    })
    original = query.strip().lower()
    lexical = 0.0
    if original and term_matches_text(original, text):
        lexical = 1.0
    elif any(variant.lower() != original and term_matches_text(variant, text) for variant in variants):
        lexical = 0.8
    elif hits:
        lexical = min(0.65, 0.35 + 0.15 * len(hits))

    # Similarities from normalized MiniLM embeddings are usually in [-1, 1].
    # Convert the useful 0.20–0.80 range to a simple 0–1 score.
    semantic = 0.0
    if embedding_similarity is not None:
        semantic = min(1.0, max(0.0, (embedding_similarity - 0.20) / 0.60))
    hybrid = 0.60 * lexical + 0.40 * semantic
    score = round(30 * hybrid)
    evidence = []
    if profile.bio:
        evidence.append(f"Public X bio: {profile.bio[:220]}")
    if hits:
        evidence.append("Public X bio contains query terms: " + ", ".join(hits[:8]))
    if embedding_similarity is not None:
        evidence.append(f"Local semantic similarity: {embedding_similarity:.2f}")
    return score, evidence


def score_candidate(
    profile: XProfile,
    query: str,
    source_count: int = 0,
    recent_posts: list[dict] | None = None,
    account_label: str | None = None,
    embedding_similarity: float | None = None,
) -> ScoredInfluencer:
    account_type = classify_account(profile, llm_label=account_label)
    topic_score, evidence = topic_relevance_score(profile, query, embedding_similarity)
    # Authority comes only from public-source consensus: how many independent
    # pages/search results mention the handle. No manual topic-specific boosts.
    authority = min(20, 5 + source_count * 3)
    if source_count:
        evidence.append(f"Discovered in {source_count} public source(s).")
    # A sparse bio should not automatically exclude a very large individual
    # account already included by a topical source list (for example, a
    # podcast host who discusses the topic without stating it in the bio).
    if topic_score < 8 and source_count and (profile.followers.estimated or 0) >= 1_000_000:
        topic_score = 8
        evidence.append("High-follower profile listed by a topical source; retained for ranking.")
    fscore = follower_score(profile.followers.estimated)
    engagement_metrics = build_engagement_metrics(recent_posts or [], follower_estimate=profile.followers.estimated)
    recent = recent_activity_score(profile.recent_activity, max_score=10)
    eng = engagement_score(engagement_metrics, max_score=10)
    total = topic_score + authority + fscore + recent + eng
    return ScoredInfluencer(
        rank=0,
        name=profile.name,
        handle=profile.handle,
        profile_url=profile.profile_url,
        profile_img_url=profile.profile_img_url,
        account_type=account_type,
        bio=profile.bio,
        followers=profile.followers,
        following=profile.following,
        niche=infer_niche(profile, query),
        relevance_evidence=evidence,
        recent_activity=profile.recent_activity,
        recent_posts=recent_posts or None,
        engagement_metrics=engagement_metrics,
        discovery_sources=profile.discovery_sources,
        score_breakdown=ScoreBreakdown(
            topic_relevance=topic_score,
            authority=authority,
            followers=fscore,
            recent_activity=recent,
            engagement=eng,
            total=total,
        ),
        confidence="high" if source_count >= 3 and profile.bio else "medium",
    )

