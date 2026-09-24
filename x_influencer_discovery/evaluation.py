from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import math
from typing import Any

from .models import ScoreBreakdown, ScoredInfluencer, XProfile
from .relevance import hybrid_relevance
from .x_profile_posts import (
    build_engagement_metrics,
    engagement_score,
    recent_activity_from_posts,
    recent_activity_score,
)


def follower_score(count: int | None, max_score: int = 30) -> int:
    if not count:
        return 0
    # A smooth log scale gives specialists useful reach credit without letting
    # celebrity-sized accounts dominate the result.
    return min(max_score, max(1, round(math.log10(count + 1) / 7 * max_score)))


def classify_account_label(profile: XProfile, llm_label: str | None = None) -> str:
    """Return one of the two account labels consumed by eligibility checks."""
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
    recent_posts: list[dict] | None = None,
    topic_terms: list[str] | None = None,
) -> tuple[int, list[str]]:
    relevance = hybrid_relevance(
        profile.bio or "",
        recent_posts or [],
        topic_terms or [query],
        semantic_similarity=embedding_similarity,
    )
    score = round(40 * relevance["hybrid_relevance"])
    evidence = []
    if profile.bio:
        evidence.append(f"Public X bio: {profile.bio[:220]}")
    if relevance["bio_hits"]:
        evidence.append("Public X bio contains related terms: " + ", ".join(relevance["bio_hits"][:8]))
    if embedding_similarity is not None:
        evidence.append(f"Normalized local semantic similarity: {relevance['semantic_similarity']:.2f}")
    post_hits = sorted({hit for hits in relevance["post_hits"] for hit in hits})
    if post_hits:
        evidence.append("Recent X posts contain related terms: " + ", ".join(post_hits[:8]))
    return score, evidence


def score_candidate(
    profile: XProfile,
    query: str,
    source_count: int = 0,
    recent_posts: list[dict] | None = None,
    account_label: str | None = None,
    embedding_similarity: float | None = None,
    topic_terms: list[str] | None = None,
) -> ScoredInfluencer:
    account_type = classify_account(profile, llm_label=account_label)
    topic_score, evidence = topic_relevance_score(
        profile,
        query,
        embedding_similarity,
        recent_posts=recent_posts,
        topic_terms=topic_terms,
    )
    # Run-local appearance frequency. It does not affect relevance or eligibility.
    frequency = min(10, max(0, source_count) * 2)
    if source_count:
        evidence.append(f"Appeared {source_count} time(s) during this run.")
    fscore = follower_score(profile.followers.estimated, max_score=20)
    engagement_metrics = build_engagement_metrics(
        recent_posts or [],
        follower_estimate=profile.followers.estimated,
    )
    recent = recent_activity_score(profile.recent_activity, max_score=15)
    eng = engagement_score(engagement_metrics, max_score=15)
    total = topic_score + frequency + fscore + recent + eng
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
            frequently_appeared=frequency,
            followers=fscore,
            recent_activity=recent,
            engagement=eng,
            total=total,
        ),
        confidence="high" if source_count >= 3 and profile.bio else "medium",
    )


@dataclass(frozen=True)
class CandidateEvaluation:
    eligible: bool
    reason: str | None
    relevance: dict[str, Any]
    score: ScoredInfluencer | None = None


def _is_requester_handle(handle: str, company_name: str | None, company_domain: str | None) -> bool:
    key = "".join(char for char in handle.casefold() if char.isalnum())
    requester_keys = {
        "".join(char for char in (company_name or "").casefold() if char.isalnum()),
        "".join(char for char in (company_domain or "").split(".", 1)[0].casefold() if char.isalnum()),
    } - {""}
    return any(key == requester or key.startswith(requester) for requester in requester_keys)


def _has_current_year_post(posts: list[dict[str, Any]], year: int) -> bool:
    for post in posts:
        try:
            if datetime.fromisoformat(str(post.get("created_at") or "").replace("Z", "+00:00")).year == year:
                return True
        except ValueError:
            continue
    return False


def evaluate_candidate(
    profile: XProfile,
    recent_posts: list[dict[str, Any]],
    *,
    related_terms: list[str],
    company_summary: str,
    minimum_followers: int,
    minimum_relevance_score: float,
    company_name: str | None = None,
    company_domain: str | None = None,
    account_label: str | None = None,
    semantic_similarity: float | None = None,
    appearance_count: int = 0,
    current_year: int | None = None,
) -> CandidateEvaluation:
    """Apply the contract's ordered business eligibility checks to one profile."""
    current_year = current_year or datetime.now().year
    empty_relevance = {"lexical_overlap": 0.0, "semantic_similarity": 0.0, "hybrid_relevance": 0.0}
    if profile.source_status != "ok":
        return CandidateEvaluation(False, "profile_fetch_failed", empty_relevance)
    if _is_requester_handle(profile.handle, company_name, company_domain):
        return CandidateEvaluation(False, "requesting_company", empty_relevance)
    if classify_account(profile, llm_label=account_label) != "person_name":
        return CandidateEvaluation(False, "company_or_organisation", empty_relevance)
    if profile.followers.estimated is None:
        return CandidateEvaluation(False, "followers_unavailable", empty_relevance)
    if profile.followers.estimated < minimum_followers:
        return CandidateEvaluation(False, "followers_below_minimum", empty_relevance)
    if not _has_current_year_post(recent_posts, current_year):
        return CandidateEvaluation(False, "no_current_year_post", empty_relevance)

    relevance = hybrid_relevance(
        profile.bio or "", recent_posts, related_terms, semantic_similarity=semantic_similarity
    )
    if relevance["hybrid_relevance"] < minimum_relevance_score:
        return CandidateEvaluation(False, "relevance_below_minimum", relevance)

    # Company summary is deliberately an explicit evaluator input. The caller
    # obtains its semantic similarity from that summary only.
    if not company_summary.strip():
        raise ValueError("company summary must not be empty")
    profile.recent_activity = recent_activity_from_posts(recent_posts)
    score = score_candidate(
        profile,
        query=related_terms[0] if related_terms else company_summary,
        source_count=appearance_count,
        recent_posts=recent_posts,
        account_label=account_label,
        embedding_similarity=semantic_similarity,
        topic_terms=related_terms,
    )
    return CandidateEvaluation(True, None, relevance, score)


def normalize_result(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "handle": (result.get("handle") or "").lower(),
        "name": result.get("name"),
        "followers": (result.get("followers") or {}).get("estimated"),
        "rank": result.get("rank"),
    }


def compare_outputs(actual: dict[str, Any], expected: dict[str, Any], top_n: int) -> dict[str, Any]:
    actual_items = [normalize_result(r) for r in actual.get("results", [])[:top_n]]
    expected_items = [normalize_result(r) for r in expected.get("results", [])[:top_n]]
    actual_handles = [r["handle"] for r in actual_items]
    expected_handles = [r["handle"] for r in expected_items]
    overlap = len(set(actual_handles) & set(expected_handles))
    same_order = actual_handles == expected_handles
    ratio = overlap / max(1, min(top_n, len(expected_handles)))
    info_matches = []
    for a, e in zip(actual_items, expected_items):
        follower_close = True
        if a["followers"] is not None and e["followers"] is not None:
            follower_close = abs(a["followers"] - e["followers"]) <= max(1000, e["followers"] * 0.05)
        info_matches.append(a["handle"] == e["handle"] and follower_close)
    return {
        "correct": same_order and all(info_matches),
        "same_ranking": same_order,
        "handle_overlap_ratio": ratio,
        "matching_handles": sorted(set(actual_handles) & set(expected_handles)),
        "actual_handles": actual_handles,
        "expected_handles": expected_handles,
        "info_match_ratio": sum(info_matches) / max(1, len(info_matches)),
    }
