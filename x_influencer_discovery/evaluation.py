from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import math
from collections import Counter
from typing import Any

from .models import ScoreBreakdown, ScoredInfluencer, XProfile
from .querying import STOPWORDS, lexical_match_details, normalize_lexical_text
from .x_profile_posts import (
    build_engagement_metrics,
    engagement_score,
    recent_activity_from_posts,
    recent_activity_score,
)


def _keyword_weight(distinct_term_count: int) -> float:
    """Saturate repeated keyword importance without an abrupt hard cap."""
    return 2.0 * distinct_term_count / (distinct_term_count + 1)


def _prepare_terms(related_terms: list[str]) -> list[tuple[str, str, tuple[str, ...]]]:
    """Return display term, normalized key, and unique normalized words."""
    prepared: list[tuple[str, str, tuple[str, ...]]] = []
    seen: set[str] = set()
    for raw_term in related_terms:
        display_term = " ".join(str(raw_term).split())
        normalized_term = normalize_lexical_text(display_term)
        if not normalized_term or normalized_term in seen:
            continue
        seen.add(normalized_term)
        words = tuple(dict.fromkeys(
            word for word in normalized_term.split()
            if word not in STOPWORDS
        ))
        if not words:
            continue
        prepared.append((display_term, normalized_term, words))
    return prepared


def _term_weights(
    related_terms: list[str],
) -> tuple[list[tuple[str, str, tuple[str, ...]]], dict[str, float], dict[str, int]]:
    prepared = _prepare_terms(related_terms)
    distinct_term_counts: Counter[str] = Counter(
        word
        for _, _, words in prepared
        for word in set(words)
    )
    word_weights = {
        word: _keyword_weight(count)
        for word, count in distinct_term_counts.items()
    }
    phrase_weights = {
        normalized_term: sum(word_weights[word] for word in words) / len(words)
        for _, normalized_term, words in prepared
    }
    return prepared, phrase_weights, dict(distinct_term_counts)


def _coverage(text: str, related_terms: list[str]) -> tuple[float, list[str]]:
    prepared, phrase_weights, distinct_term_counts = _term_weights(related_terms)
    if not prepared:
        return 0.0, []

    matched_weight = 0.0
    hits: list[str] = []
    for display_term, normalized_term, _ in prepared:
        strength, matched_words = lexical_match_details(normalized_term, text or "")
        if not strength:
            continue
        # A shared generic word such as "marketing" must not create half-credit
        # for every phrase containing it. Partial credit needs a distinctive
        # non-stop word that appears in only one configured term.
        matched_content_words = matched_words - STOPWORDS
        if strength < 1.0 and not any(
            distinct_term_counts[word] == 1 for word in matched_content_words
        ):
            continue
        matched_weight += phrase_weights[normalized_term] * strength
        hits.append(display_term)

    return round(min(1.0, matched_weight / 5.0), 4), hits


def lexical_overlap(bio: str, posts: list[dict[str, Any]], related_terms: list[str]) -> dict[str, Any]:
    """Calculate weighted keyword evidence across a bio and up to five posts."""
    bio_coverage, bio_hits = _coverage(bio, related_terms)
    surfaces = [_coverage(str(post.get("text") or ""), related_terms) for post in posts[:5]]
    post_coverage = sum(score for score, _ in surfaces) / len(surfaces) if surfaces else 0.0
    post_consistency = sum(score > 0 for score, _ in surfaces) / len(surfaces) if surfaces else 0.0
    value = 0.45 * bio_coverage + 0.40 * post_coverage + 0.15 * post_consistency
    return {
        "lexical_overlap": round(value, 4),
        "bio_coverage": round(bio_coverage, 4),
        "post_coverage": round(post_coverage, 4),
        "post_consistency": round(post_consistency, 4),
        "bio_hits": bio_hits,
        "post_hits": [hits for _, hits in surfaces],
    }


def normalize_semantic_similarity(similarity: float | None) -> float:
    """Normalize a MiniLM cosine score from ``[-1, 1]`` to ``[0, 1]``."""
    if similarity is None:
        return 0.0
    return round(min(1.0, max(0.0, (float(similarity) + 1.0) / 2.0)), 4)


def hybrid_relevance(
    bio: str,
    posts: list[dict[str, Any]],
    related_terms: list[str],
    *,
    semantic_similarity: float | None,
) -> dict[str, Any]:
    """Combine deterministic lexical overlap and local semantic similarity."""
    lexical = lexical_overlap(bio, posts, related_terms)
    semantic = normalize_semantic_similarity(semantic_similarity)
    return {
        **lexical,
        "semantic_similarity": semantic,
        "hybrid_relevance": round(0.60 * lexical["lexical_overlap"] + 0.40 * semantic, 4),
    }


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
    fscore = follower_score(profile.followers.estimated, max_score=30)
    engagement_metrics = build_engagement_metrics(
        recent_posts or [],
        follower_estimate=profile.followers.estimated,
    )
    recent = recent_activity_score(profile.recent_activity, max_score=10)
    eng = engagement_score(engagement_metrics, max_score=20)
    total = topic_score + fscore + recent + eng
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
            followers=fscore,
            recent_activity=recent,
            engagement=eng,
            total=total,
        ),
        confidence="high" if len(profile.discovery_sources) >= 3 and profile.bio else "medium",
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
