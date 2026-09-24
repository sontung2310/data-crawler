"""Deterministic, per-candidate topic relevance helpers.

Neither function receives a candidate batch. Adding an unrelated profile must
never change another profile's relevance or score.
"""

from __future__ import annotations

from typing import Any

from .querying import term_matches_text


def _term_weight(term: str) -> int:
    """Give an explicit multi-word phrase more weight than a single term."""
    return 3 if len(term.split()) > 1 else 1


def _coverage(text: str, related_terms: list[str]) -> tuple[float, list[str]]:
    normalized_terms = list(dict.fromkeys(term.strip() for term in related_terms if term.strip()))
    total_weight = sum(_term_weight(term) for term in normalized_terms)
    if not total_weight:
        return 0.0, []
    hits = [term for term in normalized_terms if term_matches_text(term, text or "")]
    return sum(_term_weight(term) for term in hits) / total_weight, hits


def lexical_overlap(bio: str, posts: list[dict[str, Any]], related_terms: list[str]) -> dict[str, Any]:
    """Calculate weighted exact-term coverage across a bio and up to five posts.

    A term contributes once per evidence surface, regardless of repeats. The
    fixed aggregation is 45% bio coverage, 40% average post coverage, and 15%
    post consistency.
    """
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
