from __future__ import annotations

import asyncio
from contextlib import nullcontext
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, ContextManager

from .browser import PlaywrightFetcher
from .config import Settings
from .discovery import Candidate, discover_candidates
from .embeddings import LocalEmbeddingMatcher
from .extractors import extract_x_profile_from_html
from .llm import ProfileClassifier
from .local_classifier import LocalProfileClassifier
from .models import XProfile
from .scoring import classify_account, score_candidate
from .storage import maybe_save_mongo, save_json
from .x_people_search import XPeopleSearcher
from .x_profile_posts import XRecentPostFetcher, recent_activity_from_posts

# Ranking evidence must not depend on the number of rows requested by the
# caller. `n` is applied only after these fixed pools have been evaluated.
DISCOVERY_POOL_SIZE = 25
PROFILE_POOL_SIZE = 80
RECENT_POST_POOL_SIZE = 50


def _safe_filename(query: str) -> str:
    import re
    return re.sub(r"[^a-z0-9]+", "_", query.lower()).strip("_") or "query"


async def run_pipeline(
    query: str,
    n: int,
    settings: Settings,
    output_path: Path | None = None,
    *,
    persist_output: bool = True,
    x_slot_factory: Callable[[], ContextManager[None]] | None = None,
) -> dict[str, Any]:
    query = " ".join(query.split())
    if not query:
        raise ValueError("query must not be empty")
    if n < 1:
        raise ValueError("n must be at least 1")

    fetcher = PlaywrightFetcher(headless=settings.headless, x_session=settings.x_session)
    # Public profile meta tags are often easier to parse without an authenticated
    # React session. Use login for People search; use public HTML for profile parsing.
    profile_fetcher = PlaywrightFetcher(headless=settings.headless, x_session=None)
    people_searcher = XPeopleSearcher(headless=settings.headless, x_session=settings.x_session)
    if settings.classification_backend == "local":
        classifier = LocalProfileClassifier(settings.local_classifier_model, settings.local_classifier_device)
    elif settings.classification_backend == "openai":
        classifier = ProfileClassifier(settings.openai_api_key, settings.openai_model)
    elif settings.classification_backend == "none":
        classifier = None
    else:
        raise ValueError("CLASSIFICATION_BACKEND must be one of: local, openai, none")
    x_slot = x_slot_factory or nullcontext
    with x_slot():
        people_cards, _ = await people_searcher.search(query, DISCOVERY_POOL_SIZE)

    candidates: dict[str, Candidate] = {}
    x_search_rank: dict[str, int] = {}
    x_card_meta: dict[str, dict[str, Any]] = {}
    for card in people_cards:
        candidates[card.handle.lower()] = Candidate(handle=card.handle, discovery_sources=set(card.discovery_sources))
        x_search_rank[card.handle.lower()] = card.search_rank
        x_card_meta[card.handle.lower()] = {
            "rank": card.search_rank,
            "name": card.name,
            "snippet": card.snippet,
            "profile_url": card.profile_url,
        }

    # X People is the primary, efficient source. A small public-web supplement
    # is still collected because X People often ranks exact-keyword accounts
    # above broadly recognized domain experts.
    fallback_candidates = await discover_candidates(
        query,
        DISCOVERY_POOL_SIZE,
        fetcher,
        num_queries=settings.public_web_num_queries,
    )
    for key, cand in fallback_candidates.items():
        if key in candidates:
            candidates[key].discovery_sources.update(cand.discovery_sources)
        else:
            candidates[key] = cand

    # Fetch more profiles than needed so ranking has room to filter bad/company accounts.
    ordered = sorted(
        candidates.values(),
        key=lambda candidate: (
            -len(candidate.discovery_sources),
            x_search_rank.get(candidate.handle.lower(), 10_000),
            candidate.handle.lower(),
        ),
    )
    # Fetch a broader candidate pool so noisy source pages cannot crowd out
    # legitimate people who were discovered later.
    profile_targets = ordered[:PROFILE_POOL_SIZE]
    urls = [f"https://x.com/{c.handle}" for c in profile_targets]
    with x_slot():
        pages = await profile_fetcher.fetch_many(urls, concurrency=4)

    profiles: list[XProfile] = []
    for cand, page in zip(profile_targets, pages):
        if not page.html:
            profiles.append(XProfile(
                name=None,
                handle=cand.handle,
                profile_url=f"https://x.com/{cand.handle}",
                bio=None,
                source_status=page.status,
                error=page.error,
                discovery_sources=sorted(cand.discovery_sources),
            ))
            continue
        profile = extract_x_profile_from_html(cand.handle, page.html)
        profile.discovery_sources = sorted(cand.discovery_sources)[:8]
        profiles.append(profile)

    classification_labels = classifier.classify_profiles(
        profile for profile in profiles if profile.source_status == "ok"
    ) if classifier else {}
    embedding_similarities = LocalEmbeddingMatcher(
        settings.embedding_enabled,
        settings.embedding_model,
    ).profile_similarities(query, (profile for profile in profiles if profile.source_status == "ok"))

    scored, _ = _score_profiles(
        profiles,
        query=query,
        candidates=candidates,
        x_search_rank=x_search_rank,
        classification_labels=classification_labels,
        embedding_similarities=embedding_similarities,
    )

    scored.sort(key=lambda item: (-item.score_breakdown.total, x_search_rank.get(item.handle.lower(), 10_000), item.handle.lower()))
    recent_post_targets = [item.handle for item in scored[:RECENT_POST_POOL_SIZE]]
    recent_posts_by_handle: dict[str, list[dict[str, Any]]] = {}
    if settings.x_session and recent_post_targets:
        recent_fetcher = XRecentPostFetcher(headless=settings.headless, x_session=settings.x_session)
        with x_slot():
            recent_posts_by_handle = await recent_fetcher.fetch_many(
                recent_post_targets,
                posts_per_profile=5,
                concurrency=2,
            )

    if recent_posts_by_handle:
        scored, _ = _score_profiles(
            profiles,
            query=query,
            candidates=candidates,
            x_search_rank=x_search_rank,
            classification_labels=classification_labels,
            embedding_similarities=embedding_similarities,
            recent_posts_by_handle=recent_posts_by_handle,
        )

    scored.sort(key=lambda item: (-item.score_breakdown.total, x_search_rank.get(item.handle.lower(), 10_000), item.handle.lower()))
    results = scored[:n]
    for rank, result in enumerate(results, 1):
        result.rank = rank

    profiles_by_handle = {profile.handle.lower(): profile for profile in profiles}

    output = _public_output(
        query,
        [_result_to_rich_dict(r, profiles_by_handle, x_search_rank, x_card_meta) for r in results],
    )
    if persist_output:
        if output_path is None:
            output_path = settings.data_dir / f"{_safe_filename(query)}_x_influencers.json"
        save_json(output, output_path)
        maybe_save_mongo(output, settings.mongodb_url)
    return output


def run(
    query: str,
    n: int,
    settings: Settings,
    output_path: Path | None = None,
    *,
    persist_output: bool = True,
    x_slot_factory: Callable[[], ContextManager[None]] | None = None,
) -> dict[str, Any]:
    return asyncio.run(
        run_pipeline(
            query,
            n,
            settings,
            output_path,
            persist_output=persist_output,
            x_slot_factory=x_slot_factory,
        )
    )


def _public_output(query: str, results: list[dict[str, Any]]) -> dict[str, Any]:
    """Return the stable JSON contract written to disk and printed by the CLI."""
    return {
        "query": query,
        "platform": "X",
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "results": results,
    }


def _x_rank_to_source_strength(rank: int | None, fallback_source_count: int) -> int:
    if rank is None:
        return fallback_source_count
    # X People rank is useful evidence, but not strong enough to override
    # repeated public-source consensus.
    return fallback_source_count + 1


def _score_profiles(
    profiles: list[XProfile],
    *,
    query: str,
    candidates: dict[str, Candidate],
    x_search_rank: dict[str, int],
    classification_labels: dict[str, str],
    embedding_similarities: dict[str, float] | None = None,
    recent_posts_by_handle: dict[str, list[dict[str, Any]]] | None = None,
) -> tuple[list, int]:
    """Apply one consistent filtering, classification, and scoring path per pass."""
    scored = []
    companies_discarded = 0
    for profile in profiles:
        key = profile.handle.lower()
        candidate = candidates.get(key)
        if profile.source_status != "ok" or not profile.followers.estimated:
            continue
        account_label = classification_labels.get(key)
        if classify_account(profile, llm_label=account_label) == "company_name":
            companies_discarded += 1
            continue
        posts = None
        if recent_posts_by_handle is not None:
            posts = recent_posts_by_handle.get(key) or []
            if posts:
                profile.recent_activity = recent_activity_from_posts(posts)
            # With an authenticated X session, top results must have at least
            # one verifiable recent post with some visible interaction.
            if not _has_usable_recent_posts(posts):
                continue
            if profile.recent_activity.status == "stale":
                continue
        if _looks_like_follow_back_account(profile) and len(candidate.discovery_sources if candidate else ()) < 3:
            continue
        item = score_candidate(
            profile,
            query=query,
            source_count=_x_rank_to_source_strength(
                x_search_rank.get(key),
                len(candidate.discovery_sources) if candidate else 0,
            ),
            recent_posts=posts,
            account_label=account_label,
            embedding_similarity=(embedding_similarities or {}).get(key),
        )
        if item.score_breakdown.topic_relevance < 8:
            continue
        if key in x_search_rank:
            item.relevance_evidence.append(f"X People search rank: {x_search_rank[key]}")
        scored.append(item)
    return scored, companies_discarded


def _has_usable_recent_posts(posts: list[dict[str, Any]]) -> bool:
    return bool(posts) and any(
        int(post.get("likes") or 0) + int(post.get("reposts") or 0) + int(post.get("replies") or 0) > 0
        for post in posts
    )


def _looks_like_follow_back_account(profile: XProfile) -> bool:
    followers = profile.followers.estimated or 0
    following = profile.following.estimated or 0
    return followers > 0 and following >= followers * 0.5


def _result_to_rich_dict(
    result,
    profiles_by_handle: dict[str, XProfile],
    x_search_rank: dict[str, int],
    x_card_meta: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    data = result.to_dict()
    key = result.handle.lower()
    profile = profiles_by_handle.get(key)
    data["source_status"] = profile.source_status if profile else None
    data["profile_fetch_error"] = profile.error if profile else None
    data["x_people_rank"] = x_search_rank.get(key)
    data["x_people_card"] = x_card_meta.get(key)
    data["discovery_source_count"] = len(result.discovery_sources)
    return data
