"""Fetch and evaluate a text list of X profiles in-process, with no SQS queue.

This uses the same SequentialProfileWorker fetch/classify/score path as
``python -m x_influencer_discovery run``, but it never enqueues work and never
drains LocalStack. Discovery is skipped.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from x_influencer_discovery.config import load_settings
from x_influencer_discovery.embeddings import LocalEmbeddingMatcher
from x_influencer_discovery.evaluation import evaluate_candidate
from x_influencer_discovery.extractors import normalize_handle
from x_influencer_discovery.pipeline import SequentialProfileWorker, _classifier_for, _ranking_document
from x_influencer_discovery.platforms import account_ref, normalize_profile_url
from x_influencer_discovery.storage import save_json
from x_influencer_discovery.worker_lock import ExclusiveLocalXWorkerLock, XWorkerLockUnavailableError

logger = logging.getLogger(__name__)

_DEFAULT_PROFILES = Path(__file__).with_name("sample_x_profiles.txt")
_DEFAULT_SUMMARY = (
    "AI-powered autonomous marketing strategy platform. Automates strategy, "
    "content generation, campaign execution, and analytics for agencies, SMBs, "
    "enterprises, and channel partners. Focus: AI marketing automation, "
    "marketing strategy, campaign optimization, ROI, thought leadership. "
    "Challenges: funding, staffing, market expansion, channel diversification"
)
_DEFAULT_RELATED_TERMS = (
    "ai marketing,marketing automation,digital marketing,martech,"
    "ai marketing tools,digital transformation,marketing agency,"
    "channel partnerships,roi marketing,marketing technology"
)


def _related_terms(value: str) -> list[str]:
    return [term.strip() for term in value.split(",") if term.strip()]


def _parse_profile_line(line: str) -> str | None:
    raw = line.strip()
    if not raw or raw.startswith("#"):
        return None
    handle = normalize_handle(raw)
    if not handle:
        raise ValueError(f"not an X handle or profile URL: {line.strip()!r}")
    return normalize_profile_url(f"https://x.com/{handle}")


def load_profile_urls(path: Path) -> list[str]:
    if not path.is_file():
        raise FileNotFoundError(f"profile list not found: {path}")
    urls: list[str] = []
    seen: set[str] = set()
    for line_number, line in enumerate(path.read_text().splitlines(), start=1):
        try:
            url = _parse_profile_line(line)
        except ValueError as exc:
            raise ValueError(f"{path}:{line_number}: {exc}") from exc
        if url is None or url in seen:
            continue
        seen.add(url)
        urls.append(url)
    if not urls:
        raise ValueError(f"{path} does not contain any X profiles")
    return urls


def _profile_row(profile_url: str, **fields: Any) -> dict[str, Any]:
    return {"profile_url": profile_url, **fields}


def _recent_post_summary(post: dict[str, Any]) -> dict[str, Any]:
    return {
        "url": post.get("url"),
        "created_at": post.get("created_at") or post.get("posted_at"),
        "text": str(post.get("text") or "")[:500],
        "engagement": {
            "likes": post.get("likes"),
            "reposts": post.get("reposts"),
            "replies": post.get("replies"),
            "views": post.get("views"),
        },
        "is_pinned": bool(post.get("is_pinned")),
    }


def _log_fetched_profile(handle: str, bio: str | None, recent_posts: list[dict[str, Any]]) -> None:
    posts = [_recent_post_summary(post) for post in recent_posts[:5]]
    logger.info(
        "profile handle=@%s bio=%s recent_posts=%s",
        handle,
        bio or "",
        json.dumps(posts, ensure_ascii=False),
    )


async def consume_listed_profiles(
    profile_urls: list[str],
    settings,
    *,
    related_terms: list[str],
    company_name: str,
    company_domain: str,
    platform: str,
) -> list[dict[str, Any]]:
    worker = SequentialProfileWorker(settings)
    classifier = _classifier_for(settings)
    embedding_matcher = LocalEmbeddingMatcher(settings.embedding_enabled, settings.embedding_model)
    rows: list[dict[str, Any]] = []
    fetched: list[tuple[str, Any]] = []

    async with worker.batch_context():
        for profile_url in profile_urls:
            await worker.wait_before_handle(profile_url)
            logger.info("fetching %s", profile_url)
            fetch = getattr(worker, "fetch_with_recovery", None) or worker.fetch
            outcome = await fetch(profile_url)
            if outcome.technical_error or outcome.profile is None:
                reason = outcome.technical_error or "profile_missing"
                logger.warning("handle=@%s state=fetch_failed reason=%s", profile_url.rsplit("/", 1)[-1], reason)
                rows.append(_profile_row(profile_url, status="fetch_failed", reason=reason))
                continue
            _log_fetched_profile(outcome.profile.handle, outcome.profile.bio, outcome.recent_posts)
            fetched.append((profile_url, outcome))

    if not fetched:
        return rows

    profiles = [outcome.profile for _, outcome in fetched if outcome.profile is not None]
    labels = await asyncio.to_thread(classifier.classify_profiles, profiles)
    documents = {
        outcome.profile.handle.lower(): _ranking_document(outcome.profile, outcome.recent_posts)
        for _, outcome in fetched
        if outcome.profile is not None
    }
    semantic_method = getattr(embedding_matcher, "strict_document_similarities", None) or embedding_matcher.document_similarities
    semantic = await asyncio.to_thread(
        semantic_method,
        settings.company_summary,
        documents,
        batch_size=settings.embedding_batch_size,
    )

    for profile_url, outcome in fetched:
        profile = outcome.profile
        assert profile is not None
        label = labels.get(profile.handle.lower())
        if not label:
            rows.append(_profile_row(
                profile_url,
                status="classification_failed",
                handle=profile.handle,
                reason="name classification did not produce a result",
            ))
            continue
        result = evaluate_candidate(
            profile,
            outcome.recent_posts,
            related_terms=related_terms,
            company_summary=settings.company_summary or "",
            minimum_followers=settings.minimum_followers,
            minimum_relevance_score=settings.minimum_relevance_score,
            company_name=company_name,
            company_domain=company_domain,
            account_label=label,
            semantic_similarity=semantic.get(profile.handle.lower()),
        )
        account = account_ref(platform, handle=profile.handle, profile_url=profile.profile_url)
        row = _profile_row(
            profile_url,
            status="ok",
            handle=profile.handle,
            account_id=account.account_id,
            display_name=profile.name,
            bio=profile.bio,
            followers=profile.followers.estimated,
            eligible=result.eligible,
            reason=result.reason,
            account_label=label,
            lexical_overlap=result.relevance.get("lexical_overlap"),
            semantic_similarity=result.relevance.get("semantic_similarity"),
            hybrid_relevance=result.relevance.get("hybrid_relevance"),
            recent_posts=[_recent_post_summary(post) for post in outcome.recent_posts[:5]],
        )
        if result.score is not None:
            row["final_score"] = result.score.score_breakdown.total
        rows.append(row)
        logger.info(
            "handle=@%s status=ok eligible=%s reason=%s followers=%s",
            profile.handle,
            result.eligible,
            result.reason,
            profile.followers.estimated,
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch and evaluate a text list of X profiles without SQS or discovery."
    )
    parser.add_argument(
        "--profiles",
        type=Path,
        default=_DEFAULT_PROFILES,
        help=f"Text file of handles or https://x.com/<handle> URLs (default: {_DEFAULT_PROFILES.name}).",
    )
    parser.add_argument("--company-name", default="Robotic Marketer")
    parser.add_argument("--company-id", default="roboticmarketer.com")
    parser.add_argument("--company-domain", default="roboticmarketer.com")
    parser.add_argument("--company-summary", default=_DEFAULT_SUMMARY)
    parser.add_argument("--related-terms", type=_related_terms, default=_related_terms(_DEFAULT_RELATED_TERMS))
    parser.add_argument("--platform", default="x")
    parser.add_argument("--output", type=Path, default=Path("logs/listed_x_influencers.json"))
    parser.add_argument("--env-file")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    if not args.related_terms:
        parser.error("--related-terms must contain at least one term")

    try:
        profile_urls = load_profile_urls(args.profiles)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))

    settings = load_settings(args.env_file)
    settings = replace(
        settings,
        company_summary=args.company_summary or settings.company_summary,
        platform=args.platform,
        enable_snowball=False,
    )
    if not settings.company_summary:
        parser.error("--company-summary or COMPANY_SUMMARY is required")

    try:
        with ExclusiveLocalXWorkerLock(settings.x_worker_lock_path):
            rows = asyncio.run(
                consume_listed_profiles(
                    profile_urls,
                    settings,
                    related_terms=args.related_terms,
                    company_name=args.company_name,
                    company_domain=args.company_domain,
                    platform=args.platform,
                )
            )
    except XWorkerLockUnavailableError as exc:
        parser.error(str(exc))
    except KeyboardInterrupt:
        logger.info("listed-profile run stopped (SIGINT)")
        raise SystemExit(130) from None

    failed = [row for row in rows if row.get("status") != "ok"]
    identity_mismatches = [
        row for row in failed
        if "profile_identity_mismatch" in str(row.get("reason") or "")
    ]
    output = {
        "status": "completed",
        "queue": False,
        "company_id": args.company_id,
        "company": args.company_name,
        "platform": args.platform,
        "profiles_file": str(args.profiles),
        "listed": len(profile_urls),
        "fetched_ok": sum(row.get("status") == "ok" for row in rows),
        "fetch_failed": len(failed),
        "profile_identity_mismatch": len(identity_mismatches),
        "eligible": sum(bool(row.get("eligible")) for row in rows),
        "profiles": rows,
    }
    save_json(output, args.output)
    print(json.dumps({
        "status": output["status"],
        "output": str(args.output),
        "listed": output["listed"],
        "fetched_ok": output["fetched_ok"],
        "fetch_failed": output["fetch_failed"],
        "profile_identity_mismatch": output["profile_identity_mismatch"],
        "eligible": output["eligible"],
        "handles": [row.get("handle") for row in rows if row.get("status") == "ok"],
        "failures": [
            {"profile_url": row["profile_url"], "reason": row.get("reason")}
            for row in failed
        ],
    }, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
