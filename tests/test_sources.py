#!/usr/bin/env python3
"""
Smoke-test every adapter in sources.REGISTRY, plus extra mode checks
(X profile / Reddit subreddit / YouTube trending / comments).

Run from the project root:
    python -m tests.test_sources

Optional:
    python -m tests.test_sources --query "artificial intelligence" --limit 5
    python -m tests.test_sources --source newsapi
    python -m tests.test_sources --source x
    python -m tests.test_sources --source youtube_api --limit 3
    python -m tests.test_sources --registry-only
    python -m tests.test_sources --extra-only
    python -m tests.test_sources --with-comments
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

# Allow `python tests/test_sources.py` as well as `python -m tests.test_sources`.
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import requests

import config
from sources import REGISTRY
from sources import reddit_playwright, x_playwright, youtube_api


@dataclass
class SourceCheck:
    name: str
    status: str
    count: int
    duration_ms: int
    note: str
    sample_title: str = ""


@dataclass
class ExtraOption:
    """One alternate mode / filter not covered by the default REGISTRY fetch."""

    name: str
    group: str
    runner: Callable[[str, int], Any]
    items_key: str  # "posts" or "videos"
    needs_comments: bool = False


def _newsapi_auth_note() -> Optional[str]:
    if not config.NEWSAPI_KEY:
        return "NEWSAPI_KEY not set in .env"
    try:
        r = requests.get(
            "https://newsapi.org/v2/top-headlines",
            params={"country": "us", "pageSize": 1, "apiKey": config.NEWSAPI_KEY},
            timeout=10,
        )
        if r.status_code == 401:
            return "NEWSAPI_KEY rejected (401 Unauthorized) — check key at newsapi.org"
        if r.status_code == 429:
            return "NewsAPI rate limited (429) — try again later"
        if not r.ok:
            return f"NewsAPI preflight failed ({r.status_code})"
    except requests.RequestException as exc:
        return f"NewsAPI preflight error: {exc}"
    return None


def _reddit_config_note() -> Optional[str]:
    missing = []
    if not config.REDDIT_CLIENT_ID:
        missing.append("REDDIT_CLIENT_ID")
    if not config.REDDIT_CLIENT_SECRET:
        missing.append("REDDIT_CLIENT_SECRET")
    if not config.REDDIT_USER_AGENT:
        missing.append("REDDIT_USER_AGENT")
    if missing:
        return ", ".join(missing) + " not set in .env"
    return None


def _x_session_note() -> Optional[str]:
    if not x_playwright.XPlaywrightClient().session_configured():
        return "X_AUTH_TOKEN / X_CT0 not set in .env"
    return None


def _reddit_playwright_session_note() -> Optional[str]:
    if not reddit_playwright.RedditPlaywrightClient().session_configured():
        return "REDDIT_SESSION not set in .env"
    return None


def _youtube_config_note() -> Optional[str]:
    if not config.YOUTUBE_API_KEY:
        return "YOUTUBE_API_KEY not set in .env"
    return None


# Skip early when credentials are missing (keyed by REGISTRY adapter name / extra group).
CREDENTIAL_NOTES: Dict[str, Callable[[], Optional[str]]] = {
    "guardian": lambda: (
        None if config.GUARDIAN_API_KEY else "GUARDIAN_API_KEY not set in .env"
    ),
    "newsapi": _newsapi_auth_note,
    "reddit_official": _reddit_config_note,
    "x_playwright": _x_session_note,
    "reddit_playwright": _reddit_playwright_session_note,
    "youtube_api": _youtube_config_note,
}

DEFAULT_X_PROFILE = "Roboticmarketer"
DEFAULT_REDDIT_SUBREDDIT = "digital_marketing"
DEFAULT_YOUTUBE_REGION = "US"


def _build_extra_options(*, with_comments: bool) -> List[ExtraOption]:
    """Alternate crawl modes worth checking beyond the default REGISTRY fetch."""

    since = (date.today() - timedelta(days=30)).isoformat()

    options: List[ExtraOption] = [
        ExtraOption(
            name="x_playwright (search/live)",
            group="x_playwright",
            items_key="posts",
            runner=lambda q, lim: x_playwright.fetch(
                q, limit=lim, search_filter="live"
            ),
        ),
        ExtraOption(
            name="x_playwright (search/top)",
            group="x_playwright",
            items_key="posts",
            runner=lambda q, lim: x_playwright.fetch(
                q, limit=lim, search_filter="top"
            ),
        ),
        ExtraOption(
            name="x_playwright (search/since)",
            group="x_playwright",
            items_key="posts",
            runner=lambda q, lim, _since=since: x_playwright.fetch(
                q, limit=lim, search_filter="live", since=_since
            ),
        ),
        ExtraOption(
            name="x_playwright (profile)",
            group="x_playwright",
            items_key="posts",
            runner=lambda _q, lim: x_playwright.fetch_profile(
                DEFAULT_X_PROFILE, limit=lim
            ),
        ),
        ExtraOption(
            name="reddit_playwright (search/top)",
            group="reddit_playwright",
            items_key="posts",
            runner=lambda q, lim: reddit_playwright.fetch(
                q, limit=lim, sort="top", time_filter="week"
            ),
        ),
        ExtraOption(
            name="reddit_playwright (search/new)",
            group="reddit_playwright",
            items_key="posts",
            runner=lambda q, lim: reddit_playwright.fetch(
                q, limit=lim, sort="new"
            ),
        ),
        ExtraOption(
            name="reddit_playwright (subreddit/hot)",
            group="reddit_playwright",
            items_key="posts",
            runner=lambda _q, lim: reddit_playwright.fetch_subreddit(
                DEFAULT_REDDIT_SUBREDDIT, limit=lim, sort="hot"
            ),
        ),
        ExtraOption(
            name="reddit_playwright (subreddit/new)",
            group="reddit_playwright",
            items_key="posts",
            runner=lambda _q, lim: reddit_playwright.fetch_subreddit(
                DEFAULT_REDDIT_SUBREDDIT, limit=lim, sort="new"
            ),
        ),
        ExtraOption(
            name="youtube_api (search/date)",
            group="youtube_api",
            items_key="videos",
            runner=lambda q, lim: youtube_api.fetch(
                q, limit=lim, region_code=DEFAULT_YOUTUBE_REGION, order="date"
            ),
        ),
        ExtraOption(
            name="youtube_api (trending)",
            group="youtube_api",
            items_key="videos",
            runner=lambda _q, lim: youtube_api.fetch_trending(
                limit=lim, region_code=DEFAULT_YOUTUBE_REGION
            ),
        ),
    ]

    if with_comments:
        options.extend(
            [
                ExtraOption(
                    name="x_playwright (search + comments)",
                    group="x_playwright",
                    items_key="posts",
                    needs_comments=True,
                    runner=lambda q, lim: x_playwright.fetch(
                        q,
                        limit=min(lim, 2),
                        search_filter="live",
                        include_comments=True,
                        num_comment_crawl=3,
                    ),
                ),
                ExtraOption(
                    name="reddit_playwright (search + comments)",
                    group="reddit_playwright",
                    items_key="posts",
                    needs_comments=True,
                    runner=lambda q, lim: reddit_playwright.fetch(
                        q,
                        limit=min(lim, 2),
                        sort="top",
                        time_filter="week",
                        include_comments=True,
                        num_comment_crawl=3,
                    ),
                ),
                ExtraOption(
                    name="youtube_api (search + comments)",
                    group="youtube_api",
                    items_key="videos",
                    needs_comments=True,
                    runner=lambda q, lim: youtube_api.fetch(
                        q,
                        limit=min(lim, 2),
                        region_code=DEFAULT_YOUTUBE_REGION,
                        include_comments=True,
                        num_comment_crawl=3,
                    ),
                ),
            ]
        )

    return options


def _extract_rows(payload: Any, items_key: Optional[str] = None) -> Tuple[List[Dict], str]:
    """Normalize list results and CrawlResult dicts into (rows, detail_note)."""
    if isinstance(payload, list):
        return payload, ""

    if not isinstance(payload, dict):
        raise TypeError(f"Expected list or dict, got {type(payload).__name__}")

    if items_key == "videos" or "videos" in payload:
        videos = payload.get("videos") or []
        comments = payload.get("comments") or []
        return videos, f"videos={len(videos)} comments={len(comments)}"

    posts = payload.get("posts") or []
    comments = payload.get("comments") or []
    return posts, f"posts={len(posts)} comments={len(comments)}"


def _items_key_for_adapter(name: str) -> Optional[str]:
    if name == "youtube_api":
        return "videos"
    if name in {"x_playwright", "reddit_playwright"}:
        return "posts"
    return None


def _sample_title(rows: Sequence[Dict]) -> str:
    if not rows:
        return ""
    first = rows[0] or {}
    title = (first.get("title") or "").strip()
    if len(title) > 70:
        return title[:70] + "..."
    return title


def _run_adapter(name: str, fetch_fn: Callable, query: str, limit: int) -> SourceCheck:
    cred_note = CREDENTIAL_NOTES.get(name, lambda: None)()
    if cred_note:
        return SourceCheck(
            name=name,
            status="SKIP",
            count=0,
            duration_ms=0,
            note=cred_note,
        )

    started = time.perf_counter()
    try:
        payload = fetch_fn(query, limit=limit)
        duration_ms = int((time.perf_counter() - started) * 1000)
    except Exception as exc:
        duration_ms = int((time.perf_counter() - started) * 1000)
        return SourceCheck(
            name=name,
            status="FAIL",
            count=0,
            duration_ms=duration_ms,
            note=f"{type(exc).__name__}: {exc}",
        )

    try:
        rows, detail = _extract_rows(payload, _items_key_for_adapter(name))
    except TypeError as exc:
        return SourceCheck(
            name=name,
            status="FAIL",
            count=0,
            duration_ms=duration_ms,
            note=str(exc),
        )

    if len(rows) == 0:
        note = "Fetch completed but returned 0 items"
        if detail:
            note = f"{note} ({detail})"
        if name in {"bbc", "techcrunch"}:
            note += " (RSS filters by query text — try a broader --query)"
        return SourceCheck(
            name=name,
            status="EMPTY",
            count=0,
            duration_ms=duration_ms,
            note=note,
        )

    note = "OK"
    if detail:
        note = f"OK ({detail})"
    return SourceCheck(
        name=name,
        status="PASS",
        count=len(rows),
        duration_ms=duration_ms,
        note=note,
        sample_title=_sample_title(rows),
    )


def _run_extra_option(option: ExtraOption, query: str, limit: int) -> SourceCheck:
    cred_note = CREDENTIAL_NOTES.get(option.group, lambda: None)()
    if cred_note:
        return SourceCheck(
            name=option.name,
            status="SKIP",
            count=0,
            duration_ms=0,
            note=cred_note,
        )

    started = time.perf_counter()
    try:
        payload = option.runner(query, limit)
        duration_ms = int((time.perf_counter() - started) * 1000)
    except Exception as exc:
        duration_ms = int((time.perf_counter() - started) * 1000)
        return SourceCheck(
            name=option.name,
            status="FAIL",
            count=0,
            duration_ms=duration_ms,
            note=f"{type(exc).__name__}: {exc}",
        )

    try:
        rows, detail = _extract_rows(payload, option.items_key)
    except TypeError as exc:
        return SourceCheck(
            name=option.name,
            status="FAIL",
            count=0,
            duration_ms=duration_ms,
            note=str(exc),
        )

    if len(rows) == 0:
        note = "Fetch completed but returned 0 items"
        if detail:
            note = f"{note} ({detail})"
        if option.group in {"x_playwright", "reddit_playwright"}:
            note += " — selectors may have changed, or session cookies expired"
        return SourceCheck(
            name=option.name,
            status="EMPTY",
            count=0,
            duration_ms=duration_ms,
            note=note,
        )

    note = "OK"
    if detail:
        note = f"OK ({detail})"

    if option.needs_comments:
        comments = (payload or {}).get("comments") or []
        if not comments:
            return SourceCheck(
                name=option.name,
                status="EMPTY",
                count=len(rows),
                duration_ms=duration_ms,
                note=f"Got {len(rows)} items but 0 comments — comment selectors may have changed",
                sample_title=_sample_title(rows),
            )

    return SourceCheck(
        name=option.name,
        status="PASS",
        count=len(rows),
        duration_ms=duration_ms,
        note=note,
        sample_title=_sample_title(rows),
    )


def _print_results(results: List[SourceCheck]) -> None:
    name_width = max(len(r.name) for r in results)
    status_width = max(len(r.status) for r in results)

    print()
    print(f"{'Source':<{name_width}}  {'Status':<{status_width}}  {'Count':>5}  {'ms':>6}  Note")
    print("-" * (name_width + status_width + 30))

    for r in results:
        print(
            f"{r.name:<{name_width}}  {r.status:<{status_width}}  {r.count:>5}  {r.duration_ms:>6}  {r.note}"
        )
        if r.sample_title:
            print(f"{'':<{name_width}}  sample: {r.sample_title}")

    print()
    passed = sum(1 for r in results if r.status == "PASS")
    empty = sum(1 for r in results if r.status == "EMPTY")
    failed = sum(1 for r in results if r.status == "FAIL")
    skipped = sum(1 for r in results if r.status == "SKIP")
    print(
        f"Summary: {passed} passed, {empty} empty, {failed} failed, {skipped} skipped "
        f"(total {len(results)})"
    )


def _normalize_filter_token(value: str) -> str:
    return " ".join(value.lower().replace("(", " ").replace(")", " ").replace("_", " ").split())


def _match_name(name: str, filters: Sequence[str]) -> bool:
    """Match adapter keys, extra option names, or categories (x / reddit / news / …)."""
    name_n = _normalize_filter_token(name)
    for raw in filters:
        f = _normalize_filter_token(raw)
        if name_n == f:
            return True
        if name_n.startswith(f + " "):
            return True
        # category shorthand: --source x → x_playwright*
        meta = REGISTRY.get(name)
        if meta and meta.get("category") == raw.strip().lower():
            return True
        if name.startswith(raw.strip().lower() + "_") or name.startswith(
            raw.strip().lower() + " "
        ):
            return True
    return False


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Smoke-test Data-Crawler-Task sources independently."
    )
    parser.add_argument(
        "--query",
        default="news",
        help='Search query passed to each source (default: "news")',
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Max items to request per source (default: 5)",
    )
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        metavar="NAME",
        help=(
            "Run only this source / option. Accepts REGISTRY keys "
            "(x_playwright, newsapi, …), categories (x, reddit, news, …), "
            "or extra option names. Can be repeated."
        ),
    )
    parser.add_argument(
        "--registry-only",
        action="store_true",
        help="Only run default REGISTRY fetchers (skip extra mode checks).",
    )
    parser.add_argument(
        "--extra-only",
        action="store_true",
        help="Only run extra mode checks (profile / subreddit / trending / …).",
    )
    parser.add_argument(
        "--with-comments",
        action="store_true",
        help="Also run comment-crawl options (slower).",
    )
    args = parser.parse_args(argv)

    if args.registry_only and args.extra_only:
        print("Choose at most one of --registry-only / --extra-only", file=sys.stderr)
        return 2

    extra_options = _build_extra_options(with_comments=args.with_comments)
    available = list(REGISTRY.keys()) + [opt.name for opt in extra_options]

    selected_registry: List[str] = []
    selected_extra: List[ExtraOption] = []

    if args.sources:
        unknown = []
        for name in args.sources:
            matched_registry = [
                key
                for key in REGISTRY
                if _match_name(key, [name]) and not args.extra_only
            ]
            matched_extra = [
                opt
                for opt in extra_options
                if _match_name(opt.name, [name]) or _match_name(opt.group, [name])
            ]
            if args.registry_only:
                matched_extra = []
            if args.extra_only:
                matched_registry = []

            for key in matched_registry:
                if key not in selected_registry:
                    selected_registry.append(key)
            for opt in matched_extra:
                if opt.name not in {s.name for s in selected_extra}:
                    selected_extra.append(opt)

            if not matched_registry and not matched_extra:
                # Also allow category → all adapters in that category
                cat = name.strip().lower()
                cat_keys = [
                    k for k, m in REGISTRY.items() if m["category"] == cat
                ]
                if cat_keys and not args.extra_only:
                    for key in cat_keys:
                        if key not in selected_registry:
                            selected_registry.append(key)
                elif not matched_extra:
                    unknown.append(name)

        if unknown:
            print("Unknown source(s):", ", ".join(unknown), file=sys.stderr)
            print("Available:", ", ".join(available), file=sys.stderr)
            return 2
        if not selected_registry and not selected_extra:
            print(
                "No matching sources for filter(s):",
                ", ".join(args.sources),
                file=sys.stderr,
            )
            return 2
    else:
        if not args.extra_only:
            selected_registry = list(REGISTRY.keys())
        if not args.registry_only:
            selected_extra = list(extra_options)

    total = len(selected_registry) + len(selected_extra)
    print(
        f"Testing {total} check(s) with query={args.query!r}, limit={args.limit}"
        f"{' (+ comments)' if args.with_comments else ''}"
    )
    print(f"Credentials: {_ROOT / '.env'}")
    if selected_extra:
        print(
            f"Extra defaults: X profile=@{DEFAULT_X_PROFILE}, "
            f"Reddit subreddit=r/{DEFAULT_REDDIT_SUBREDDIT}, "
            f"YouTube region={DEFAULT_YOUTUBE_REGION}"
        )

    results: List[SourceCheck] = []

    for name in selected_registry:
        print(f"  -> {name} ...", flush=True)
        results.append(
            _run_adapter(name, REGISTRY[name]["fetch"], args.query, args.limit)
        )

    for option in selected_extra:
        print(f"  -> {option.name} ...", flush=True)
        results.append(_run_extra_option(option, args.query, args.limit))

    _print_results(results)

    if any(r.status == "FAIL" for r in results):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
