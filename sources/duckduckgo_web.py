"""
DuckDuckGo web search → social-host filter → trafilatura extract.

CLI-first (not in REGISTRY). Returns flat raw_insights-shaped rows.

  cd code/Backend/Backend
  python -m api.sources.duckduckgo_web --query "agentic AI marketing" --limit 5 --timelimit w
  python -m api.sources.duckduckgo_web --query "agentic AI marketing" --limit 5 --json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from ddgs import DDGS
from trafilatura import extract, fetch_url

from .utils import crawled_at_now, human_delay, web_search_query, filter_rows_by_query, overfetch_limit

logger = logging.getLogger(__name__)

SOURCE_KEY = "duckduckgo_web"
DEFAULT_REGION = "au-en"
DEFAULT_TIMELIMIT = "w"
ALLOWED_TIMELIMITS = frozenset({"d", "w", "m", "y"})

EXTRACT_DELAY_MIN = 0.4
EXTRACT_DELAY_MAX = 1.0

SOCIAL_HOST_SUFFIXES = (
    "linkedin.com",
    "facebook.com",
    "fb.com",
    "instagram.com",
    "tiktok.com",
    "reddit.com",
    "x.com",
    "twitter.com",
    "youtube.com",
    "youtu.be",
    "threads.net",
    "pinterest.com",
    "snapchat.com",
    "tumblr.com",
    "truthsocial.com",
    "bsky.app",
    "mastodon.social",
    "t.co",
    "lnkd.in",
    "vm.tiktok.com",
)


def _hostname(url: str) -> str:
    try:
        host = (urlparse(url).hostname or "").lower().strip()
    except Exception:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host


def _is_social_url(url: str) -> bool:
    host = _hostname(url)
    if not host:
        return True
    for suffix in SOCIAL_HOST_SUFFIXES:
        if host == suffix or host.endswith("." + suffix):
            return True
    return False


def _normalize_url(url: str) -> str:
    return (url or "").strip()


def _parse_published_ts(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        ts = int(value)
        # trafilatura sometimes returns ms; treat huge values as ms
        if ts > 10_000_000_000:
            ts //= 1000
        return ts
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        pass
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d %b %Y", "%B %d, %Y"):
        try:
            dt = datetime.strptime(s[:32], fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            continue
    return None


def _search_urls(
    query: str,
    *,
    region: str,
    timelimit: Optional[str],
    max_results: int,
) -> List[Dict[str, str]]:
    kwargs: Dict[str, Any] = {
        "query": query,
        "region": region,
        "safesearch": "moderate",
        "max_results": max_results,
    }
    if timelimit:
        kwargs["timelimit"] = timelimit

    try:
        raw = DDGS().text(**kwargs) or []
    except Exception as exc:
        logger.exception("DDGS text search failed: %s", exc)
        return []

    out: List[Dict[str, str]] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        href = _normalize_url(item.get("href") or item.get("url") or "")
        if not href:
            continue
        parsed = urlparse(href)
        if parsed.scheme not in ("http", "https"):
            continue
        if _is_social_url(href):
            continue
        key = href.rstrip("/").lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "title": (item.get("title") or "").strip(),
                "href": href,
                "body": (item.get("body") or item.get("snippet") or "").strip(),
            }
        )
        if len(out) >= max_results:
            break
    return out


def _extract_article(url: str) -> Dict[str, Any]:
    """Fetch URL with trafilatura; return title/text/author/published_ts (may be empty)."""
    try:
        downloaded = fetch_url(url)
    except Exception as exc:
        logger.warning("trafilatura fetch_url failed for %s: %s", url, exc)
        return {}
    if not downloaded:
        return {}

    try:
        raw = extract(
            downloaded,
            output_format="json",
            with_metadata=True,
            include_comments=False,
        )
    except Exception as exc:
        logger.warning("trafilatura extract failed for %s: %s", url, exc)
        return {}
    if not raw:
        return {}

    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        return {"text": raw if isinstance(raw, str) else ""}

    if not isinstance(data, dict):
        return {}

    return {
        "title": (data.get("title") or "").strip(),
        "text": (data.get("text") or "").strip(),
        "author": (data.get("author") or "").strip() or None,
        "published_ts": _parse_published_ts(
            data.get("date") or data.get("published") or data.get("date_published")
        ),
    }


def _map_row(
    *,
    url: str,
    serp_title: str,
    serp_body: str,
    extracted: Dict[str, Any],
) -> Dict[str, Any]:
    title = (extracted.get("title") or "").strip() or serp_title
    text = (extracted.get("text") or "").strip() or serp_body
    return {
        "post_id": f"ddg_web:{hash(url)}",
        "source": SOURCE_KEY,
        "url": url,
        "title": title,
        "text": text,
        "text_html": "",
        "published_ts": extracted.get("published_ts"),
        "crawled_at": crawled_at_now(),
        "author": extracted.get("author"),
        "engagement": None,
    }


def fetch(
    query: str,
    limit: int = 20,
    *,
    region: str = DEFAULT_REGION,
    timelimit: Optional[str] = DEFAULT_TIMELIMIT,
) -> List[Dict]:
    """
    Registry-compatible fetcher: fetch(query, limit) -> list[dict] raw_insights rows.

    limit is both DDGS max_results and max rows returned (after social filter /
    extract failures, length may be <= limit).
    """
    q = web_search_query(query) or (query or "").strip()
    if not q:
        logger.warning("duckduckgo_web: empty query")
        return []

    lim = max(1, int(limit))
    fetch_n = overfetch_limit(lim) or lim
    tl = (timelimit or "").strip().lower() or None
    if tl and tl not in ALLOWED_TIMELIMITS:
        logger.warning("duckduckgo_web: invalid timelimit=%r; ignoring", timelimit)
        tl = None

    candidates = _search_urls(
        q, region=region or DEFAULT_REGION, timelimit=tl, max_results=fetch_n
    )
    out: List[Dict] = []
    for i, hit in enumerate(candidates):
        url = hit["href"]
        extracted = _extract_article(url)
        out.append(
            _map_row(
                url=url,
                serp_title=hit.get("title") or "",
                serp_body=hit.get("body") or "",
                extracted=extracted,
            )
        )
        if i + 1 < len(candidates):
            human_delay(EXTRACT_DELAY_MIN, EXTRACT_DELAY_MAX)

    out = filter_rows_by_query(out, query, limit=lim, soft=True)

    logger.info(
        "duckduckgo_web fetched %d rows for query=%r shaped=%r region=%s timelimit=%s",
        len(out),
        query,
        q,
        region,
        tl,
    )
    return out


def _print_summary(rows: List[Dict], duration_ms: int) -> None:
    print(f"duckduckgo_web rows={len(rows)} duration_ms={duration_ms}")
    for i, row in enumerate(rows, start=1):
        print(f"{i}. {row.get('title')}")
        print(f"   url:  {row.get('url')}")
        text = (row.get("text") or "")[:120].replace("\n", " ")
        if text:
            print(f"   text: {text}...")


def _cli_main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="DuckDuckGo web search + trafilatura extract (CLI-first source)."
    )
    parser.add_argument(
        "--query",
        default="artificial intelligence marketing",
        help='Search query (default: "artificial intelligence marketing")',
    )
    parser.add_argument("--limit", type=int, default=5, help="Max articles to return")
    parser.add_argument(
        "--region",
        default=DEFAULT_REGION,
        help=f"DDGS region (default: {DEFAULT_REGION})",
    )
    parser.add_argument(
        "--timelimit",
        default=DEFAULT_TIMELIMIT,
        choices=["d", "w", "m", "y", "none"],
        help="Recency filter: d/w/m/y, or none (default: w)",
    )
    parser.add_argument(
        "--json", action="store_true", help="Print full JSON output"
    )
    args = parser.parse_args(argv)

    tl: Optional[str] = None if args.timelimit == "none" else args.timelimit
    started = time.perf_counter()
    rows = fetch(
        args.query,
        limit=args.limit,
        region=args.region,
        timelimit=tl,
    )
    duration_ms = int((time.perf_counter() - started) * 1000)

    if args.json:
        print(json.dumps(rows, indent=2, ensure_ascii=False))
    else:
        _print_summary(rows, duration_ms)

    return 0 if rows else 1


if __name__ == "__main__":
    raise SystemExit(_cli_main())
