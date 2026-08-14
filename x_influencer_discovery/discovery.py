from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

from .browser import PlaywrightFetcher, is_safe_public_url
from .extractors import extract_handles_from_html, normalize_handle
from .querying import text_matches_query

logger = logging.getLogger(__name__)


@dataclass
class Candidate:
    handle: str
    discovery_sources: set[str] = field(default_factory=set)


def query_patterns(query: str, num_queries: int = 3) -> list[str]:
    """Generic public-search queries. No topic-specific hard-coding."""
    query = query.strip()
    patterns = [
        f'top {query} influencers X',
        f'top 10 {query} influencers X',
        f'best {query} experts to follow on X',
        f'top {query} thought leaders X',
        f'{query} accounts to follow on X',
    ]
    return patterns[: max(0, num_queries)]


def search_duckduckgo(
    queries: list[str],
    *,
    max_results: int = 10,
    timeout: int = 15,
) -> list[dict[str, str]]:
    """Search with the DDGS Python client using its default backend selection.

    Searches are intentionally sequential to avoid sending a burst of requests
    to providers. A failure in one query does not discard successful results
    from the other queries. This matches the established Data-Crawler-Task
    integration and avoids hard-pinning DDGS to DuckDuckGo's HTML endpoint.
    """
    from ddgs import DDGS

    client = DDGS(timeout=timeout)
    results: list[dict[str, str]] = []
    for search_query in queries:
        try:
            raw_results: list[dict[str, Any]] = client.text(
                search_query,
                region="au-en",
                safesearch="moderate",
                timelimit="y",
                max_results=max_results,
            )
        except Exception as exc:
            logger.warning("DuckDuckGo search failed for %r: %s", search_query, exc)
            continue
        for result in raw_results or []:
            url = str(result.get("href") or "").strip()
            if not url.startswith(("http://", "https://")):
                continue
            results.append({
                "query": search_query,
                "title": str(result.get("title") or "").strip(),
                "url": url,
                "snippet": str(result.get("body") or "").strip(),
            })
    return results


async def discover_candidates(query: str, n: int, fetcher: PlaywrightFetcher, num_queries: int = 3) -> dict[str, Candidate]:
    """Discover candidates only from public search/list pages.

    This intentionally avoids curated/manual seed handles so the pipeline works
    fairly for unknown domains.
    """
    candidates: dict[str, Candidate] = {}
    patterns = query_patterns(query, num_queries=num_queries)
    search_results = await asyncio.to_thread(search_duckduckgo, patterns)
    source_results: list[tuple[str, str]] = []
    for result in search_results:
        title = result["title"]
        source_url = result["url"]
        searchable_text = f"{title} {result['snippet']} {source_url}"
        if not text_matches_query(searchable_text, query):
            continue
        if not await asyncio.to_thread(is_safe_public_url, source_url):
            logger.warning("Skipping non-public search result URL: %s", source_url)
            continue
        source_results.append((title, source_url))
        for handle in extract_handles_from_html(searchable_text):
            _add_candidate(candidates, handle, source_url)

    # Fetch top list/source pages to extract handles from their body.
    source_urls = [u for _, u in source_results]
    non_x_sources = [u for u in dict.fromkeys(source_urls) if "x.com/" not in u and "twitter.com/" not in u][: max(20, n * 4)]
    source_pages = await fetcher.fetch_many(non_x_sources, concurrency=4)
    for page in source_pages:
        if not page.html:
            continue
        for handle in extract_handles_from_html(page.html):
            _add_candidate(candidates, handle, page.url)
    return candidates


def _add_candidate(candidates: dict[str, Candidate], raw_handle: str, source: str) -> Candidate:
    handle = normalize_handle(raw_handle)
    if not handle:
        raise ValueError(f"Invalid handle after discovery: {raw_handle}")
    key = handle.lower()
    if key not in candidates:
        candidates[key] = Candidate(handle=handle)
    candidates[key].discovery_sources.add(source)
    return candidates[key]

