from __future__ import annotations

import asyncio
import html as html_lib
import logging
import re
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import TYPE_CHECKING, Any, Callable
from urllib.parse import parse_qsl, urlencode, urlparse, urlsplit, urlunsplit

from .browser import PlaywrightFetcher, is_safe_public_url
from .extractors import extract_handles_from_html, normalize_handle
from .models import CandidateEvidence
from .platforms import normalize_profile_url
from .querying import text_matches_query

logger = logging.getLogger(__name__)
_X_PROFILE_HOSTS = {"x.com", "www.x.com", "twitter.com", "www.twitter.com"}

if TYPE_CHECKING:
    from .classification import ArticleLinkFilter


class SearchResults(list[dict[str, str]]):
    """Search results plus best-effort per-query failure metadata."""

    def __init__(self, values: list[dict[str, str]], failed_queries: list[str] | tuple[str, ...] = ()):
        super().__init__(values)
        self.failed_queries = tuple(failed_queries)


@dataclass
class Candidate:
    handle: str
    discovery_sources: set[str] = field(default_factory=set)
    display_name: str | None = None
    evidence: list[CandidateEvidence] = field(default_factory=list)

    def add_evidence(self, item: CandidateEvidence) -> None:
        if item.type != "x_post" and item.source_url:
            for index, existing in enumerate(self.evidence):
                if existing.type == "x_post" or existing.source_url != item.source_url:
                    continue
                # One article is one independent evidence item even when it is
                # returned by several search queries. Prefer parsed list-entry
                # evidence over the lighter search-result snippet.
                if existing.type == "public_search_result" and item.type != "public_search_result":
                    self.evidence[index] = item
                self.discovery_sources.add(item.source_url)
                return
        key = item.to_dict()
        if not any(existing.to_dict() == key for existing in self.evidence):
            self.evidence.append(item)
        if item.source_url:
            self.discovery_sources.add(item.source_url)


class PublicDiscoveryCandidates(dict[str, Candidate]):
    """Public candidates plus discovery failures for operational reporting."""

    def __init__(
        self,
        *args: Any,
        failed_queries: list[str] | tuple[str, ...] = (),
        failed_article_urls: list[str] | tuple[str, ...] = (),
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self.failed_queries = tuple(failed_queries)
        self.failed_article_urls = tuple(failed_article_urls)


def public_query_patterns(topic_terms: list[str]) -> list[str]:
    return [search_query for _, search_query in public_search_lanes(topic_terms)]


def public_search_lanes(topic_terms: list[str]) -> list[tuple[str, str]]:
    if not topic_terms:
        return []
    templates = (
        "top 40 {term} influencers on X",
        "best {term} experts to follow on X",
        "{term} thought leaders to follow on X",
    )
    field, *related_terms = topic_terms
    lanes = [(field, template.format(term=field)) for template in templates]
    lanes.extend(
        (term, templates[index % len(templates)].format(term=term))
        for index, term in enumerate(related_terms)
    )
    return lanes


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


def canonical_article_url(value: str) -> str:
    """Deduplicate public articles across fragments and tracking parameters."""
    parsed = urlsplit(value)
    query = urlencode(sorted(
        (key, item) for key, item in parse_qsl(parsed.query, keep_blank_values=True)
        if not key.casefold().startswith("utm_")
    ))
    path = parsed.path.rstrip("/") or "/"
    return urlunsplit((parsed.scheme.casefold(), parsed.netloc.casefold(), path, query, ""))


def _is_x_or_twitter_url(value: str) -> bool:
    return (urlparse(value).hostname or "").casefold() in _X_PROFILE_HOSTS


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
    failed_queries: list[str] = []
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
            failed_queries.append(search_query)
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
    return SearchResults(results, failed_queries)


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


async def discover_public_candidates(
    topic_terms: list[str],
    fetcher: PlaywrightFetcher,
    *,
    candidates_per_article: int = 40,
    article_filter: ArticleLinkFilter | None = None,
) -> dict[str, Candidate]:
    """Extract each unique qualified article without a global candidate cap."""
    candidates: dict[str, Candidate] = {}
    failed_queries: list[str] = []
    lanes: list[dict[str, Any]] = []
    unique_topic_terms = list(dict.fromkeys(topic_term for topic_term, _ in public_search_lanes(topic_terms)))
    for topic_term in unique_topic_terms:
        logger.debug("public discovery: querying keyword=%r", topic_term)
    for topic_term, search_query in public_search_lanes(topic_terms):
        results = await asyncio.to_thread(search_duckduckgo, [search_query], max_results=10)
        failed_queries.extend(getattr(results, "failed_queries", ()))
        safe_results: list[dict[str, str]] = []
        for result in results:
            if not await asyncio.to_thread(is_safe_public_url, result["url"]):
                continue
            safe_results.append(result)
        lanes.append({
            "topic_term": topic_term,
            "results": results,
            "safe_results": safe_results,
        })

    # Canonicalize before filtration so the model sees each public article once.
    # The internal ID then remains stable even when several search lanes found it.
    articles_by_url: dict[str, dict[str, str]] = {}
    for lane in lanes:
        canonical_results: list[dict[str, str]] = []
        for result in lane["safe_results"]:
            canonical_url = canonical_article_url(result["url"])
            article = articles_by_url.setdefault(canonical_url, {
                "id": f"article_{len(articles_by_url)}",
                "url": canonical_url,
                "title": result["title"],
                "search_term": lane["topic_term"],
            })
            canonical_results.append({**result, "url": canonical_url, "_article_id": article["id"]})
        lane["safe_results"] = canonical_results
    articles = list(articles_by_url.values())
    if article_filter and article_filter.enabled():
        _log_article_links("before LLM filtering", articles)
        logger.debug(
            "public-search article filter: classifying %d search-result links (%d unique URLs)",
            len(articles),
            len({article["url"] for article in articles}),
        )
        qualified = await asyncio.to_thread(
            article_filter.filter_articles,
            topic_terms[0],
            topic_terms[1:],
            articles,
        )
        qualified_ids = {article["id"] for article in qualified}
        for lane in lanes:
            lane["safe_results"] = [
                result
                for result in lane["safe_results"]
                if result["_article_id"] in qualified_ids
            ]
        _log_article_links("after LLM filtering", qualified)
        logger.debug(
            "public-search article filter: retained %d/%d search-result links (%d unique URLs)",
            len(qualified),
            len(articles),
            len({article["url"] for article in qualified}),
        )

    article_sources: dict[str, dict[str, Any]] = {}
    direct_x_urls: set[str] = set()
    for lane in lanes:
        for result in lane["safe_results"]:
            url = result["url"]
            if _is_x_or_twitter_url(url):
                direct_x_urls.add(url)
                handle = normalize_handle(url)
                if handle:
                    _add_evidence_candidate(candidates, handle, CandidateEvidence(
                        type="public_search_result",
                        query=lane["topic_term"],
                        source_url=url,
                        source_domain=urlparse(url).netloc,
                        source_title=result["title"],
                        context=result["snippet"][:500],
                    ))
                continue
            canonical_url = canonical_article_url(url)
            canonical_result = {**result, "url": canonical_url}
            source = article_sources.setdefault(canonical_url, {"result": canonical_result, "queries": []})
            if lane["topic_term"] not in source["queries"]:
                source["queries"].append(lane["topic_term"])

    article_urls = list(article_sources)
    retained_unique_urls = {
        item["url"]
        for lane in lanes
        for item in lane["safe_results"]
    }
    logger.debug(
        "public-search retrieval: fetching %d unique non-X article links from %d terms "
        "(%d retained unique URLs; %d direct X/Twitter URLs skipped)",
        len(article_urls),
        len(lanes),
        len(retained_unique_urls),
        len(direct_x_urls),
    )
    pages = await fetcher.fetch_many(article_urls, concurrency=1)
    pages_by_url = {page.url: page for page in pages}
    failed_article_urls = [
        url
        for url in article_urls
        if (page := pages_by_url.get(url)) is None or page.status != "ok" or not page.html
    ]
    if failed_article_urls:
        logger.warning(
            "public-search article retrieval failed for %d/%d retained articles",
            len(failed_article_urls),
            len(article_urls),
        )

    for url, source in article_sources.items():
        result = source["result"]
        query = source["queries"][0]
        page = pages_by_url.get(url)
        article_handles: set[str] = set()
        if page and page.html:
            ranked_items = extract_ranked_list_evidence(
                page.html,
                page.url,
                query=query,
                source_title=result["title"],
            )
            for handle, evidence in ranked_items:
                if len(article_handles) >= candidates_per_article:
                    break
                _add_evidence_candidate(candidates, handle, evidence)
                article_handles.add(handle.lower())

        # Search snippets can expose a valid X handle even when the article
        # fetch fails, but they share the same per-article allowance.
        for handle in extract_handles_from_html(f"{result['title']} {result['snippet']}"):
            if len(article_handles) >= candidates_per_article:
                break
            evidence = CandidateEvidence(
                type="public_search_result",
                query=query,
                source_url=url,
                source_domain=urlparse(url).netloc,
                source_title=result["title"],
                context=result["snippet"][:500],
            )
            _add_evidence_candidate(candidates, handle, evidence)
            article_handles.add(handle.lower())

        logger.debug(
            "public-search article: extracted %d/%d handles | %s",
            len(article_handles),
            candidates_per_article,
            url,
        )

    for topic_term in unique_topic_terms:
        handles_for_term = {
            candidate.handle.lower()
            for candidate in candidates.values()
            if any(evidence.query == topic_term for evidence in candidate.evidence)
        }
        logger.debug("public discovery: found %d unique handles keyword=%r", len(handles_for_term), topic_term)
    return PublicDiscoveryCandidates(
        candidates,
        failed_queries=failed_queries,
        failed_article_urls=failed_article_urls,
    )


def enqueue_public_candidates(
    queue: Any,
    candidates: dict[str, Candidate],
    *,
    company_id: str | None = None,
    platform: str = "x",
    on_discovered: Callable[[str], None] | None = None,
    on_evidence: Callable[[str, CandidateEvidence], None] | None = None,
) -> int:
    """Submit public profiles discovered during this run."""
    sent = 0
    for candidate in candidates.values():
        url = normalize_profile_url(f"https://x.com/{candidate.handle}")
        if on_discovered is not None:
            on_discovered(url)
        if on_evidence is not None:
            for evidence in candidate.evidence:
                on_evidence(url, evidence)
        try:
            sent += int(queue.send(url, company_id=company_id, platform=platform, handle=candidate.handle))
        except TypeError:
            sent += int(queue.send(url))
    return sent


def _public_candidate_priority(candidate: Candidate) -> tuple[int, int, str]:
    independent_sources = {
        evidence.source_url
        for evidence in candidate.evidence
        if evidence.source_url and evidence.type != "x_post"
    }
    source_ranks = [
        evidence.source_rank
        for evidence in candidate.evidence
        if evidence.source_rank is not None
    ]
    best_rank = min(source_ranks, default=10_000)
    return (-len(independent_sources), best_rank, candidate.handle.lower())


def _log_article_links(stage: str, articles: list[dict[str, str]]) -> None:
    logger.debug("public-search article links %s:", stage)
    for article in articles:
        logger.debug(
            "  [%s] %s | %s",
            article["search_term"],
            article["title"] or "(untitled)",
            article["url"],
        )


class _LinkContextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.stack: list[dict[str, Any]] = []
        self.items: list[tuple[str, str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = dict(attrs)
        self.stack.append({"tag": tag, "href": attrs_dict.get("href"), "text": []})

    def handle_data(self, data: str) -> None:
        for node in self.stack:
            node["text"].append(data)

    def handle_endtag(self, tag: str) -> None:
        if not self.stack:
            return
        index = next((i for i in range(len(self.stack) - 1, -1, -1) if self.stack[i]["tag"] == tag), None)
        if index is None:
            return
        node = self.stack[index]
        text = re.sub(r"\s+", " ", " ".join(node["text"])).strip()
        if node.get("href"):
            parent_text = ""
            if index:
                parent_text = re.sub(r"\s+", " ", " ".join(self.stack[index - 1]["text"])).strip()
            self.items.append((str(node["href"]), text, parent_text))
        del self.stack[index:]


def extract_ranked_list_evidence(
    page_html: str,
    source_url: str,
    *,
    query: str,
    source_title: str | None = None,
) -> list[tuple[str, CandidateEvidence]]:
    """Extract X profile links in appearance order with nearby list metadata."""
    parser = _LinkContextParser()
    parser.feed(page_html)
    output: list[tuple[str, CandidateEvidence]] = []
    seen: set[str] = set()
    for href, anchor_text, context in parser.items:
        handle = normalize_handle(href)
        if not handle or handle.lower() in seen:
            continue
        if not _is_x_or_twitter_url(href):
            continue
        seen.add(handle.lower())
        href_index = page_html.find(href)
        nearby_html = page_html[max(0, href_index - 250): href_index + len(href) + 500] if href_index >= 0 else ""
        nearby_text = re.sub(r"<[^>]+>", " ", nearby_html)
        nearby_text = re.sub(r"\s+", " ", html_lib.unescape(nearby_text)).strip()
        combined = re.sub(r"\s+", " ", f"{nearby_text} {anchor_text} {context}").strip()
        rank_context = re.sub(r"\s+", " ", f"{anchor_text} {context}").strip()
        explicit_rank = re.search(r"(?:^|\s)#?([1-9][0-9]{0,2})[.)\s]", rank_context)
        rank = int(explicit_rank.group(1)) if explicit_rank else len(output) + 1
        output.append((handle, CandidateEvidence(
            type="ranked_public_list",
            query=query,
            source_url=source_url,
            source_domain=urlparse(source_url).netloc,
            source_rank=rank,
            source_name=anchor_text.lstrip("@").strip() or None,
            source_bio=context[:500] or None,
            source_title=source_title,
            context=combined[:500] or None,
        )))
    return output


def _add_evidence_candidate(candidates: dict[str, Candidate], raw_handle: str, evidence: CandidateEvidence) -> Candidate:
    candidate = _add_candidate(candidates, raw_handle, evidence.source_url or evidence.type)
    candidate.add_evidence(evidence)
    return candidate


def _add_candidate(candidates: dict[str, Candidate], raw_handle: str, source: str) -> Candidate:
    handle = normalize_handle(raw_handle)
    if not handle:
        raise ValueError(f"Invalid handle after discovery: {raw_handle}")
    key = handle.lower()
    if key not in candidates:
        candidates[key] = Candidate(handle=handle)
    candidates[key].discovery_sources.add(source)
    return candidates[key]
