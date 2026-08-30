"""Authenticated X search surfaces and their HTML/DOM parsers."""

from __future__ import annotations

import asyncio
import html as html_lib
import json
import logging
import os
import random
import re
from dataclasses import dataclass, field
from http.cookies import SimpleCookie
from typing import Any, Callable, Iterable
from urllib.parse import quote_plus, urlencode

from .extractors import _parse_aria_metrics, estimate_count, normalize_handle
from .models import CandidateEvidence
from .platforms import normalize_profile_url
from .querying import query_expansions

logger = logging.getLogger(__name__)


@dataclass
class PeopleCard:
    handle: str
    name: str | None = None
    snippet: str | None = None
    profile_url: str | None = None
    search_rank: int = 0
    discovery_sources: list[str] = field(default_factory=list)


def build_people_search_url(query: str) -> str:
    return f"https://x.com/search?q={quote_plus(query)}&src=typed_query&f=user"


def people_search_queries(query: str) -> list[str]:
    query = query.strip()
    return query_expansions(query) if query else []


def prepare_x_cookies(session: str | None) -> list[dict]:
    """Convert a raw cookie header or Playwright state into X cookies."""
    if not session:
        return []
    session = session.strip()
    if session.startswith("{"):
        try:
            state = json.loads(session)
            return [
                _cookie(c["name"], c["value"])
                for c in state.get("cookies", [])
                if c.get("name") in {"auth_token", "ct0", "twid"} and c.get("value")
            ]
        except Exception:
            return []
    if "=" not in session or ";" not in session:
        return [_cookie("auth_token", session)]
    parsed = SimpleCookie()
    parsed.load(session)
    cookies = []
    for name, morsel in parsed.items():
        if name in {"auth_token", "ct0", "twid"}:
            cookies.append(_cookie(name, morsel.value))
    return cookies


def _cookie(name: str, value: str) -> dict:
    return {
        "name": name,
        "value": value,
        "domain": ".x.com",
        "path": "/",
        "httpOnly": True,
        "secure": True,
        "sameSite": "Lax",
    }


def parse_people_cards_from_html(page_html: str, source_url: str | None = None) -> list[PeopleCard]:
    cards: list[PeopleCard] = []
    marker = re.compile(r'<div[^>]+data-testid=["\']UserCell["\']', re.I)
    starts = [match.start() for match in marker.finditer(page_html)]
    blocks = [page_html[start:end] for start, end in zip(starts, starts[1:] + [len(page_html)])]
    for block in blocks:
        card = _parse_user_cell_block(block, source_url)
        if card:
            cards.append(card)
    if not cards:
        for match in re.finditer(
            r'href=["\']/(?!i/|search|home|settings|notifications|messages|compose)([A-Za-z0-9_]{1,15})(?:["\'/])',
            page_html,
        ):
            handle = normalize_handle(match.group(1))
            if handle:
                cards.append(
                    PeopleCard(
                        handle=handle,
                        profile_url=f"https://x.com/{handle}",
                        discovery_sources=[source_url] if source_url else [],
                    )
                )
    return _dedupe_cards(cards)


def _parse_user_cell_block(block: str, source_url: str | None) -> PeopleCard | None:
    handles = re.findall(r"@([A-Za-z0-9_]{1,15})", html_lib.unescape(block))
    handle = normalize_handle(handles[0]) if handles else None
    if not handle:
        links = re.findall(
            r'href=["\']/(?!i/|search|home|settings|notifications|messages|compose)([A-Za-z0-9_]{1,15})(?:["\'/])',
            block,
        )
        handle = normalize_handle(links[0]) if links else None
    if not handle:
        return None
    text = _text(block)
    name = None
    if "@" + handle in text:
        name = text.split("@" + handle, 1)[0].strip() or None
        if name:
            name = re.split(r"\s{2,}|\n", name)[-1].strip() or name
    snippet = text
    if name:
        snippet = snippet.replace(name, "", 1).strip()
    snippet = snippet.replace("@" + handle, "", 1).strip()
    return PeopleCard(
        handle=handle,
        name=name,
        snippet=snippet[:300] if snippet else None,
        profile_url=f"https://x.com/{handle}",
        discovery_sources=[source_url] if source_url else [],
    )


def _text(markup: str) -> str:
    text = re.sub(r"<script.*?</script>|<style.*?</style>", " ", markup, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", html_lib.unescape(text)).strip()


def _dedupe_cards(cards: Iterable[PeopleCard]) -> list[PeopleCard]:
    seen: set[str] = set()
    out: list[PeopleCard] = []
    for card in cards:
        key = card.handle.lower()
        if key in seen:
            continue
        seen.add(key)
        card.search_rank = len(out) + 1
        out.append(card)
    return out


class XPeopleSearcher:
    def __init__(self, headless: bool = True, x_session: str | None = None, timeout_ms: int = 30000):
        self.headless = headless
        self.x_session = x_session
        self.timeout_ms = timeout_ms

    async def search(
        self,
        query: str,
        n: int,
        max_scrolls: int = 8,
    ) -> tuple[list[PeopleCard], dict]:
        card_groups: list[list[PeopleCard]] = []
        metas: list[dict] = []
        queries = people_search_queries(query)
        for search_query in queries:
            cards, meta = await self._search_single(
                search_query,
                n,
                max_scrolls=max(2, max_scrolls // 2),
            )
            metas.append(meta)
            card_groups.append(cards)
        deduped = _round_robin_cards(card_groups, limit=max(n * 8, 40))
        return deduped, {
            "source": "x_people_search",
            "queries": queries,
            "query_runs": metas,
            "logged_in": bool(
                prepare_x_cookies(self.x_session)
                or (self.x_session and os.path.exists(self.x_session))
            ),
            "cards_found": len(deduped),
        }

    async def _search_single(
        self,
        query: str,
        n: int,
        max_scrolls: int = 4,
    ) -> tuple[list[PeopleCard], dict]:
        url = build_people_search_url(query)
        try:
            from playwright.async_api import async_playwright

            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=self.headless)
                context_kwargs = {
                    "user_agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 Chrome/126 Safari/537.36"
                    ),
                    "viewport": {"width": 1280, "height": 1000},
                }
                if self.x_session and os.path.exists(self.x_session):
                    context_kwargs["storage_state"] = self.x_session
                context = await browser.new_context(**context_kwargs)
                cookies = prepare_x_cookies(self.x_session)
                if cookies:
                    await context.add_cookies(cookies)
                page = await context.new_page()
                page.set_default_timeout(self.timeout_ms)
                await page.goto(url, wait_until="domcontentloaded")
                await page.wait_for_timeout(2500)
                cards: list[PeopleCard] = []
                for _ in range(max_scrolls + 1):
                    cards = await _extract_cards_from_live_page(page, url)
                    if len(cards) >= n + 5:
                        break
                    await page.mouse.wheel(0, 2200)
                    await page.wait_for_timeout(1200)
                await browser.close()
                return cards[: max(n + 10, n)], {
                    "source": "x_people_search",
                    "url": url,
                    "logged_in": bool(cookies),
                    "cards_found": len(cards),
                }
        except Exception as exc:
            return await self._http_fallback(url, n, str(exc))

    async def _http_fallback(
        self,
        url: str,
        n: int,
        error: str,
    ) -> tuple[list[PeopleCard], dict]:
        import urllib.request

        def fetch() -> str:
            request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            return urllib.request.urlopen(request, timeout=self.timeout_ms / 1000).read().decode(
                "utf-8",
                "ignore",
            )

        try:
            page_html = await asyncio.to_thread(fetch)
            cards = parse_people_cards_from_html(page_html, url)
            return cards[: max(n + 10, n)], {
                "source": "x_people_search_http_fallback",
                "url": url,
                "playwright_error": error,
                "cards_found": len(cards),
            }
        except Exception as exc:
            return [], {
                "source": "x_people_search_failed",
                "url": url,
                "playwright_error": error,
                "fallback_error": str(exc),
                "cards_found": 0,
            }


async def _extract_cards_from_live_page(page, source_url: str) -> list[PeopleCard]:
    raw_cards = await page.locator('[data-testid="UserCell"]').evaluate_all(
        """
        cells => {
          const profileRe = new RegExp('^/[A-Za-z0-9_]{1,15}$');
          const handleRe = new RegExp('@([A-Za-z0-9_]{1,15})');
          return cells.map(cell => {
            const text = cell.innerText || '';
            const links = Array.from(cell.querySelectorAll('a[href^="/"]'));
            const link = links.find(a => profileRe.test(a.getAttribute('href') || ''));
            const handleMatch = text.match(handleRe);
            const handle = handleMatch ? handleMatch[1] : (link ? link.getAttribute('href').slice(1) : null);
            const lines = text.split('\\n').map(s => s.trim()).filter(Boolean);
            const atIndex = lines.findIndex(line => line === '@' + handle);
            const name = atIndex > 0 ? lines[atIndex - 1] : (lines[0] || null);
            return {handle, name, snippet: text};
          });
        }
        """
    )
    cards = []
    for raw in raw_cards:
        handle = normalize_handle(raw.get("handle"))
        if not handle:
            continue
        cards.append(
            PeopleCard(
                handle=handle,
                name=raw.get("name"),
                snippet=(raw.get("snippet") or "")[:300],
                profile_url=f"https://x.com/{handle}",
                discovery_sources=[source_url],
            )
        )
    return _dedupe_cards(cards)


def _round_robin_cards(groups: list[list[PeopleCard]], limit: int) -> list[PeopleCard]:
    """Balance candidates across query variants instead of favoring query one."""
    by_handle: dict[str, PeopleCard] = {}
    max_length = max((len(group) for group in groups), default=0)
    for index in range(max_length):
        for group in groups:
            if index >= len(group):
                continue
            card = group[index]
            key = card.handle.lower()
            if key in by_handle:
                existing = by_handle[key]
                existing.discovery_sources = list(dict.fromkeys(
                    existing.discovery_sources + card.discovery_sources
                ))
                continue
            card.search_rank = len(by_handle) + 1
            by_handle[key] = card
            if len(by_handle) >= limit:
                return list(by_handle.values())
    return list(by_handle.values())


def build_latest_search_query(topic_term: str, language: str = "en") -> str:
    escaped = topic_term.replace('"', " ").strip()
    return f'"{escaped}" min_faves:200 -is:retweet lang:{language}'


def build_latest_search_url(topic_term: str, language: str = "en") -> str:
    return "https://x.com/search?" + urlencode({
        "q": build_latest_search_query(topic_term, language),
        "src": "typed_query",
        "f": "live",
    })


@dataclass
class PostAuthor:
    handle: str
    display_name: str | None
    evidence: CandidateEvidence


class XPostSearcher:
    """Authenticated adapter for X's Latest post-search surface."""

    def __init__(self, headless: bool = True, x_session: str | None = None, timeout_ms: int = 30_000):
        self.headless = headless
        self.x_session = x_session
        self.timeout_ms = timeout_ms
        self.failed_terms: set[str] = set()

    async def search(
        self,
        topic_terms: list[str],
        *,
        language: str = "en",
        authors_per_query: int = 20,
        max_scrolls_per_query: int = 8,
    ) -> dict[str, list[PostAuthor]]:
        self.failed_terms = set()
        if not self.x_session:
            self.failed_terms = set(topic_terms)
            logger.warning(
                "X Latest discovery is unavailable because no authenticated X session is configured"
            )
            return {term: [] for term in topic_terms}
        from playwright.async_api import async_playwright

        output: dict[str, list[PostAuthor]] = {}
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=self.headless)
            context = await browser.new_context(viewport={"width": 1280, "height": 1000})
            cookies = prepare_x_cookies(self.x_session)
            if cookies:
                await context.add_cookies(cookies)
            try:
                for index, term in enumerate(topic_terms):
                    if index:
                        delay = random.uniform(1.0, 3.0)
                        logger.debug("X Latest pacing: waiting %.2fs before next keyword", delay)
                        await asyncio.sleep(delay)
                    logger.debug("X discovery: querying keyword=%r", term)
                    page = await context.new_page()
                    page.set_default_timeout(self.timeout_ms)
                    found: set[str] = set()
                    collected: list[PostAuthor] = []
                    seen_posts: set[str] = set()
                    try:
                        await page.goto(
                            build_latest_search_url(term, language),
                            wait_until="domcontentloaded",
                        )
                        await page.wait_for_timeout(2500)
                        for scroll in range(max_scrolls_per_query + 1):
                            for author in await extract_post_authors(page, term):
                                key = author.handle.lower()
                                if key not in found and len(found) >= authors_per_query:
                                    continue
                                found.add(key)
                                post_key = author.evidence.post_url or f"{key}:{author.evidence.posted_at}"
                                if post_key not in seen_posts:
                                    collected.append(author)
                                    seen_posts.add(post_key)
                            if len(found) >= authors_per_query or scroll == max_scrolls_per_query:
                                break
                            logger.debug(
                                "X Latest lane %r: scroll %d/%d, %d unique authors",
                                term,
                                scroll + 1,
                                max_scrolls_per_query,
                                len(found),
                            )
                            await page.mouse.wheel(0, 2400)
                            await page.wait_for_timeout(1200)
                    except Exception as exc:
                        self.failed_terms.add(term)
                        logger.warning("X Latest lane %r failed: %s", term, exc)
                    finally:
                        try:
                            await asyncio.wait_for(page.close(), timeout=5)
                        except Exception:
                            pass
                    output[term] = collected
                    logger.debug(
                        "X discovery: found %d unique handles keyword=%r",
                        len(found),
                        term,
                    )
            finally:
                for resource in (context, browser):
                    try:
                        await asyncio.wait_for(resource.close(), timeout=5)
                    except Exception:
                        pass
        return output


def enqueue_x_search_results(
    queue: Any,
    lanes: dict[str, list[PostAuthor]],
    related_terms: list[str],
    *,
    company_id: str | None = None,
    platform: str = "x",
    on_discovered: Callable[[str], None] | None = None,
    on_evidence: Callable[[str, CandidateEvidence], None] | None = None,
) -> int:
    """Send every X Latest author through the common URL-only producer."""
    sent = 0
    for term in related_terms:
        for author in lanes.get(term, []):
            profile_url = normalize_profile_url(f"https://x.com/{author.handle}")
            if on_discovered is not None:
                on_discovered(profile_url)
            if on_evidence is not None:
                on_evidence(profile_url, author.evidence)
            try:
                sent += int(queue.send(
                    profile_url,
                    company_id=company_id,
                    platform=platform,
                    handle=author.handle,
                ))
            except TypeError:
                sent += int(queue.send(profile_url))
    return sent


async def extract_post_authors(page: Any, topic_term: str) -> list[PostAuthor]:
    # Keep discovery compatible with both X's legacy tweet cards and its
    # schema.org SocialMediaPosting article surface.
    raw_posts = await page.locator("article").evaluate_all(
        """
        articles => {
          const content = (element, property) => {
            const node = element.querySelector(`meta[itemprop="${property}"], [itemprop="${property}"]`);
            return node ? (node.getAttribute('content') || node.getAttribute('href') || node.innerText || '') : '';
          };
          const asPath = value => {
            if (!value) return null;
            try { return new URL(value, location.origin).pathname; }
            catch (_) { return value; }
          };
          return articles
            .filter(article => {
              const legacy = article.getAttribute('data-testid') === 'tweet';
              const schema = /SocialMediaPosting/i.test(article.getAttribute('itemtype') || '')
                || article.getAttribute('itemprop') === 'hasPart';
              const hasStatus = Boolean(article.querySelector('a[href*="/status/"], meta[itemprop="url"], [itemprop="url"]'));
              return legacy || schema || hasStatus;
            })
            .map(article => {
              const time = article.querySelector('time');
              const timeHref = time && time.closest('a') ? time.closest('a').getAttribute('href') : null;
              const statusHref = article.querySelector('a[href*="/status/"]')?.getAttribute('href');
              const href = asPath(timeHref || statusHref || content(article, 'url') || article.getAttribute('itemid'));
              const textNode = article.querySelector('[data-testid="tweetText"]');
              const userName = article.querySelector('[data-testid="User-Name"]');
              const schemaName = content(article, 'alternateName') || content(article, 'name');
              const aria = Array.from(article.querySelectorAll('[aria-label]'))
                .map(e => e.getAttribute('aria-label')).join(' | ');
              return {
                href,
                created_at: (time && time.getAttribute('datetime')) || content(article, 'datePublished') || content(article, 'dateCreated') || null,
                text: textNode ? textNode.innerText : (article.innerText || content(article, 'text')),
                user_name: userName ? userName.innerText : schemaName,
                aria,
                schema_metrics: {
                  replies: content(article, 'commentCount') || content(article, 'replyCount'),
                  reposts: content(article, 'repostCount') || content(article, 'shareCount'),
                  likes: content(article, 'likeCount'),
                  views: content(article, 'viewCount') || content(article, 'interactionCount'),
                },
              };
            });
        }
        """
    )
    output: list[PostAuthor] = []
    seen: set[str] = set()
    for raw in raw_posts:
        match = re.match(r"^/([A-Za-z0-9_]{1,15})/status/([0-9]+)", raw.get("href") or "", re.I)
        handle = normalize_handle(match.group(1)) if match else None
        if not handle or handle.lower() in seen:
            continue
        seen.add(handle.lower())
        user_lines = [
            line.strip()
            for line in (raw.get("user_name") or "").splitlines()
            if line.strip()
        ]
        display_name = next((line for line in user_lines if not line.startswith("@")), None)
        metrics = _parse_aria_metrics(raw.get("aria") or "")
        schema_metrics = raw.get("schema_metrics") or {}
        for metric_name in ("replies", "reposts", "likes", "views"):
            if metrics.get(metric_name) is None and schema_metrics.get(metric_name) is not None:
                metrics[metric_name] = estimate_count(str(schema_metrics[metric_name]))
        href = raw.get("href")
        output.append(PostAuthor(
            handle=handle,
            display_name=display_name,
            evidence=CandidateEvidence(
                type="x_post",
                query=topic_term,
                post_url=f"https://x.com{href}",
                posted_at=raw.get("created_at"),
                text=(raw.get("text") or "")[:500],
                likes=metrics.get("likes"),
                reposts=metrics.get("reposts"),
                replies=metrics.get("replies"),
                views=metrics.get("views"),
            ),
        ))
    return output
