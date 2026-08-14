from __future__ import annotations

import asyncio
import html as html_lib
import json
import os
import re
from dataclasses import dataclass, field
from http.cookies import SimpleCookie
from typing import Iterable
from urllib.parse import quote_plus

from exceptions import SessionExpiredError

from .extractors import normalize_handle
from .querying import query_expansions


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
    if not session:
        return []
    session = session.strip()
    if session.startswith("{"):
        try:
            state = json.loads(session)
            return [_cookie(c["name"], c["value"]) for c in state.get("cookies", []) if c.get("name") in {"auth_token", "ct0", "twid"} and c.get("value")]
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
    return {"name": name, "value": value, "domain": ".x.com", "path": "/", "httpOnly": True, "secure": True, "sameSite": "Lax"}


def parse_people_cards_from_html(page_html: str, source_url: str | None = None) -> list[PeopleCard]:
    cards: list[PeopleCard] = []
    marker = re.compile(r'<div[^>]+data-testid=["\']UserCell["\']', re.I)
    starts = [m.start() for m in marker.finditer(page_html)]
    blocks = [page_html[start:end] for start, end in zip(starts, starts[1:] + [len(page_html)])]
    for block in blocks:
        card = _parse_user_cell_block(block, source_url)
        if card:
            cards.append(card)
    if not cards:
        for match in re.finditer(r'href=["\']/(?!i/|search|home|settings|notifications|messages|compose)([A-Za-z0-9_]{1,15})(?:["\'/])', page_html):
            handle = normalize_handle(match.group(1))
            if handle:
                cards.append(PeopleCard(handle=handle, profile_url=f"https://x.com/{handle}", discovery_sources=[source_url] if source_url else []))
    return _dedupe_cards(cards)


def _parse_user_cell_block(block: str, source_url: str | None) -> PeopleCard | None:
    handles = re.findall(r'@([A-Za-z0-9_]{1,15})', html_lib.unescape(block))
    handle = normalize_handle(handles[0]) if handles else None
    if not handle:
        links = re.findall(r'href=["\']/(?!i/|search|home|settings|notifications|messages|compose)([A-Za-z0-9_]{1,15})(?:["\'/])', block)
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
    return PeopleCard(handle=handle, name=name, snippet=snippet[:300] if snippet else None, profile_url=f"https://x.com/{handle}", discovery_sources=[source_url] if source_url else [])


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
        all_cards: list[PeopleCard] = []
        card_groups: list[list[PeopleCard]] = []
        metas: list[dict] = []
        queries = people_search_queries(query)
        for search_query in queries:
            cards, meta = await self._search_single(search_query, n, max_scrolls=max(2, max_scrolls // 2))
            metas.append(meta)
            all_cards.extend(cards)
            card_groups.append(cards)
        deduped = _round_robin_cards(card_groups, limit=max(n * 8, 40))
        return deduped, {"source": "x_people_search", "queries": queries, "query_runs": metas, "logged_in": bool(prepare_x_cookies(self.x_session) or (self.x_session and os.path.exists(self.x_session))), "cards_found": len(deduped)}

    async def _search_single(self, query: str, n: int, max_scrolls: int = 4) -> tuple[list[PeopleCard], dict]:
        url = build_people_search_url(query)
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=self.headless)
                context_kwargs = {"user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/126 Safari/537.36", "viewport": {"width": 1280, "height": 1000}}
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
                page_url = page.url.lower()
                login_wall = (
                    "/login" in page_url
                    or "/i/flow/login" in page_url
                    or await page.locator('input[autocomplete="username"]').count() > 0
                )
                if login_wall:
                    await browser.close()
                    raise SessionExpiredError(
                        "x_influencer_discovery",
                        "X login wall detected — refresh X_AUTH_TOKEN and X_CT0",
                    )
                cards: list[PeopleCard] = []
                for _ in range(max_scrolls + 1):
                    cards = await _extract_cards_from_live_page(page, url)
                    if len(cards) >= n + 5:
                        break
                    await page.mouse.wheel(0, 2200)
                    await page.wait_for_timeout(1200)
                await browser.close()
                return cards[: max(n + 10, n)], {"source": "x_people_search", "url": url, "logged_in": bool(cookies), "cards_found": len(cards)}
        except SessionExpiredError:
            raise
        except Exception as exc:
            return await self._http_fallback(url, n, str(exc))

    async def _http_fallback(self, url: str, n: int, error: str) -> tuple[list[PeopleCard], dict]:
        import urllib.request
        def fetch() -> str:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            return urllib.request.urlopen(req, timeout=self.timeout_ms / 1000).read().decode("utf-8", "ignore")
        try:
            page_html = await asyncio.to_thread(fetch)
            cards = parse_people_cards_from_html(page_html, url)
            return cards[: max(n + 10, n)], {"source": "x_people_search_http_fallback", "url": url, "playwright_error": error, "cards_found": len(cards)}
        except Exception as exc:
            return [], {"source": "x_people_search_failed", "url": url, "playwright_error": error, "fallback_error": str(exc), "cards_found": 0}


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
        cards.append(PeopleCard(handle=handle, name=raw.get("name"), snippet=(raw.get("snippet") or "")[:300], profile_url=f"https://x.com/{handle}", discovery_sources=[source_url]))
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
