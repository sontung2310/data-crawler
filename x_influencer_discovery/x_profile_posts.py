from __future__ import annotations

import asyncio
import logging
import math
import random
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .browser import _capture_response_after_render, _rendered_x_profile_surface
from .models import RecentActivity
from .extractors import (
    SnowballRelationships,
    _parse_aria_metrics,
    estimate_count,
    extract_following_profile_urls,
    extract_relevant_relationship_urls,
)
from .x_search import prepare_x_cookies, x_browser_context_options

logger = logging.getLogger(__name__)


class RecentPostMarkupError(RuntimeError):
    """The expected X timeline surface is absent or structurally incomplete."""


class XAccessShellRecoveryRequired(RecentPostMarkupError):
    """The profile page returned X's blank, access, or error shell and needs batch recovery."""


@dataclass
class FetchedXProfilePage:
    """One authenticated profile visit: identity HTML plus recent posts."""

    posts: list[dict[str, Any]]
    html: str | None = None
    profile_surface: Any = None
    problem: str | None = None
    final_url: str | None = None
    http_status: int | None = None


class XFollowingRateLimitError(RuntimeError):
    """X returned an access/rate-limit shell while reading a Following page."""

    def __init__(self, handle: str, partial_profile_urls: set[str], diagnostics: dict[str, Any]):
        self.handle = handle
        self.partial_profile_urls = tuple(sorted(partial_profile_urls))
        self.diagnostics = dict(diagnostics)
        markers = ",".join(str(marker) for marker in diagnostics.get("visible_markers", [])) or "unknown"
        super().__init__(f"X Following access/rate-limit shell for @{handle} [visible_markers={markers}]")


def build_engagement_metrics(posts: list[dict[str, Any]], follower_estimate: int | None = None) -> dict[str, Any] | None:
    if not posts:
        return None
    def avg(key: str) -> float:
        vals = [float(p.get(key) or 0) for p in posts]
        return sum(vals) / len(vals) if vals else 0.0
    average_likes = avg("likes")
    average_reposts = avg("reposts")
    average_replies = avg("replies")
    average_views = avg("views")
    weighted = average_likes + average_reposts * 2 + average_replies * 1.5 + average_views * 0.005
    # Shrink tiny-account rates so one interaction on a 6-follower account is
    # not treated as stronger proof than sustained specialist engagement.
    efficiency = weighted / max(follower_estimate, 1_000) if follower_estimate else None
    return {
        "posts_count": len(posts),
        "average_likes": _clean_float(average_likes),
        "average_reposts": _clean_float(average_reposts),
        "average_replies": _clean_float(average_replies),
        "average_views": _clean_float(average_views),
        "weighted_average_engagement": _clean_float(weighted),
        "engagement_efficiency_rate": round(efficiency, 6) if efficiency is not None else None,
        "weights": {"likes": 1.0, "reposts": 2.0, "replies": 1.5, "views": 0.005},
    }


def recent_activity_from_posts(posts: list[dict[str, Any]], now: datetime | None = None) -> RecentActivity:
    now = now or datetime.now(timezone.utc)
    dated = []
    for post in posts:
        if post.get("is_pinned"):
            continue
        dt = _parse_dt(post.get("created_at"))
        if dt:
            dated.append((dt, post))
    if not dated:
        return RecentActivity(status="unknown_no_recent_posts_parsed", evidence="Could not parse timestamps for latest non-pinned posts from X UI.")
    dated.sort(key=lambda x: x[0], reverse=True)
    top5 = dated[:5]
    ages = [max(0, (now - dt).total_seconds() / 86400) for dt, _ in top5]
    avg_age = sum(ages) / len(ages)
    status = "active_recently" if avg_age <= 14 else "active" if avg_age <= 45 else "stale"
    return RecentActivity(
        status=status,
        latest_visible_post_at=top5[0][0].isoformat(),
        evidence=f"Latest {len(top5)} non-pinned posts average age {avg_age:.1f} days.",
    )


def recent_activity_score(activity: RecentActivity, max_score: int = 30) -> int:
    if not activity.latest_visible_post_at:
        return 0
    match = re.search(r"average age ([0-9.]+) days", activity.evidence or "")
    if not match:
        # Only timestamps calculated from verified, owned, non-pinned posts
        # qualify for a recency boost.
        return 0
    days = float(match.group(1))
    if days <= 3:
        return max_score
    if days <= 7:
        return round(max_score * 0.85)
    if days <= 14:
        return round(max_score * 0.7)
    if days <= 30:
        return round(max_score * 0.45)
    if days <= 60:
        return round(max_score * 0.25)
    return round(max_score * 0.1)


def engagement_score(metrics: dict[str, Any] | None, max_score: int = 20) -> int:
    if not metrics:
        return 0
    weighted = float(metrics.get("weighted_average_engagement") or 0)
    if weighted <= 0:
        return 0
    raw_score = min(max_score, max(1, round((math.log10(weighted + 1) / 4) * max_score)))
    rate = metrics.get("engagement_efficiency_rate")
    if rate is None:
        return raw_score
    # Efficiency matters, but raw engagement carries most of the engagement signal.
    efficiency_score = min(max_score, max(1, round((math.log10(float(rate) * 10_000 + 1) / 3) * max_score)))
    return round(efficiency_score * 0.3 + raw_score * 0.7)


class XRecentPostFetcher:
    def __init__(
        self,
        headless: bool = True,
        x_session: str | None = None,
        timeout_ms: int = 30000,
        *,
        scroll_delay_min_seconds: float = 1.5,
        scroll_delay_max_seconds: float = 3.0,
    ):
        self.headless = headless
        self.x_session = x_session
        self.timeout_ms = timeout_ms
        self.scroll_delay_min_seconds = max(0.0, float(scroll_delay_min_seconds))
        self.scroll_delay_max_seconds = max(
            self.scroll_delay_min_seconds,
            float(scroll_delay_max_seconds),
        )
        self._batch_browser: _RecentPostBrowser | None = None

    def _new_browser(self) -> _RecentPostBrowser:
        return _RecentPostBrowser(
            self.headless,
            self.x_session,
            self.timeout_ms,
            self.scroll_delay_min_seconds,
            self.scroll_delay_max_seconds,
        )

    def has_batch_context(self) -> bool:
        return self._batch_browser is not None

    @asynccontextmanager
    async def batch_context(self):
        """Reuse one authenticated browser context while the caller holds it.

        Nested callers share the already-open Chromium so SQS batches do not
        log in again. Direct ``fetch_one`` callers still get a short-lived
        browser when no batch context is active.
        """
        close_on_exit = self._batch_browser is None
        if close_on_exit:
            await self.open_batch_context()
        try:
            yield self
        finally:
            if close_on_exit:
                await self.close_batch_context()

    async def open_batch_context(self) -> None:
        """Open the reusable authenticated context for a queue batch."""
        if self._batch_browser is not None:
            raise RuntimeError("X recent-post batch context is already active")
        browser = self._new_browser()
        await browser.__aenter__()
        self._batch_browser = browser

    async def close_batch_context(self) -> None:
        """Close the active batch context, if one is open."""
        browser = self._batch_browser
        self._batch_browser = None
        if browser is not None:
            await browser.__aexit__(None, None, None)

    async def fetch_many(self, handles: list[str], posts_per_profile: int = 5, concurrency: int = 2) -> dict[str, list[dict[str, Any]]]:
        # Timeline work always touches X and must remain behind one worker.
        concurrency = 1
        sem = asyncio.Semaphore(concurrency)
        async with self._new_browser() as browser:
            async def one(handle: str):
                async with sem:
                    delay = random.uniform(0.5, 1.5)
                    logger.debug("X timeline pacing: waiting %.2fs before @%s", delay, handle)
                    await asyncio.sleep(delay)
                    try:
                        posts = await asyncio.wait_for(browser.fetch_posts(handle, posts_per_profile), timeout=self.timeout_ms / 1000 + 10)
                    except Exception as exc:
                        logger.warning("recent-post fetch failed for @%s: %s", handle, exc)
                        posts = []
                    return handle.lower(), posts
            tasks = [asyncio.create_task(one(handle)) for handle in handles]
            pairs = []
            for task in asyncio.as_completed(tasks):
                pairs.append(await task)
                completed = len(pairs)
                if completed == 1 or completed == len(handles) or completed % 10 == 0:
                    logger.debug("recent-post progress: %d/%d profiles complete", completed, len(handles))
        return dict(pairs)

    async def fetch_one(self, handle: str, posts_per_profile: int = 5) -> list[dict[str, Any]]:
        """Fetch one timeline and propagate technical failures to queue work."""
        if self._batch_browser is not None:
            return await asyncio.wait_for(
                self._batch_browser.fetch_posts(handle, posts_per_profile), timeout=self.timeout_ms / 1000 + 10
            )
        async with self._new_browser() as browser:
            return await asyncio.wait_for(
                browser.fetch_posts(handle, posts_per_profile), timeout=self.timeout_ms / 1000 + 10
            )

    async def fetch_one_with_profile_surface(
        self,
        handle: str,
        posts_per_profile: int = 5,
    ) -> tuple[list[dict[str, Any]], Any]:
        """Fetch activity and the authenticated header surface in one page visit.

        The public profile read remains the source of identity and HTTP status.
        This optional surface merely fills metadata (notably a bio) that X may
        omit from its public render while still exposing it to the authenticated
        timeline that is already required for activity scoring.
        """
        if self._batch_browser is not None:
            return await asyncio.wait_for(
                self._batch_browser.fetch_posts_with_profile_surface(handle, posts_per_profile),
                timeout=self.timeout_ms / 1000 + 10,
            )
        async with self._new_browser() as browser:
            return await asyncio.wait_for(
                browser.fetch_posts_with_profile_surface(handle, posts_per_profile),
                timeout=self.timeout_ms / 1000 + 10,
            )

    async def fetch_profile_and_posts(
        self,
        handle: str,
        posts_per_profile: int = 5,
    ) -> FetchedXProfilePage:
        """Read identity and recent posts from one authenticated profile visit."""
        timeout = self.timeout_ms / 1000 + 18
        if self._batch_browser is not None:
            return await asyncio.wait_for(
                self._batch_browser.fetch_profile_and_posts(handle, posts_per_profile),
                timeout=timeout,
            )
        async with self._new_browser() as browser:
            return await asyncio.wait_for(
                browser.fetch_profile_and_posts(handle, posts_per_profile),
                timeout=timeout,
            )

    async def fetch_relationships(
        self,
        handle: str,
        related_terms: list[str],
        following_max_scrolls: int,
    ) -> SnowballRelationships:
        """Read transient snowball relationships with the same sequential X session."""
        if self._batch_browser is not None:
            return await asyncio.wait_for(
                self._batch_browser.fetch_relationships(handle, related_terms, following_max_scrolls),
                timeout=self.timeout_ms / 1000 + max(20, following_max_scrolls * 3),
            )
        async with self._new_browser() as browser:
            return await asyncio.wait_for(
                browser.fetch_relationships(handle, related_terms, following_max_scrolls),
                timeout=self.timeout_ms / 1000 + max(20, following_max_scrolls * 3),
            )

    async def fetch_following(
        self,
        handle: str,
        following_max_scrolls: int,
    ) -> SnowballRelationships:
        """Read only the Following surface for a dedicated one-hop snowball."""

        if self._batch_browser is not None:
            return await asyncio.wait_for(
                self._batch_browser.fetch_following(handle, following_max_scrolls),
                timeout=self.timeout_ms / 1000 + max(20, following_max_scrolls * 3),
            )
        async with self._new_browser() as browser:
            return await asyncio.wait_for(
                browser.fetch_following(handle, following_max_scrolls),
                timeout=self.timeout_ms / 1000 + max(20, following_max_scrolls * 3),
            )


class _RecentPostBrowser:
    def __init__(
        self,
        headless: bool,
        x_session: str | None,
        timeout_ms: int,
        scroll_delay_min_seconds: float = 1.5,
        scroll_delay_max_seconds: float = 3.0,
    ):
        self.headless = headless
        self.x_session = x_session
        self.timeout_ms = timeout_ms
        self.scroll_delay_min_seconds = max(0.0, float(scroll_delay_min_seconds))
        self.scroll_delay_max_seconds = max(
            self.scroll_delay_min_seconds,
            float(scroll_delay_max_seconds),
        )
        self.pw = None
        self.browser = None
        self.context = None

    async def _wait_between_scrolls(self, page: Any, handle: str, scroll_number: int) -> None:
        minimum = max(0.0, float(getattr(self, "scroll_delay_min_seconds", 1.5)))
        maximum = max(
            minimum,
            float(getattr(self, "scroll_delay_max_seconds", 3.0)),
        )
        delay = random.uniform(minimum, maximum)
        logger.debug(
            "X timeline pacing: waiting %.2fs before scroll %d for @%s",
            delay,
            scroll_number,
            handle,
        )
        await page.wait_for_timeout(round(delay * 1000))

    async def __aenter__(self):
        from playwright.async_api import async_playwright
        self.pw = await async_playwright().start()
        self.browser = await self.pw.chromium.launch(headless=self.headless)
        self.context = await self.browser.new_context(**x_browser_context_options())
        cookies = prepare_x_cookies(self.x_session)
        if cookies:
            await self.context.add_cookies(cookies)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        for resource in (self.context, self.browser):
            if resource:
                try:
                    await asyncio.wait_for(resource.close(), timeout=5)
                except Exception:
                    pass
        if self.pw:
            try:
                await asyncio.wait_for(self.pw.stop(), timeout=5)
            except Exception:
                pass

    async def fetch_posts(self, handle: str, posts_per_profile: int) -> list[dict[str, Any]]:
        posts, _surface = await self._fetch_posts(handle, posts_per_profile, capture_profile_surface=False)
        return posts

    async def fetch_posts_with_profile_surface(self, handle: str, posts_per_profile: int) -> tuple[list[dict[str, Any]], Any]:
        return await self._fetch_posts(handle, posts_per_profile, capture_profile_surface=True)

    async def fetch_profile_and_posts(self, handle: str, posts_per_profile: int) -> FetchedXProfilePage:
        page = await self.context.new_page()
        page.set_default_timeout(self.timeout_ms)
        requested_url = f"https://x.com/{handle}"
        try:
            response = await page.goto(requested_url, wait_until="domcontentloaded")
            http_status = response.status if response else None
            html, final_url, problem = await _capture_response_after_render(
                page,
                requested_url,
                http_status=http_status,
                timeout_ms=self.timeout_ms,
            )
            if problem in {"profile_identity_mismatch", "rate_limited"}:
                raise XAccessShellRecoveryRequired(
                    f"Unexpected response: {problem} requested_url={requested_url} final_url={final_url}"
                )
            if problem:
                return FetchedXProfilePage(
                    posts=[],
                    html=None,
                    problem=problem,
                    final_url=final_url,
                    http_status=http_status,
                )
            posts = await self._collect_posts(page, handle, posts_per_profile)
            html = await page.content()
            profile_surface = await _rendered_x_profile_surface(page, requested_url)
            return FetchedXProfilePage(
                posts=posts,
                html=html,
                profile_surface=profile_surface,
                final_url=final_url,
                http_status=http_status,
            )
        finally:
            await page.close()

    async def _fetch_posts(
        self,
        handle: str,
        posts_per_profile: int,
        *,
        capture_profile_surface: bool,
    ) -> tuple[list[dict[str, Any]], Any]:
        page = await self.context.new_page()
        page.set_default_timeout(self.timeout_ms)
        try:
            await page.goto(f"https://x.com/{handle}", wait_until="domcontentloaded")
            await page.wait_for_timeout(2500)
            profile_surface = (
                await _rendered_x_profile_surface(page, f"https://x.com/{handle}")
                if capture_profile_surface
                else None
            )
            posts = await self._collect_posts(page, handle, posts_per_profile)
            return posts, profile_surface
        finally:
            await page.close()

    async def _collect_posts(self, page: Any, handle: str, posts_per_profile: int) -> list[dict[str, Any]]:
        posts: list[dict[str, Any]] = []
        markup_error: RecentPostMarkupError | None = None
        for scroll in range(5):
            try:
                posts = await _extract_posts_from_page(page, handle)
                markup_error = None
            except RecentPostMarkupError as exc:
                markup_error = exc
                posts = []
            non_pinned = [p for p in posts if not p.get("is_pinned")]
            if len(non_pinned) >= posts_per_profile:
                break
            await page.mouse.wheel(0, 2200)
            await self._wait_between_scrolls(page, handle, scroll + 1)
        if markup_error is not None and not posts:
            diagnostics = await _collect_timeline_surface_diagnostics(page, str(markup_error))
            message = _format_timeline_markup_error(markup_error, diagnostics)
            if diagnostics.get("surface_state") == "access_or_error_shell":
                raise XAccessShellRecoveryRequired(message)
            raise RecentPostMarkupError(message)
        return [p for p in posts if not p.get("is_pinned")][:posts_per_profile]

    async def fetch_relationships(
        self,
        handle: str,
        related_terms: list[str],
        following_max_scrolls: int,
    ) -> SnowballRelationships:
        """Collect relationship URLs without retaining parent/crawl metadata."""
        page = await self.context.new_page()
        page.set_default_timeout(self.timeout_ms)
        try:
            await page.goto(f"https://x.com/{handle}", wait_until="domcontentloaded")
            await page.wait_for_timeout(2_500)
            signals: list[dict[str, Any]] = []
            seen_signals: set[tuple[str, tuple[str, ...], tuple[str, ...]]] = set()
            for scroll in range(5):
                for signal in await _extract_relationship_signals_from_page(page, handle):
                    key = (
                        signal["text"],
                        tuple(signal["hrefs"]),
                        tuple(signal["quote_hrefs"]),
                    )
                    if key not in seen_signals:
                        signals.append(signal)
                        seen_signals.add(key)
                if len(signals) >= 5 or scroll == 4:
                    break
                await page.mouse.wheel(0, 2200)
                await self._wait_between_scrolls(page, handle, scroll + 1)

            urls = extract_relevant_relationship_urls(
                signals,
                related_terms=related_terms,
                source_handle=handle,
            )
            following_urls, following_incomplete = await self._fetch_following_urls(
                page, handle, following_max_scrolls
            )
            urls.update(following_urls)
            return SnowballRelationships(tuple(sorted(urls)), following_incomplete)
        finally:
            await page.close()

    async def fetch_following(
        self,
        handle: str,
        following_max_scrolls: int,
    ) -> SnowballRelationships:
        """Collect Following accounts without fetching timeline relationships."""

        page = await self.context.new_page()
        page.set_default_timeout(self.timeout_ms)
        try:
            following_urls, following_incomplete = await self._fetch_following_urls(
                page, handle, following_max_scrolls
            )
            return SnowballRelationships(tuple(sorted(following_urls)), following_incomplete)
        finally:
            await page.close()

    async def _fetch_following_urls(
        self,
        page: Any,
        handle: str,
        following_max_scrolls: int,
    ) -> tuple[set[str], bool]:
        await page.goto(f"https://x.com/{handle}/following", wait_until="domcontentloaded")
        await page.wait_for_timeout(2_500)
        urls: set[str] = set()
        stagnant_rounds = 0
        budget = max(0, following_max_scrolls)
        for scroll in range(budget + 1):
            hrefs = await _extract_following_hrefs_from_page(page)
            if not hrefs:
                diagnostics = await _collect_following_surface_diagnostics(page)
                markers = diagnostics.get("visible_markers") or []
                explicit_shell = any(marker != "unusual_activity" for marker in markers)
                if explicit_shell or (markers and not urls):
                    raise XFollowingRateLimitError(handle, urls, diagnostics)
            previous_count = len(urls)
            urls.update(extract_following_profile_urls(hrefs, source_handle=handle))
            if len(urls) == previous_count:
                stagnant_rounds += 1
            else:
                stagnant_rounds = 0
            if stagnant_rounds >= 2:
                return urls, False
            if scroll == budget:
                return urls, True
            before_height = await page.evaluate("document.body.scrollHeight")
            await page.mouse.wheel(0, 2400)
            await self._wait_between_scrolls(page, handle, scroll + 1)
            after_height = await page.evaluate("document.body.scrollHeight")
            if after_height <= before_height:
                stagnant_rounds += 1
                if stagnant_rounds >= 2:
                    return urls, False
        return urls, True


async def _collect_timeline_surface_diagnostics(page: Any, error_message: str) -> dict[str, Any]:
    """Capture bounded, non-content timeline evidence after a final failure."""
    try:
        surface = await page.evaluate(
            r"""
            () => {
              const articles = Array.from(document.querySelectorAll('article'));
              const legacy = article => article.getAttribute('data-testid') === 'tweet';
              const schema = article => /SocialMediaPosting/i.test(article.getAttribute('itemtype') || '')
                || article.getAttribute('itemprop') === 'hasPart';
              const hasStatus = article => Boolean(article.querySelector(
                'a[href*="/status/"], meta[itemprop="url"], [itemprop="url"]'
              ));
              const body = document.body ? (document.body.innerText || '') : '';
              const markerDefinitions = [
                ['login_or_sign_in', /\b(log in|sign in)\b/i],
                ['something_went_wrong', /something went wrong/i],
                ['try_again_later', /try again later/i],
                ['rate_limit', /rate limit|too many requests/i],
                ['unusual_activity', /unusual activity|challenge/i],
              ];
              return {
                article_count: articles.length,
                legacy_tweet_count: document.querySelectorAll('[data-testid="tweet"]').length,
                schema_article_count: articles.filter(schema).length,
                recognized_article_count: articles.filter(article => legacy(article) || schema(article) || hasStatus(article)).length,
                status_link_count: document.querySelectorAll('a[href*="/status/"]').length,
                page_title: (document.title || '').slice(0, 200),
                final_url: (location.href || '').slice(0, 300),
                visible_markers: markerDefinitions
                  .filter(([, pattern]) => pattern.test(body))
                  .map(([name]) => name),
              };
            }
            """
        )
        if not isinstance(surface, dict):
            surface = {}
    except Exception:
        logger.debug("could not collect X timeline diagnostics", exc_info=True)
        surface = {}

    surface.setdefault("article_count", 0)
    surface.setdefault("legacy_tweet_count", 0)
    surface.setdefault("schema_article_count", 0)
    surface.setdefault("recognized_article_count", 0)
    surface.setdefault("status_link_count", 0)
    surface.setdefault("page_title", "")
    surface.setdefault("final_url", "")
    surface.setdefault("visible_markers", [])
    surface["surface_state"] = _timeline_surface_state(error_message, surface)
    return surface


def _timeline_surface_state(error_message: str, surface: dict[str, Any]) -> str:
    markers = surface.get("visible_markers") or []
    if markers:
        return "access_or_error_shell"
    if "timestamps" in error_message.casefold():
        return "missing_timestamps"
    if "status links" in error_message.casefold():
        return "missing_status_links"
    if not surface.get("article_count"):
        return "timeline_not_rendered"
    if not surface.get("recognized_article_count"):
        return "unrecognized_markup"
    return "timeline_not_rendered"


def _format_timeline_markup_error(error: RecentPostMarkupError, diagnostics: dict[str, Any]) -> str:
    def compact(value: Any, limit: int) -> str:
        return " ".join(str(value or "").split())[:limit]

    markers = diagnostics.get("visible_markers") or []
    return (
        f"{error} ["
        f"surface_state={diagnostics.get('surface_state')}, "
        f"article_count={diagnostics.get('article_count', 0)}, "
        f"legacy_tweet_count={diagnostics.get('legacy_tweet_count', 0)}, "
        f"schema_article_count={diagnostics.get('schema_article_count', 0)}, "
        f"status_link_count={diagnostics.get('status_link_count', 0)}, "
        f"page_title={compact(diagnostics.get('page_title'), 120)!r}, "
        f"final_url={compact(diagnostics.get('final_url'), 240)!r}, "
        f"visible_markers={','.join(str(marker) for marker in markers) or 'none'}]"
    )


async def _extract_posts_from_page(page, handle: str) -> list[dict[str, Any]]:
    # X has moved from the legacy ``data-testid="tweet"`` cards to schema.org
    # ``SocialMediaPosting`` articles. Read both surfaces in one DOM pass so
    # markup drift does not require another browser attempt.
    raw_posts = await page.locator("article").evaluate_all(
        r"""
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
          const metaMetric = (element, properties) => {
            for (const property of properties) {
              const value = content(element, property);
              if (value) return value;
            }
            return null;
          };
          const posts = articles
            .filter(article => {
              const legacy = article.getAttribute('data-testid') === 'tweet';
              const schema = /SocialMediaPosting/i.test(article.getAttribute('itemtype') || '')
                || article.getAttribute('itemprop') === 'hasPart';
              const hasStatus = Boolean(article.querySelector('a[href*="/status/"], meta[itemprop="url"], [itemprop="url"]'));
              return legacy || schema || hasStatus;
            })
            .map(article => {
            const text = article.innerText || '';
            const time = article.querySelector('time');
            const timeHref = time && time.closest('a') ? time.closest('a').getAttribute('href') : null;
            const statusHref = article.querySelector('a[href*="/status/"]')?.getAttribute('href');
            const schemaUrl = content(article, 'url') || article.getAttribute('itemid');
            const href = asPath(timeHref || statusHref || schemaUrl);
            const created_at = (time && time.getAttribute('datetime'))
              || content(article, 'datePublished')
              || content(article, 'dateCreated')
              || null;
            const aria = Array.from(article.querySelectorAll('[aria-label]')).map(e => e.getAttribute('aria-label')).join(' | ');
            return {
              text: text || content(article, 'text'),
              created_at,
              href,
              aria,
              schema_metrics: {
                replies: metaMetric(article, ['commentCount', 'replyCount']),
                reposts: metaMetric(article, ['repostCount', 'shareCount']),
                likes: metaMetric(article, ['likeCount']),
                views: metaMetric(article, ['viewCount', 'interactionCount']),
              },
              is_pinned: /Pinned/i.test(text),
            };
          });
          const emptyState = document.querySelector('[data-testid="emptyState"]');
          const primaryColumn = document.querySelector('[data-testid="primaryColumn"]');
          const emptyText = [
            emptyState && emptyState.innerText,
            primaryColumn && primaryColumn.innerText,
            document.body && document.body.innerText,
          ].filter(Boolean).join(' ');
          if (/\b0\s+posts?\b|no posts yet|doesn['’]t have any posts|hasn['’]t posted|has not posted/i.test(emptyText)) {
            posts.push({is_explicit_empty_timeline: true});
          }
          return posts;
        }
        """
    )
    explicitly_empty = any(raw.get("is_explicit_empty_timeline") for raw in raw_posts)
    raw_posts = [raw for raw in raw_posts if not raw.get("is_explicit_empty_timeline")]
    if not raw_posts:
        if explicitly_empty:
            return []
        raise RecentPostMarkupError("timeline markup contains no tweet articles")
    out = []
    authored_statuses = 0
    observed_status_links = 0
    for raw in raw_posts:
        text = raw.get("text") or ""
        href = raw.get("href") or ""
        if re.match(r"^/[A-Za-z0-9_]{1,15}/status/[0-9]+(?:$|[/?#])", href, re.I):
            observed_status_links += 1
        # A profile timeline includes reposts, whose engagement belongs to the
        # original author. Only score statuses authored by this profile.
        if not re.match(rf"^/{re.escape(handle)}/status/", href, re.I):
            continue
        authored_statuses += 1
        if not raw.get("created_at"):
            continue
        metrics = _parse_aria_metrics(raw.get("aria") or "")
        schema_metrics = raw.get("schema_metrics") or {}
        for metric_name in ("replies", "reposts", "likes", "views"):
            if metrics.get(metric_name) is None and schema_metrics.get(metric_name) is not None:
                metrics[metric_name] = estimate_count(str(schema_metrics[metric_name]))
        out.append({
            "text": text[:500],
            "created_at": raw.get("created_at"),
            "url": f"https://x.com{href}",
            "is_pinned": bool(raw.get("is_pinned")),
            **metrics,
        })
    if not observed_status_links:
        raise RecentPostMarkupError("timeline markup omits status links")
    if authored_statuses and not out:
        raise RecentPostMarkupError("timeline markup omits timestamps for the profile's posts")
    seen = set()
    deduped = []
    for post in out:
        key = post.get("url") or (post.get("created_at"), post.get("text", "")[:80])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(post)
    return deduped


def _is_authored_status_href(href: str | None, handle: str) -> bool:
    return bool(re.match(rf"^/{re.escape(handle)}/status/[0-9]+(?:$|[/?#])", href or "", re.I))


async def _extract_relationship_signals_from_page(page: Any, handle: str) -> list[dict[str, Any]]:
    """Keep DOM extraction isolated from relationship normalization rules."""
    raw = await page.locator("article").evaluate_all(
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
              const status_href = asPath(timeHref || statusHref || content(article, 'url') || article.getAttribute('itemid'));
              const links = Array.from(article.querySelectorAll('a[href]')).map(a => a.getAttribute('href'));
              const quote = article.querySelector('[data-testid="quoteTweet"]');
              const quoteLinks = quote
                ? Array.from(quote.querySelectorAll('a[href]')).map(a => a.getAttribute('href'))
                : [];
              return {
                text: article.innerText || content(article, 'text'),
                status_href,
                hrefs: links,
                quote_hrefs: quoteLinks
              };
            });
        }
        """
    )
    return [signal for signal in raw if _is_authored_status_href(signal.get("status_href"), handle)]


async def _extract_following_hrefs_from_page(page: Any) -> list[str]:
    return await page.locator('[data-testid="UserCell"] a[href]').evaluate_all(
        "anchors => anchors.map(anchor => anchor.getAttribute('href')).filter(Boolean)"
    )


async def _collect_following_surface_diagnostics(page: Any) -> dict[str, Any]:
    """Collect bounded Following-shell evidence without retaining page content."""

    try:
        surface = await page.evaluate(
            r"""
            () => {
              const body = document.body ? (document.body.innerText || '') : '';
              const markerDefinitions = [
                ['login_or_sign_in', /\b(log in|sign in)\b/i],
                ['something_went_wrong', /something went wrong/i],
                ['try_again_later', /try again later/i],
                ['rate_limit', /rate limit|rate limited|too many requests/i],
                ['unusual_activity', /unusual activity|challenge/i],
              ];
              return {
                user_cell_count: document.querySelectorAll('[data-testid="UserCell"]').length,
                page_title: (document.title || '').slice(0, 200),
                final_url: (location.href || '').slice(0, 300),
                visible_markers: markerDefinitions
                  .filter(([, pattern]) => pattern.test(body))
                  .map(([name]) => name),
              };
            }
            """
        )
        if not isinstance(surface, dict):
            surface = {}
    except Exception:
        logger.debug("could not collect X Following diagnostics", exc_info=True)
        surface = {}
    surface.setdefault("user_cell_count", 0)
    surface.setdefault("page_title", "")
    surface.setdefault("final_url", "")
    surface.setdefault("visible_markers", [])
    return surface


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _clean_float(value: float) -> float | int:
    return int(value) if float(value).is_integer() else round(value, 2)
