from __future__ import annotations

import asyncio
import ipaddress
import logging
import random
import re
import socket
import urllib.request
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import urlparse

from .models import RenderedProfileSurface
from .x_search import prepare_x_cookies, x_browser_context_options

logger = logging.getLogger(__name__)

_INITIAL_RENDER_WAIT_MS = 800
# X often first returns the generic ``Profile / X`` shell, then replaces its
# title and metadata client-side. Five seconds proved too short for ordinary
# authenticated profiles during a real queue run. Keep this inside the
# worker's 12-second page timeout while allowing a normal delayed render.
_X_PROFILE_IDENTITY_WAIT_MS = 10_000


@dataclass
class FetchResult:
    url: str
    html: str | None
    status: str
    error: str | None = None
    http_status: int | None = None
    final_url: str | None = None
    profile_surface: RenderedProfileSurface | None = None


class PlaywrightFetcher:
    """Small, stable Playwright wrapper with graceful HTTP fallback for tests/dev."""

    def __init__(self, headless: bool = True, x_session: str | None = None, timeout_ms: int = 25000):
        self.headless = headless
        self.x_session = x_session
        self.timeout_ms = timeout_ms

    async def fetch_many(self, urls: Iterable[str], concurrency: int = 4) -> list[FetchResult]:
        urls = list(urls)
        if not urls:
            return []
        # Any X request shares the authenticated-session safety budget. Public
        # URLs may still use their caller's concurrency when no X URL is present.
        concurrency = 1 if any(_is_x_url(url) for url in urls) else max(1, concurrency)
        sem = asyncio.Semaphore(concurrency)
        async with _BrowserContext(self.headless, self.x_session, self.timeout_ms) as ctx:
            async def one(index: int, url: str) -> tuple[int, FetchResult]:
                async with sem:
                    if _is_x_url(url):
                        delay = random.uniform(0.5, 1.5)
                        logger.debug("X profile pacing: waiting %.2fs before %s", delay, url)
                        await asyncio.sleep(delay)
                    try:
                        result = await asyncio.wait_for(
                            ctx.fetch(url),
                            timeout=self.timeout_ms / 1000 + 5,
                        )
                    except TimeoutError:
                        result = FetchResult(
                            url=url,
                            html=None,
                            status="fetch_timeout",
                            error=f"Hard timeout after {self.timeout_ms / 1000 + 5:.0f}s",
                        )
                    return index, result
            tasks = [asyncio.create_task(one(index, url)) for index, url in enumerate(urls)]
            results: list[FetchResult | None] = [None] * len(urls)
            completed = 0
            for task in asyncio.as_completed(tasks):
                index, result = await task
                results[index] = result
                completed += 1
                if completed == 1 or completed == len(urls) or completed % 10 == 0:
                    logger.debug("fetch progress: %d/%d pages complete", completed, len(urls))
            return [result for result in results if result is not None]

    async def fetch_one(self, url: str) -> FetchResult:
        results = await self.fetch_many([url], concurrency=1)
        return results[0]


class _BrowserContext:
    def __init__(self, headless: bool, x_session: str | None, timeout_ms: int):
        self.headless = headless
        self.x_session = x_session
        self.timeout_ms = timeout_ms
        self.playwright = None
        self.browser = None
        self.context = None

    async def __aenter__(self):
        try:
            from playwright.async_api import async_playwright
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(headless=self.headless)
            self.context = await self.browser.new_context(
                **x_browser_context_options(viewport={"width": 1280, "height": 900}),
            )
            if self.x_session:
                cookies = prepare_x_cookies(self.x_session)
                if cookies:
                    await self.context.add_cookies(cookies)
        except Exception:
            # Pipeline still works with urllib fallback, less JS capable.
            self.context = None
        return self

    async def __aexit__(self, exc_type, exc, tb):
        for resource in (self.context, self.browser):
            if resource:
                try:
                    await asyncio.wait_for(resource.close(), timeout=5)
                except Exception:
                    pass
        if self.playwright:
            try:
                await asyncio.wait_for(self.playwright.stop(), timeout=5)
            except Exception:
                pass

    async def fetch(self, url: str) -> FetchResult:
        if self.context:
            page = None
            try:
                page = await self.context.new_page()
                page.set_default_timeout(self.timeout_ms)
                async def guard_navigation(route, request):
                    if request.is_navigation_request() and not await asyncio.to_thread(is_safe_public_url, request.url):
                        await route.abort("blockedbyclient")
                    else:
                        await route.continue_()
                await page.route("**/*", guard_navigation)
                response = await page.goto(url, wait_until="domcontentloaded")
                http_status = response.status if response else None
                html, final_url, problem = await _capture_response_after_render(
                    page,
                    url,
                    http_status=http_status,
                    timeout_ms=self.timeout_ms,
                )
                if problem:
                    return FetchResult(
                        url=url, html=None, status=problem, http_status=http_status,
                        final_url=final_url, error=f"Unexpected response: {problem}",
                    )
                profile_surface = await _rendered_x_profile_surface(page, url) if _is_x_url(url) else None
                return FetchResult(
                    url=url, html=html, status="ok", http_status=http_status,
                    final_url=final_url, profile_surface=profile_surface,
                )
            except Exception as exc:
                return FetchResult(url=url, html=None, status="fetch_failed", error=str(exc))
            finally:
                if page:
                    await page.close()
        return await asyncio.to_thread(self._urllib_fetch, url)

    def _urllib_fetch(self, url: str) -> FetchResult:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            opener = urllib.request.build_opener(_SafeRedirectHandler())
            with opener.open(req, timeout=self.timeout_ms / 1000) as response:
                html = response.read().decode("utf-8", "ignore")
                http_status = response.status
                final_url = response.geturl()
            problem = _response_problem(url, final_url, http_status, html)
            if problem:
                return FetchResult(
                    url=url, html=None, status=problem, http_status=http_status,
                    final_url=final_url, error=f"Unexpected response: {problem}",
                )
            return FetchResult(
                url=url, html=html, status="ok", http_status=http_status,
                final_url=final_url,
            )
        except Exception as exc:
            return FetchResult(url=url, html=None, status="fetch_failed", error=str(exc))


class _SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if not is_safe_public_url(newurl):
            raise OSError("Redirect to a non-public URL was blocked")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _is_x_url(url: str) -> bool:
    return (urlparse(url).hostname or "").lower() in {
        "x.com", "www.x.com", "twitter.com", "www.twitter.com",
    }


async def _rendered_x_profile_surface(page, requested_url: str) -> RenderedProfileSurface | None:
    """Read profile metadata from the stable, rendered X profile surfaces.

    The DOM fields are intentionally optional: a legitimate profile can have
    no bio or avatar, and an absent surface must not turn an otherwise valid
    fetch into a technical failure. The HTML parser remains the fallback for
    non-browser and changed-markup scenarios.
    """
    handle = next((part for part in urlparse(requested_url).path.split("/") if part), "")
    if not handle:
        return None
    try:
        bios = await page.locator("[data-testid='UserDescription']").evaluate_all(
            "els => els.slice(0, 1).map(el => (el.innerText || '').trim())"
        )
        avatars = await page.locator(f"a[href='/{handle}/photo'] img").evaluate_all(
            "els => els.slice(0, 1).map(el => el.currentSrc || el.src || '')"
        )
        if not avatars:
            # Some X profiles do not render the profile-photo link, but retain
            # the accessible avatar alt text on the profile header image.
            avatars = await page.locator("img[alt='Opens profile photo']").evaluate_all(
                "els => els.slice(0, 1).map(el => el.currentSrc || el.src || '')"
            )
        canonical_urls = await page.locator("link[rel='canonical']").evaluate_all(
            "els => els.slice(0, 1).map(el => el.href || '')"
        )
    except Exception as exc:
        logger.debug("Could not read rendered X profile metadata for %s: %s", requested_url, exc)
        return None

    bio = _clean_rendered_text(bios[0] if bios else None)
    profile_img_url = _clean_public_url(avatars[0] if avatars else None)
    canonical_url = _clean_public_url(canonical_urls[0] if canonical_urls else None)
    if not any((bio, profile_img_url, canonical_url)):
        return None
    return RenderedProfileSurface(
        bio=bio,
        profile_img_url=profile_img_url,
        canonical_url=canonical_url,
    )


def _clean_rendered_text(value: object) -> str | None:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text[:2_000] or None


def _clean_public_url(value: object) -> str | None:
    candidate = str(value or "").strip()
    parsed = urlparse(candidate)
    if parsed.scheme != "https" or not parsed.hostname:
        return None
    return candidate


async def _capture_response_after_render(
    page,
    requested_url: str,
    *,
    http_status: int | None,
    timeout_ms: int,
) -> tuple[str, str, str | None]:
    """Wait briefly for X's client-rendered profile identity before rejecting it.

    X often reports a generic ``Profile / X`` document title immediately after
    ``domcontentloaded``. The identity guard remains strict, but gets a bounded
    window to observe the requested handle before marking the work retryable.
    """
    total_wait_ms = min(max(0, timeout_ms), _X_PROFILE_IDENTITY_WAIT_MS)
    initial_wait_ms = min(_INITIAL_RENDER_WAIT_MS, total_wait_ms)
    if initial_wait_ms:
        await page.wait_for_timeout(initial_wait_ms)
    elapsed_ms = initial_wait_ms

    while True:
        html = await page.content()
        final_url = page.url
        problem = _response_problem(requested_url, final_url, http_status, html)
        if not _is_x_url(requested_url) or problem != "profile_identity_mismatch":
            return html, final_url, problem
        if elapsed_ms >= total_wait_ms:
            logger.warning(
                "X profile identity did not render within %d ms requested_url=%s final_url=%s title=%r",
                total_wait_ms,
                requested_url,
                final_url,
                _document_title(html),
            )
            return html, final_url, problem
        delay_ms = min(250, total_wait_ms - elapsed_ms)
        await page.wait_for_timeout(delay_ms)
        elapsed_ms += delay_ms


def is_safe_public_url(url: str) -> bool:
    """Allow ordinary public HTTP(S) URLs and reject local/private targets."""
    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            return False
        if parsed.username or parsed.password or parsed.port not in {None, 80, 443}:
            return False
        addresses = socket.getaddrinfo(parsed.hostname, parsed.port or 443, type=socket.SOCK_STREAM)
        return bool(addresses) and all(ipaddress.ip_address(item[4][0]).is_global for item in addresses)
    except (OSError, ValueError):
        return False


def _response_problem(requested_url: str, final_url: str, http_status: int | None, html: str) -> str | None:
    if http_status == 429:
        return "rate_limited"
    if http_status == 404 and _is_x_url(requested_url):
        # Public profile reads expose a genuine missing account as 404. Keep
        # this distinct from generic HTTP failures so the queue can perform
        # its configured retry/DLQ isolation without holding unrelated FIFO
        # work for a full visibility lease.
        return "profile_not_found"
    if http_status is not None and http_status >= 400:
        return "http_error"
    requested = urlparse(requested_url)
    if requested.hostname in {"x.com", "www.x.com", "twitter.com", "www.twitter.com"}:
        final = urlparse(final_url)
        requested_parts = [part for part in requested.path.split("/") if part]
        final_parts = [part for part in final.path.split("/") if part]
        if not requested_parts or not final_parts or requested_parts[0].lower() != final_parts[0].lower():
            return "profile_redirected"
        handle = requested_parts[0]
        identity_patterns = (
            rf"\(@{re.escape(handle)}\)",
            rf'<meta[^>]+(?:property|name)=["\']og:url["\'][^>]+content=["\'][^"\']*/{re.escape(handle)}(?:["\'/])',
        )
        if not any(re.search(pattern, html, re.I) for pattern in identity_patterns):
            return "profile_identity_mismatch"
    return None


def _document_title(html: str) -> str | None:
    """Extract a bounded document title for safe operational diagnostics."""
    match = re.search(r"<title[^>]*>(.*?)</title>", html, re.S | re.I)
    if not match:
        return None
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", match.group(1))).strip()[:200] or None
