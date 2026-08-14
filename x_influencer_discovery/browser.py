from __future__ import annotations

import asyncio
import ipaddress
import re
import socket
import urllib.request
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import urlparse

from .x_people_search import prepare_x_cookies


@dataclass
class FetchResult:
    url: str
    html: str | None
    status: str
    error: str | None = None
    http_status: int | None = None
    final_url: str | None = None


class PlaywrightFetcher:
    """Small, stable Playwright wrapper with graceful HTTP fallback for tests/dev."""

    def __init__(self, headless: bool = True, x_session: str | None = None, timeout_ms: int = 25000):
        self.headless = headless
        self.x_session = x_session
        self.timeout_ms = timeout_ms

    async def fetch_many(self, urls: Iterable[str], concurrency: int = 4) -> list[FetchResult]:
        sem = asyncio.Semaphore(concurrency)
        async with _BrowserContext(self.headless, self.x_session, self.timeout_ms) as ctx:
            async def one(url: str) -> FetchResult:
                async with sem:
                    return await ctx.fetch(url)
            return await asyncio.gather(*(one(url) for url in urls))

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
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/126 Safari/537.36",
                viewport={"width": 1280, "height": 900},
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
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

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
                await page.wait_for_timeout(800)
                html = await page.content()
                http_status = response.status if response else None
                final_url = page.url
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

