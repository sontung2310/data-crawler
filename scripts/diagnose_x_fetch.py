"""Run a real, non-queueing diagnostic of X search, profile, and timeline fetches.

The utility uses the configured authenticated X browser session, but never
prints the session value, cookies, or raw page HTML. It reports the exact stage
that failed and writes only public profile/post data and safe markup indicators.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

# Allow direct execution from the repository root without requiring installation.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from x_influencer_discovery.browser import PlaywrightFetcher, _response_problem
from x_influencer_discovery.config import Settings, load_settings
from x_influencer_discovery.extractors import ProfileMarkupError, _count_before_label, _meta, extract_x_profile_from_html
from x_influencer_discovery.queueing import normalize_profile_url
from x_influencer_discovery.x_profile_posts import XRecentPostFetcher
from x_influencer_discovery.x_search import X_PLAYWRIGHT_USER_AGENT, XPostSearcher, prepare_x_cookies

logger = logging.getLogger(__name__)

_SAFE_RESPONSE_HEADERS = {
    "cache-control",
    "cf-ray",
    "content-type",
    "location",
    "retry-after",
    "server",
    "www-authenticate",
    "x-frame-options",
    "x-rate-limit-limit",
    "x-rate-limit-remaining",
    "x-rate-limit-reset",
    "x-served-by",
}


def _ensure_usable_playwright_browsers() -> dict[str, Any]:
    """If Cursor sandbox pointed Playwright at a missing cache, fall back to the user cache."""
    original = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
    info: dict[str, Any] = {
        "playwright_browsers_path": original,
        "cleared_unusable_path": False,
    }
    if not original:
        return info
    cache = Path(original)
    has_headless_shell = any(cache.glob("chromium_headless_shell-*"))
    if has_headless_shell:
        return info
    os.environ.pop("PLAYWRIGHT_BROWSERS_PATH", None)
    info["cleared_unusable_path"] = True
    info["reason"] = (
        "PLAYWRIGHT_BROWSERS_PATH had Chromium but no chromium_headless_shell; "
        "headless launch would fail. Cleared it so diagnosis uses the default ms-playwright cache."
    )
    return info


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be at least 0")
    return parsed


def _env_present(name: str) -> bool:
    return bool((os.getenv(name) or "").strip())


def _session_analysis(cli_settings: Settings) -> dict[str, Any]:
    return {
        "cli_load_settings_has_session": bool(cli_settings.x_session),
        "x_auth_token_present": _env_present("X_AUTH_TOKEN"),
        "x_ct0_present": _env_present("X_CT0"),
        "note": "load_settings() uses X_AUTH_TOKEN + X_CT0 only; it does not read X_SESSION or X_BROWSER_SESSION.",
    }


def _session_source(settings: Settings) -> str:
    if not settings.x_session:
        return "none"
    if _env_present("X_AUTH_TOKEN") and _env_present("X_CT0"):
        return "load_settings (X_AUTH_TOKEN+X_CT0)"
    return "load_settings"


def _settings_with_resolved_session(cli_settings: Settings) -> tuple[Settings, dict[str, Any]]:
    analysis = _session_analysis(cli_settings)
    analysis["session_source_used"] = _session_source(cli_settings)
    analysis["cookie_names_applied"] = [
        cookie["name"] for cookie in prepare_x_cookies(cli_settings.x_session or "")
    ]
    return cli_settings, analysis


def _safe_headers(headers: dict[str, str] | None) -> dict[str, str]:
    return {
        key: value
        for key, value in (headers or {}).items()
        if key.lower() in _SAFE_RESPONSE_HEADERS
    }


def _safe_post(post: dict[str, Any]) -> dict[str, Any]:
    """Keep useful public post fields while bounding output size."""
    return {
        "url": post.get("url"),
        "created_at": post.get("created_at"),
        "text": str(post.get("text") or "")[:500],
        "likes": post.get("likes"),
        "reposts": post.get("reposts"),
        "replies": post.get("replies"),
        "views": post.get("views"),
        "is_pinned": bool(post.get("is_pinned")),
    }


def _http_error_diagnosis(page: dict[str, Any]) -> str:
    status = page.get("http_status")
    session = page.get("session_configured")
    login = (page.get("identity_markers") or {}).get("login_or_sign_in_visible")
    challenge = (page.get("identity_markers") or {}).get("challenge_or_error_visible")
    parts = [
        f"X returned HTTP {status}, which the pipeline maps to Unexpected response: http_error "
        f"(any status >= 400 except 404/429)."
    ]
    if not session:
        parts.append(
            "No authenticated X session was applied for this probe. A logged-in desktop "
            "browser can still open the profile while this crawler gets a 4xx."
        )
    elif login:
        parts.append(
            "Cookies were applied but a login/sign-in wall is still visible; "
            "X_AUTH_TOKEN / X_CT0 are likely expired or incomplete."
        )
    elif challenge:
        parts.append("Cookies were applied but X showed a challenge, unusual-activity, or rate-limit page.")
    else:
        parts.append("Cookies were applied; inspect document_responses and response headers for the 4xx.")
    return " ".join(parts)


def _diagnosis(
    page: dict[str, Any],
    parsed: dict[str, Any],
    posts: dict[str, Any],
    *,
    pipeline: dict[str, Any] | None = None,
    unauthenticated: dict[str, Any] | None = None,
) -> str:
    unauth_status = (unauthenticated or {}).get("status")
    pipeline_status = (pipeline or {}).get("status")
    authenticated_ok = pipeline_status == "ok" or page.get("response_problem") is None
    if unauth_status == "http_error" and page.get("session_configured") and authenticated_ok:
        return (
            "Unauthenticated Playwright fetch reproduced Unexpected response: http_error "
            f"(HTTP {unauthenticated.get('http_status')}). The same URL succeeded once "
            "cookies were applied. python -m x_influencer_discovery now loads "
            "X_AUTH_TOKEN/X_CT0 the same way the SQS orchestrator does."
        )
    if unauth_status == "http_error" and page.get("session_configured"):
        if pipeline_status == "profile_identity_mismatch" or page.get("response_problem") == "profile_identity_mismatch":
            return (
                "Unauthenticated Playwright fetch reproduced Unexpected response: http_error "
                f"(HTTP {unauthenticated.get('http_status')}). With X_AUTH_TOKEN/X_CT0 applied, "
                "X returned HTTP 200 but the profile identity never rendered (title stayed "
                "'Profile / X'). The CLI http_error is from the anonymous 403; this remaining "
                "failure is a slower client-render / session-completeness issue."
            )
        return (
            "Unauthenticated Playwright fetch reproduced Unexpected response: http_error "
            f"(HTTP {unauthenticated.get('http_status')}). Authenticated fetch also failed. "
            + _http_error_diagnosis(page)
        )
    if page.get("status") == "error":
        pipeline_status = (pipeline or {}).get("status")
        pipeline_error = (pipeline or {}).get("error")
        if pipeline_status and pipeline_status != "ok":
            transport = (pipeline or {}).get("transport")
            extra = f" Pipeline fetcher status={pipeline_status}"
            if pipeline_error:
                extra += f" error={pipeline_error}"
            if transport:
                extra += f" transport={transport}."
            return (
                "Detailed page probe failed to launch Playwright "
                f"({str(page.get('error') or '')[:180]}).{extra}"
            )
        return "Playwright navigation or browser setup failed; inspect page.error."
    if page.get("response_problem") == "http_error":
        return _http_error_diagnosis(page)
    if page.get("response_problem") == "profile_identity_mismatch":
        return (
            "X returned HTML without stable profile identity for the requested handle. "
            "This usually means an expired/incomplete session, an X login/challenge shell, "
            "or client-rendered profile data that was not ready at capture time."
        )
    if page.get("response_problem") == "profile_redirected":
        return "X redirected the requested profile URL; verify the account still exists and the session can access it."
    if page.get("response_problem") == "rate_limited":
        return "X returned HTTP 429 (rate limited)."
    if page.get("response_problem") == "profile_not_found":
        return "X returned HTTP 404; the handle may not exist."
    if parsed.get("status") == "error":
        return (
            "The page identity was visible but the current X markup did not satisfy the profile parser. "
            "Use parsed.error and page.follower_surface to update the extractor or increase the render wait."
        )
    if posts.get("status") == "error":
        return "Profile parsing succeeded, but fetching the recent-post timeline failed; inspect recent_posts.error."
    return "Profile and timeline fetching completed successfully."


async def _follower_dom_samples(page) -> list[dict[str, str | None]]:
    """Return a few public follower-link text samples without retaining page HTML."""
    return await page.locator("a").evaluate_all(
        """
        links => links
            .filter(link => /\\bfollowers?\\b/i.test((link.innerText || '').trim()))
            .slice(0, 5)
            .map(link => ({
                text: (link.innerText || '').trim().slice(0, 200),
                href: link.getAttribute('href'),
                aria_label: link.getAttribute('aria-label'),
            }))
        """
    )


async def _profile_dom_surface(page, handle: str) -> dict[str, Any]:
    """Capture bounded public profile metadata from X's rendered DOM."""
    profile_path = f"/{handle}/photo"
    return {
        "bio": (await page.locator("[data-testid='UserDescription']").evaluate_all(
            "els => els.slice(0, 1).map(el => (el.innerText || '').trim().slice(0, 2000))"
        ))[:1],
        "canonical_urls": await page.locator("link[rel='canonical']").evaluate_all(
            "els => els.slice(0, 3).map(el => el.href)"
        ),
        "avatar_images": await page.locator(f"a[href='{profile_path}'] img").evaluate_all(
            "els => els.slice(0, 3).map(el => ({src: el.currentSrc || el.src, raw_src: el.getAttribute('src'), srcset: el.getAttribute('srcset'), alt: el.alt || null}))"
        ),
        "user_avatar_images": await page.locator("[data-testid^='UserAvatar-Container'] img").evaluate_all(
            "els => els.slice(0, 5).map(el => ({src: el.currentSrc || el.src, raw_src: el.getAttribute('src'), srcset: el.getAttribute('srcset'), alt: el.alt || null}))"
        ),
    }


def _record_document_response(bucket: list[dict[str, Any]], response) -> None:
    if response.request.resource_type != "document":
        return
    bucket.append(
        {
            "url": (response.url or "")[:300],
            "status": response.status,
            "status_text": response.status_text,
            "headers": _safe_headers(response.headers),
        }
    )


async def _probe_profile_page(profile_url: str, settings: Settings, wait_ms: int) -> dict[str, Any]:
    """Fetch one actual X profile page and retain safe evidence for diagnosis."""
    cookies = prepare_x_cookies(settings.x_session or "")
    result: dict[str, Any] = {
        "status": "error",
        "requested_url": profile_url,
        "session_configured": bool(settings.x_session),
        "session_cookie_count": len(cookies),
        "session_cookie_names": [cookie["name"] for cookie in cookies],
        "user_agent": X_PLAYWRIGHT_USER_AGENT,
    }
    try:
        from playwright.async_api import async_playwright

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=settings.headless)
            context = await browser.new_context(
                user_agent=X_PLAYWRIGHT_USER_AGENT,
                viewport={"width": 1280, "height": 900},
            )
            try:
                if cookies:
                    await context.add_cookies(cookies)
                page = await context.new_page()
                page.set_default_timeout(30_000)
                document_responses: list[dict[str, Any]] = []
                page.on("response", lambda response: _record_document_response(document_responses, response))
                response = await page.goto(profile_url, wait_until="domcontentloaded")
                await page.wait_for_timeout(wait_ms)
                html = await page.content()
                title = await page.title()
                final_url = page.url
                body_text = await page.locator("body").inner_text()
                http_status = response.status if response else None
                problem = _response_problem(profile_url, final_url, http_status, html)
                normalized_body = re.sub(r"\s+", " ", body_text).casefold()
                handle = profile_url.rsplit("/", 1)[-1]
                result = {
                    "status": "ok",
                    "requested_url": profile_url,
                    "final_url": final_url,
                    "http_status": http_status,
                    "http_status_text": response.status_text if response else None,
                    "response_headers": _safe_headers(response.headers if response else {}),
                    "document_responses": document_responses[:8],
                    "page_title": title[:300],
                    "response_problem": problem,
                    "session_configured": bool(settings.x_session),
                    "session_cookie_count": len(cookies),
                    "session_cookie_names": [cookie["name"] for cookie in cookies],
                    "user_agent": X_PLAYWRIGHT_USER_AGENT,
                    "host": (urlparse(final_url).hostname or ""),
                    "identity_markers": {
                        "og_url": _meta(html, "og:url"),
                        "has_requested_handle": f"@{handle.casefold()}" in normalized_body,
                        "login_or_sign_in_visible": bool(re.search(r"\b(log in|sign in)\b", normalized_body)),
                        "challenge_or_error_visible": any(
                            marker in normalized_body
                            for marker in ("something went wrong", "unusual activity", "try again later", "rate limit")
                        ),
                    },
                    "follower_surface": {
                        "followers_label_visible": "followers" in normalized_body,
                        "parsed_follower_value": _count_before_label(html, "Followers"),
                        "following_label_visible": "following" in normalized_body,
                        "parsed_following_value": _count_before_label(html, "Following"),
                        "rendered_link_samples": await _follower_dom_samples(page),
                    },
                    "profile_dom_surface": await _profile_dom_surface(page, handle),
                }
            finally:
                await context.close()
                await browser.close()
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def _infer_transport(result_error: str | None, http_status: int | None) -> str:
    error = result_error or ""
    if "urlopen" in error or error.startswith("HTTP Error ") or "Tunnel connection failed" in error:
        return "urllib_fallback"
    if http_status is not None:
        return "playwright"
    return "unknown"


async def _probe_pipeline_fetcher(profile_url: str, settings: Settings) -> dict[str, Any]:
    """Reproduce the exact PlaywrightFetcher path the influencer pipeline uses."""
    fetcher = PlaywrightFetcher(
        headless=settings.headless,
        x_session=settings.x_session,
        timeout_ms=12_000,
    )
    result = await fetcher.fetch_one(profile_url)
    return {
        "status": result.status,
        "error": result.error,
        "http_status": result.http_status,
        "final_url": result.final_url,
        "html_captured": bool(result.html),
        "session_configured": bool(settings.x_session),
        "transport": _infer_transport(result.error, result.http_status),
    }


async def _probe_profile(
    profile_url: str,
    settings: Settings,
    wait_ms: int,
    *,
    page_only: bool = False,
    compare_unauthenticated: bool = True,
) -> dict[str, Any]:
    page = await _probe_profile_page(profile_url, settings, wait_ms)
    pipeline = await _probe_pipeline_fetcher(profile_url, settings)
    unauthenticated: dict[str, Any] = {"status": "skipped"}
    if compare_unauthenticated and settings.x_session:
        logger.info("reproducing unauthenticated pipeline fetch for %s", profile_url)
        unauthenticated = await _probe_pipeline_fetcher(
            profile_url,
            replace(settings, x_session=None),
        )
    elif compare_unauthenticated:
        unauthenticated = {
            "status": pipeline.get("status"),
            "error": pipeline.get("error"),
            "http_status": pipeline.get("http_status"),
            "note": "CLI/load_settings session is empty, so the pipeline probe above is already unauthenticated.",
        }

    parsed: dict[str, Any] = {"status": "skipped"}
    posts: dict[str, Any] = {"status": "skipped"}

    if page["status"] == "ok" and not page_only:
        try:
            from playwright.async_api import async_playwright

            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(headless=settings.headless)
                context = await browser.new_context(
                    user_agent=X_PLAYWRIGHT_USER_AGENT,
                    viewport={"width": 1280, "height": 900},
                )
                cookies = prepare_x_cookies(settings.x_session or "")
                if cookies:
                    await context.add_cookies(cookies)
                fetch_page = await context.new_page()
                await fetch_page.goto(profile_url, wait_until="domcontentloaded")
                await fetch_page.wait_for_timeout(wait_ms)
                html = await fetch_page.content()
                handle = profile_url.rsplit("/", 1)[-1]
                profile = extract_x_profile_from_html(handle, html)
                parsed = {"status": "ok", "profile": asdict(profile)}
                await context.close()
                await browser.close()
        except (ProfileMarkupError, ValueError) as exc:
            parsed = {"status": "error", "error": f"{type(exc).__name__}: {exc}"}
        except Exception as exc:
            parsed = {"status": "error", "error": f"{type(exc).__name__}: {exc}"}

    if parsed["status"] == "ok":
        try:
            handle = parsed["profile"]["handle"]
            recent = await XRecentPostFetcher(settings.headless, settings.x_session).fetch_one(handle, posts_per_profile=5)
            posts = {"status": "ok", "count": len(recent), "items": [_safe_post(post) for post in recent]}
        except Exception as exc:
            posts = {"status": "error", "error": f"{type(exc).__name__}: {exc}"}

    return {
        "profile_url": profile_url,
        "profile_page": page,
        "pipeline_fetcher": pipeline,
        "unauthenticated_pipeline_fetcher": unauthenticated,
        "profile_parse": parsed,
        "recent_posts": posts,
        "diagnosis": _diagnosis(page, parsed, posts, pipeline=pipeline, unauthenticated=unauthenticated),
    }


async def _urls_from_query(query: str, settings: Settings, max_profiles: int, max_scrolls: int) -> tuple[list[str], dict[str, Any]]:
    searcher = XPostSearcher(settings.headless, settings.x_session)
    try:
        lanes = await searcher.search(
            [query],
            authors_per_query=max_profiles,
            max_scrolls_per_query=max_scrolls,
        )
        urls = list(dict.fromkeys(
            normalize_profile_url(f"https://x.com/{author.handle}")
            for author in lanes.get(query, [])
        ))[:max_profiles]
        return urls, {
            "status": "ok",
            "query": query,
            "author_records": len(lanes.get(query, [])),
            "profile_urls": urls,
            "failed_terms": sorted(searcher.failed_terms),
        }
    except Exception as exc:
        return [], {"status": "error", "query": query, "error": f"{type(exc).__name__}: {exc}"}


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    playwright_browsers = _ensure_usable_playwright_browsers()
    cli_settings = load_settings(args.env_file)
    settings, session_analysis = _settings_with_resolved_session(cli_settings)
    if args.headed:
        settings = replace(settings, headless=False)
    if args.cli_session_only:
        settings = replace(settings, x_session=None)
        session_analysis["session_source_used"] = "none"
        session_analysis["cookie_names_applied"] = []

    profile_urls: list[str] = []
    search: dict[str, Any] | None = None
    if args.query:
        queried_urls, search = await _urls_from_query(args.query, settings, args.max_profiles, args.search_scrolls)
        profile_urls.extend(queried_urls)
    for raw_url in args.profile_url:
        try:
            profile_urls.append(normalize_profile_url(raw_url))
        except ValueError as exc:
            logger.warning("Skipping invalid profile URL %r: %s", raw_url, exc)
    profile_urls = list(dict.fromkeys(profile_urls))[:args.max_profiles]

    probes = []
    for profile_url in profile_urls:
        logger.info("diagnosing X profile %s", profile_url)
        probes.append(
            await _probe_profile(
                profile_url,
                settings,
                args.wait_ms,
                page_only=args.page_only,
                compare_unauthenticated=args.compare_unauthenticated,
            )
        )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "session_configured": bool(settings.x_session),
        "session_analysis": session_analysis,
        "playwright_browsers": playwright_browsers,
        "query_search": search,
        "profiles_requested": profile_urls,
        "profiles": probes,
        "summary": {
            "requested": len(profile_urls),
            "profile_pages_ok": sum(item["profile_page"]["status"] == "ok" for item in probes),
            "pipeline_fetcher_ok": sum(item["pipeline_fetcher"]["status"] == "ok" for item in probes),
            "unauthenticated_http_error": sum(
                item["unauthenticated_pipeline_fetcher"].get("status") == "http_error" for item in probes
            ),
            "profiles_parsed": sum(item["profile_parse"]["status"] == "ok" for item in probes),
            "timelines_fetched": sum(item["recent_posts"]["status"] == "ok" for item in probes),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a real X profile-fetch diagnostic without SQS or MongoDB.")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--query", help="Run one real X Latest search, then diagnose discovered profiles.")
    source.add_argument("--profile-url", action="append", default=[], help="Diagnose one X profile URL; repeatable.")
    parser.add_argument("--max-profiles", type=_positive_int, default=3, help="Maximum profiles to diagnose (default: 3).")
    parser.add_argument("--search-scrolls", type=_non_negative_int, default=1, help="Additional X Latest scrolls when using --query (default: 1).")
    parser.add_argument("--wait-ms", type=_positive_int, default=3_500, help="Milliseconds to wait after X navigation (default: 3500).")
    parser.add_argument("--page-only", action="store_true", help="Inspect only the profile page; skip a second parse fetch and timeline fetch.")
    parser.add_argument("--headed", action="store_true", help="Show the browser for login/challenge diagnosis.")
    parser.add_argument(
        "--cli-session-only",
        action="store_true",
        help="Do not apply X_AUTH_TOKEN/X_CT0, to reproduce an anonymous fetch.",
    )
    parser.add_argument(
        "--compare-unauthenticated",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Also reproduce an unauthenticated pipeline fetch (default: true).",
    )
    parser.add_argument("--env-file", help="Environment file to load (default: .env).")
    parser.add_argument("--output", type=Path, default=Path("data/diagnostics/x_fetch_diagnostic.json"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    report = asyncio.run(_run(args))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps({
        "output": str(args.output),
        "session_analysis": report.get("session_analysis"),
        "playwright_browsers": report.get("playwright_browsers"),
        "summary": report["summary"],
        "diagnoses": [
            {
                "profile_url": item["profile_url"],
                "diagnosis": item["diagnosis"],
                "pipeline_fetcher": item["pipeline_fetcher"].get("status"),
                "unauthenticated": item["unauthenticated_pipeline_fetcher"].get("status"),
                "unauthenticated_http_status": item["unauthenticated_pipeline_fetcher"].get("http_status"),
            }
            for item in report["profiles"]
        ],
    }, indent=2))


if __name__ == "__main__":
    main()
