from __future__ import annotations

import asyncio
import math
import re
from datetime import datetime, timezone
from typing import Any

from .models import RecentActivity
from .extractors import estimate_count
from .x_people_search import prepare_x_cookies


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
    efficiency = weighted / follower_estimate if follower_estimate else None
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
    def __init__(self, headless: bool = True, x_session: str | None = None, timeout_ms: int = 30000):
        self.headless = headless
        self.x_session = x_session
        self.timeout_ms = timeout_ms

    async def fetch_many(self, handles: list[str], posts_per_profile: int = 5, concurrency: int = 2) -> dict[str, list[dict[str, Any]]]:
        sem = asyncio.Semaphore(concurrency)
        async with _RecentPostBrowser(self.headless, self.x_session, self.timeout_ms) as browser:
            async def one(handle: str):
                async with sem:
                    return handle.lower(), await browser.fetch_posts(handle, posts_per_profile)
            pairs = await asyncio.gather(*(one(h) for h in handles))
        return dict(pairs)


class _RecentPostBrowser:
    def __init__(self, headless: bool, x_session: str | None, timeout_ms: int):
        self.headless = headless
        self.x_session = x_session
        self.timeout_ms = timeout_ms
        self.pw = None
        self.browser = None
        self.context = None

    async def __aenter__(self):
        from playwright.async_api import async_playwright
        self.pw = await async_playwright().start()
        self.browser = await self.pw.chromium.launch(headless=self.headless)
        self.context = await self.browser.new_context(viewport={"width": 1280, "height": 1000})
        cookies = prepare_x_cookies(self.x_session)
        if cookies:
            await self.context.add_cookies(cookies)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.pw:
            await self.pw.stop()

    async def fetch_posts(self, handle: str, posts_per_profile: int) -> list[dict[str, Any]]:
        page = await self.context.new_page()
        page.set_default_timeout(self.timeout_ms)
        try:
            await page.goto(f"https://x.com/{handle}", wait_until="domcontentloaded")
            await page.wait_for_timeout(2500)
            posts: list[dict[str, Any]] = []
            for _ in range(5):
                posts = await _extract_posts_from_page(page, handle)
                non_pinned = [p for p in posts if not p.get("is_pinned")]
                if len(non_pinned) >= posts_per_profile:
                    break
                await page.mouse.wheel(0, 2200)
                await page.wait_for_timeout(1200)
            return [p for p in posts if not p.get("is_pinned")][:posts_per_profile]
        except Exception:
            return []
        finally:
            await page.close()


async def _extract_posts_from_page(page, handle: str) -> list[dict[str, Any]]:
    raw_posts = await page.locator('article[data-testid="tweet"]').evaluate_all(
        """
        articles => articles.map(article => {
          const text = article.innerText || '';
          const time = article.querySelector('time');
          const href = time && time.closest('a') ? time.closest('a').getAttribute('href') : null;
          const aria = Array.from(article.querySelectorAll('[aria-label]')).map(e => e.getAttribute('aria-label')).join(' | ');
          return {text, created_at: time ? time.getAttribute('datetime') : null, href, aria, is_pinned: /Pinned/i.test(text)};
        })
        """
    )
    out = []
    for raw in raw_posts:
        text = raw.get("text") or ""
        if not raw.get("created_at"):
            continue
        href = raw.get("href") or ""
        # A profile timeline includes reposts, whose engagement belongs to the
        # original author. Only score statuses authored by this profile.
        if not re.match(rf"^/{re.escape(handle)}/status/", href, re.I):
            continue
        metrics = _parse_aria_metrics(raw.get("aria") or "")
        out.append({
            "text": text[:500],
            "created_at": raw.get("created_at"),
            "url": f"https://x.com{href}",
            "is_pinned": bool(raw.get("is_pinned")),
            **metrics,
        })
    seen = set()
    deduped = []
    for post in out:
        key = post.get("url") or (post.get("created_at"), post.get("text", "")[:80])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(post)
    return deduped


def _parse_aria_metrics(label: str) -> dict[str, int | None]:
    lower = label.lower()
    return {
        "replies": _metric(lower, ["replies", "reply"]),
        "reposts": _metric(lower, ["reposts", "repost"]),
        "likes": _metric(lower, ["likes", "like"]),
        "views": _metric(lower, ["views", "view"]),
    }


def _metric(text: str, names: list[str]) -> int | None:
    for name in names:
        patterns = [rf"([0-9][0-9,.]*\s*[km]?)\s+{name}", rf"{name}\s+([0-9][0-9,.]*\s*[km]?)"]
        for pat in patterns:
            m = re.search(pat, text, re.I)
            if m:
                return estimate_count(m.group(1).strip())
    return None


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _clean_float(value: float) -> float | int:
    return int(value) if float(value).is_integer() else round(value, 2)

