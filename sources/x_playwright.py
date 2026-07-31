"""
X (Twitter) crawler via Playwright — search, profile timelines, and comments.

Session cookies must be copied from a logged-in browser session.

Both search and profile return a payload shaped for downstream persistence:

    {"posts": [...], "comments": [...]}

Comments are a flat list (same idea as YouTube) so callers can persist posts and
comments independently. Persistence to Mongo is out of scope here.

================================================================================
CLI
================================================================================

Search:
  python -m api.sources.x_playwright search [options]

  --query TEXT          Search keywords (default: "news")
  --limit N             Max tweets, 1–100 (default: 5)
  --filter {live,top,user,media,list}
  --since YYYY-MM-DD
  --until YYYY-MM-DD
  --min-replies N
  --min-faves N
  --min-retweets N
  --include-comments    Also crawl replies for each post
  --num-comment-crawl N Max top-level replies per post (default: all up to cap 50)
  --get-replies         Also include nested replies under top-level replies
  --json

Profile:
  python -m api.sources.x_playwright profile [options]

  --user TEXT           Username, @handle, or profile URL (required)
  --limit N
  --include-comments
  --num-comment-crawl N
  --get-replies
  --json

Examples:
  python -m api.sources.x_playwright search --query "AI" --limit 5 --filter live
  python -m api.sources.x_playwright search --query "AI" --limit 3 \\
      --include-comments --num-comment-crawl 20 --json
  python -m api.sources.x_playwright profile --user Roboticmarketer --limit 5 \\
      --include-comments --get-replies
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, TypedDict
from urllib.parse import quote_plus, urlparse

import requests
from playwright.sync_api import Browser, BrowserContext, Page, TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from .utils import (
    clean_html_to_text,
    crawled_at_now,
    human_delay,
    parse_engagement_count,
    resolve_limit,
    truncate_title,
)

logger = logging.getLogger(__name__)

# --- session from env (X_AUTH_TOKEN, X_CT0) ---------------------------------
from config import X_AUTH_TOKEN, X_CT0
from exceptions import SessionExpiredError

# --- crawl settings ----------------------------------------------------------
X_HEADLESS = True
X_SCROLL_DELAY_MIN = 1.5
X_SCROLL_DELAY_MAX = 3.5
X_NAV_TIMEOUT_MS = 30000
X_MAX_SCROLLS = 50

# Extra delays between opening individual tweet pages for comments (anti-block).
X_COMMENT_PAGE_DELAY_MIN = 2.0
X_COMMENT_PAGE_DELAY_MAX = 4.5
X_COMMENT_SCROLL_MAX = 6

# Search tab: "live" = Latest (chronological), "top" = Top (popularity).
X_SEARCH_FILTER = "top"

# Advanced Search date window (X operators since:/until:). Leave empty to skip.
X_SINCE_DATE = ""
X_UNTIL_DATE = ""

# Advanced Search engagement minimums. Leave empty to skip.
X_MIN_REPLIES = ""
X_MIN_FAVES = ""
X_MIN_RETWEETS = ""

# Comments (YouTube-aligned knobs).
X_COMMENTS_HARD_CAP = 50
X_GET_REPLIES = False

SOURCE_KEY_SEARCH = "x_playwright"
SOURCE_KEY_PROFILE = "x_playwright_profile"
SOURCE_KEY_COMMENTS = "x_playwright_comments"
TWEET_SELECTOR = 'article[data-testid="tweet"]'
STATUS_ID_RE = re.compile(r"/status/(\d+)")
USER_ID_RE = re.compile(r"/i/user/(\d+)")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{1,15}$")
ALLOWED_SEARCH_FILTERS = {"live", "top", "user", "media", "list"}
DESKTOP_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


class CrawlResult(TypedDict):
    """Payload shaped so downstream can persist posts and comments separately."""

    posts: List[Dict]
    comments: List[Dict]


def empty_crawl_result() -> CrawlResult:
    return {"posts": [], "comments": []}


def status_id_from_post(row: Dict) -> Optional[str]:
    post_id = (row.get("post_id") or "").strip()
    if ":" in post_id:
        return post_id.rsplit(":", 1)[-1] or None
    url = row.get("url") or ""
    match = STATUS_ID_RE.search(url)
    return match.group(1) if match else None


def resolve_comment_limit(num_comment_crawl: Optional[int]) -> int:
    if num_comment_crawl is None:
        return X_COMMENTS_HARD_CAP
    try:
        n = int(num_comment_crawl)
    except (TypeError, ValueError):
        return X_COMMENTS_HARD_CAP
    if n <= 0:
        return 0
    return min(n, X_COMMENTS_HARD_CAP)


def resolve_get_replies(get_replies: Optional[bool]) -> bool:
    if get_replies is None:
        return bool(X_GET_REPLIES)
    return bool(get_replies)


def map_comment_row(
    *,
    comment_id: str,
    parent_content_id: str,
    parent_content_url: str,
    author: str,
    author_channel_id: Optional[str],
    text: str,
    like_count: Optional[int],
    reply_count: int,
    parent_comment_id: Optional[str],
    published_ts: Optional[int],
) -> Optional[Dict]:
    comment_id = (comment_id or "").strip()
    if not comment_id:
        return None
    return {
        "comment_id": comment_id,
        "source": SOURCE_KEY_COMMENTS,
        "parent_content_id": (parent_content_id or "").strip(),
        "parent_content_url": (parent_content_url or "").strip(),
        "author": (author or "").strip(),
        "author_channel_id": author_channel_id or None,
        "text": text or "",
        "like_count": like_count,
        "reply_count": reply_count if parent_comment_id is None else 0,
        "parent_comment_id": parent_comment_id,
        "published_ts": published_ts,
        "crawled_at": crawled_at_now(),
    }


class XPlaywrightClient:
    """Shared Playwright engine: browser session, navigation, tweet/comment parsing."""

    def session_configured(self) -> bool:
        return bool((X_AUTH_TOKEN or "").strip() and (X_CT0 or "").strip())

    def build_cookies(self) -> List[Dict]:
        return [
            {
                "name": "auth_token",
                "value": X_AUTH_TOKEN.strip(),
                "domain": ".x.com",
                "path": "/",
            },
            {
                "name": "ct0",
                "value": X_CT0.strip(),
                "domain": ".x.com",
                "path": "/",
            },
        ]

    def check_robots_txt(self) -> None:
        try:
            r = requests.get(
                "https://x.com/robots.txt",
                headers={"User-Agent": DESKTOP_UA},
                timeout=10,
            )
            logger.info("x.com robots.txt status=%s (advisory only)", r.status_code)
        except Exception as exc:
            logger.debug("robots.txt check skipped: %s", exc)

    def crawl_feed_and_comments(
        self,
        feed_url: str,
        limit: Optional[int],
        source_key: str,
        *,
        include_comments: bool = False,
        num_comment_crawl: Optional[int] = None,
        get_replies: Optional[bool] = None,
        on_item=None,
    ) -> CrawlResult:
        """One browser session: collect feed posts, optionally comments per post."""
        if not self.session_configured():
            raise SessionExpiredError(
                "x_playwright",
                "X_AUTH_TOKEN and X_CT0 must be set in .env",
            )

        limit = resolve_limit(limit, hard_cap=100) or 100
        self.check_robots_txt()
        out = empty_crawl_result()

        try:
            with sync_playwright() as playwright:
                browser: Browser = playwright.chromium.launch(headless=X_HEADLESS)
                context: BrowserContext = browser.new_context(
                    viewport={"width": 1280, "height": 800},
                    user_agent=DESKTOP_UA,
                )
                context.add_cookies(self.build_cookies())
                page = context.new_page()

                posts = self._navigate_and_collect(page, feed_url, limit, source_key)
                out["posts"] = posts
                if on_item:
                    for post in posts:
                        on_item("post", post)

                if include_comments and posts:
                    comments_crawler = XCommentsCrawler(self)
                    comments = comments_crawler.fetch_for_posts(
                        page,
                        posts,
                        num_comment_crawl=num_comment_crawl,
                        get_replies=get_replies,
                        on_item=on_item,
                    )
                    out["comments"] = comments

                context.close()
                browser.close()
        except SessionExpiredError:
            raise
        except Exception as exc:
            logger.exception("x_playwright crawl failed: %s", exc)
            return out

        return out

    def _navigate_and_collect(
        self, page: Page, url: str, limit: int, source_key: str
    ) -> List[Dict]:
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=X_NAV_TIMEOUT_MS)
            page.wait_for_selector(TWEET_SELECTOR, timeout=X_NAV_TIMEOUT_MS)
        except PlaywrightTimeoutError:
            if self.is_login_wall(page):
                raise SessionExpiredError(
                    "x_playwright",
                    "login wall detected — refresh X_AUTH_TOKEN and X_CT0 in .env",
                )
            logger.warning("x_playwright: timed out waiting for tweets url=%s", url)
            return []

        if self.is_login_wall(page):
            raise SessionExpiredError(
                "x_playwright",
                "login wall detected — refresh X_AUTH_TOKEN and X_CT0 in .env",
            )

        return self.collect_tweets(page, limit, source_key)

    # Kept for callers that only need a bare feed list (internal/tests).
    def crawl_url(self, url: str, limit: int, source_key: str) -> List[Dict]:
        result = self.crawl_feed_and_comments(url, limit, source_key, include_comments=False)
        return result["posts"]

    def is_login_wall(self, page: Page) -> bool:
        try:
            url = page.url.lower()
            if "/login" in url or "/i/flow/login" in url:
                return True
            if page.locator('input[autocomplete="username"]').count() > 0:
                return True
            if (
                page.locator('input[name="text"]').count() > 0
                and page.locator(TWEET_SELECTOR).count() == 0
            ):
                return True
        except Exception:
            pass
        return False

    def collect_tweets(self, page: Page, limit: int, source_key: str) -> List[Dict]:
        seen_ids: set[str] = set()
        results: List[Dict] = []

        for _ in range(X_MAX_SCROLLS):
            articles = page.locator(TWEET_SELECTOR)
            count = articles.count()

            for i in range(count):
                article = articles.nth(i)
                status_id = self.extract_status_id(article)
                if not status_id or status_id in seen_ids:
                    continue

                mapped = self.map_tweet(article, status_id, source_key)
                if not mapped:
                    continue

                seen_ids.add(status_id)
                results.append(mapped)
                if len(results) >= limit:
                    return results[:limit]

            if len(results) >= limit:
                break

            human_delay(X_SCROLL_DELAY_MIN, X_SCROLL_DELAY_MAX)
            page.evaluate("window.scrollBy(0, Math.floor(window.innerHeight * 0.8));")

        return results[:limit]

    def try_expand_replies(self, page: Page) -> None:
        """Click a few 'Show replies' / 'Show more' controls if present."""
        patterns = [
            'div[role="button"]:has-text("Show replies")',
            'div[role="button"]:has-text("Show more replies")',
            'div[role="button"]:has-text("Show")',
            'button:has-text("Show replies")',
        ]
        clicks = 0
        for pattern in patterns:
            if clicks >= 3:
                break
            loc = page.locator(pattern)
            try:
                n = min(loc.count(), 2)
            except Exception:
                continue
            for i in range(n):
                try:
                    loc.nth(i).click(timeout=1500)
                    clicks += 1
                    human_delay(0.6, 1.2)
                except Exception:
                    continue

    def map_tweet(self, article, status_id: str, source_key: str) -> Optional[Dict]:
        author = self.extract_author(article)
        text = self.extract_text(article)
        url = self.build_canonical_url(author, status_id)
        title = truncate_title(text)

        if not text and not title:
            return None

        return {
            "post_id": f"{source_key}:{status_id}",
            "source": source_key,
            "url": url,
            "title": title,
            "text": text,
            "text_html": "",
            "published_ts": self.extract_published_ts(article),
            "crawled_at": crawled_at_now(),
            "author": author,
            "domain": self.extract_domain(article),
            "engagement": self.extract_engagement(article),
        }

    @staticmethod
    def parse_datetime_to_ts(value: Optional[str]) -> Optional[int]:
        if not value:
            return None
        try:
            normalized = value.replace("Z", "+00:00")
            dt = datetime.fromisoformat(normalized)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            return None

    def extract_status_id(self, article) -> Optional[str]:
        try:
            links = article.locator('a[href*="/status/"]')
            for i in range(links.count()):
                href = links.nth(i).get_attribute("href") or ""
                match = STATUS_ID_RE.search(href)
                if match:
                    return match.group(1)
        except Exception:
            pass
        return None

    def extract_reply_to_status_ids(self, article) -> List[str]:
        """Status ids referenced in 'Replying to' / social context (excluding self)."""
        ids: List[str] = []
        try:
            # Prefer social-context region when present.
            regions = article.locator(
                '[data-testid="socialContext"], div[dir="ltr"]:has-text("Replying to")'
            )
            scope = regions.first if regions.count() > 0 else article
            links = scope.locator('a[href*="/status/"]')
            own = self.extract_status_id(article)
            for i in range(min(links.count(), 12)):
                href = links.nth(i).get_attribute("href") or ""
                match = STATUS_ID_RE.search(href)
                if not match:
                    continue
                sid = match.group(1)
                if sid and sid != own and sid not in ids:
                    ids.append(sid)
        except Exception:
            pass
        return ids

    def extract_author(self, article) -> str:
        try:
            user_name = article.locator('[data-testid="User-Name"]')
            if user_name.count() > 0:
                anchors = user_name.first.locator('a[href^="/"]')
                for i in range(anchors.count()):
                    href = anchors.nth(i).get_attribute("href") or ""
                    if href.startswith("/") and "/status/" not in href:
                        handle = href.strip("/").split("/")[0]
                        if handle:
                            return f"@{handle}"
        except Exception:
            pass

        try:
            profile_link = article.locator('a[href^="/"][role="link"]').first
            href = profile_link.get_attribute("href") or ""
            handle = href.strip("/").split("/")[0]
            if handle and handle not in {"search", "home", "explore", "notifications"}:
                return f"@{handle}"
        except Exception:
            pass
        return ""

    def extract_author_channel_id(self, article) -> Optional[str]:
        """Best-effort numeric/rest_id from /i/user/{id} or data attributes."""
        try:
            links = article.locator('a[href*="/i/user/"]')
            for i in range(min(links.count(), 8)):
                href = links.nth(i).get_attribute("href") or ""
                match = USER_ID_RE.search(href)
                if match:
                    return match.group(1)
        except Exception:
            pass

        for attr in ("data-user-id", "data-testid"):
            try:
                nodes = article.locator(f"[{attr}]")
                for i in range(min(nodes.count(), 20)):
                    val = nodes.nth(i).get_attribute(attr) or ""
                    if attr == "data-user-id" and val.isdigit():
                        return val
                    # e.g. UserAvatar-Container-1234567890
                    m = re.search(r"(\d{6,})", val)
                    if attr == "data-testid" and "UserAvatar" in val and m:
                        return m.group(1)
            except Exception:
                continue
        return None

    @staticmethod
    def extract_text(article) -> str:
        try:
            text_node = article.locator('[data-testid="tweetText"]').first
            if text_node.count() > 0:
                return clean_html_to_text(text_node.inner_text())
        except Exception:
            pass
        return ""

    def extract_published_ts(self, article) -> Optional[int]:
        try:
            time_node = article.locator("time").first
            if time_node.count() > 0:
                return self.parse_datetime_to_ts(time_node.get_attribute("datetime"))
        except Exception:
            pass
        return None

    @staticmethod
    def extract_domain(article) -> str:
        try:
            card_link = article.locator('[data-testid="card.wrapper"] a[href^="http"]').first
            if card_link.count() > 0:
                href = card_link.get_attribute("href") or ""
                host = urlparse(href).netloc
                if host:
                    return host.lstrip("www.")
        except Exception:
            pass

        try:
            external = article.locator(
                'a[href^="http"]:not([href*="x.com"]):not([href*="twitter.com"])'
            ).first
            if external.count() > 0:
                href = external.get_attribute("href") or ""
                host = urlparse(href).netloc
                if host:
                    return host.lstrip("www.")
        except Exception:
            pass
        return "x.com"

    @staticmethod
    def metric_from_button(article, testid: str) -> Optional[int]:
        try:
            button = article.locator(f'[data-testid="{testid}"]').first
            if button.count() == 0:
                return None
            label = button.get_attribute("aria-label") or button.inner_text()
            return parse_engagement_count(label)
        except Exception:
            return None

    @staticmethod
    def extract_views(article) -> Optional[int]:
        try:
            analytics = article.locator('[data-testid="app-text-transition-container"]').first
            if analytics.count() > 0:
                value = parse_engagement_count(analytics.inner_text())
                if value is not None:
                    return value
        except Exception:
            pass

        try:
            candidates = article.locator("span, div")
            for i in range(min(candidates.count(), 40)):
                text = candidates.nth(i).inner_text() or ""
                if "view" in text.lower():
                    value = parse_engagement_count(text)
                    if value is not None:
                        return value
        except Exception:
            pass
        return None

    def extract_engagement(self, article) -> Dict[str, Optional[int]]:
        return {
            "likes": self.metric_from_button(article, "like"),
            "retweets": self.metric_from_button(article, "retweet"),
            "comments": self.metric_from_button(article, "reply"),
            "views": self.extract_views(article),
        }

    @staticmethod
    def build_canonical_url(author: str, status_id: str) -> str:
        handle = author.lstrip("@").strip()
        if handle:
            return f"https://x.com/{handle}/status/{status_id}"
        return f"https://x.com/i/status/{status_id}"


class XCommentsCrawler:
    """Crawl replies for tweet status pages (YouTube-style flat comment list)."""

    def __init__(self, client: Optional[XPlaywrightClient] = None):
        self.client = client or XPlaywrightClient()

    def fetch_for_post(
        self,
        page: Page,
        post: Dict,
        *,
        num_comment_crawl: Optional[int] = None,
        get_replies: Optional[bool] = None,
    ) -> List[Dict]:
        parent_id = status_id_from_post(post)
        if not parent_id:
            return []

        limit = resolve_comment_limit(num_comment_crawl)
        if limit <= 0:
            return []

        include_nested = resolve_get_replies(get_replies)
        parent_url = (post.get("url") or "").strip() or f"https://x.com/i/status/{parent_id}"

        try:
            page.goto(parent_url, wait_until="domcontentloaded", timeout=X_NAV_TIMEOUT_MS)
            page.wait_for_selector(TWEET_SELECTOR, timeout=X_NAV_TIMEOUT_MS)
        except PlaywrightTimeoutError:
            logger.warning("x_playwright comments: timeout on %s", parent_url)
            return []

        if self.client.is_login_wall(page):
            raise SessionExpiredError(
                "x_playwright",
                "login wall on status page — refresh X_AUTH_TOKEN and X_CT0 in .env",
            )

        # Load more replies gradually (respect rate limits).
        seen_article_ids: set[str] = set()
        for _ in range(X_COMMENT_SCROLL_MAX):
            if include_nested:
                self.client.try_expand_replies(page)
            articles = page.locator(TWEET_SELECTOR)
            # Early stop if we already have enough distinct reply ids beyond the root.
            reply_ids = []
            for i in range(articles.count()):
                sid = self.client.extract_status_id(articles.nth(i))
                if sid and sid != parent_id:
                    reply_ids.append(sid)
            if len(set(reply_ids)) >= limit and not include_nested:
                break
            if len(set(reply_ids)) >= limit * 3:
                break
            human_delay(X_SCROLL_DELAY_MIN, X_SCROLL_DELAY_MAX)
            page.evaluate("window.scrollBy(0, Math.floor(window.innerHeight * 0.75));")

        return self._parse_thread(
            page,
            parent_content_id=parent_id,
            parent_content_url=parent_url,
            max_top_level=limit,
            include_nested=include_nested,
            seen_article_ids=seen_article_ids,
        )

    def _parse_thread(
        self,
        page: Page,
        *,
        parent_content_id: str,
        parent_content_url: str,
        max_top_level: int,
        include_nested: bool,
        seen_article_ids: set[str],
    ) -> List[Dict]:
        out: List[Dict] = []
        top_level_count = 0
        # Track which reply ids are top-level so nested can point at them.
        top_level_ids: set[str] = set()

        articles = page.locator(TWEET_SELECTOR)
        count = articles.count()

        for i in range(count):
            article = articles.nth(i)
            status_id = self.client.extract_status_id(article)
            if not status_id or status_id == parent_content_id:
                continue
            if status_id in seen_article_ids:
                continue
            seen_article_ids.add(status_id)

            reply_tos = self.client.extract_reply_to_status_ids(article)
            # Top-level: replies to the root post (or no explicit reply-to status).
            is_top_level = (
                not reply_tos
                or parent_content_id in reply_tos
                or all(r == parent_content_id for r in reply_tos)
            )
            # Nested: reply-to points at another reply in this thread.
            nested_parent = None
            if not is_top_level:
                for rid in reply_tos:
                    if rid != parent_content_id:
                        nested_parent = rid
                        break

            if is_top_level:
                if top_level_count >= max_top_level:
                    continue
                parent_comment_id = None
            else:
                if not include_nested:
                    continue
                parent_comment_id = nested_parent
                # Only keep nested replies under a top-level we already accepted.
                if parent_comment_id and parent_comment_id not in top_level_ids:
                    # Still allow if we saw it as any prior comment in this thread.
                    if parent_comment_id not in seen_article_ids:
                        continue

            text = self.client.extract_text(article)
            author = self.client.extract_author(article)
            if not text and not author:
                continue

            like_count = self.client.metric_from_button(article, "like")
            reply_count = self.client.metric_from_button(article, "reply") or 0
            row = map_comment_row(
                comment_id=status_id,
                parent_content_id=parent_content_id,
                parent_content_url=parent_content_url,
                author=author,
                author_channel_id=self.client.extract_author_channel_id(article),
                text=text,
                like_count=like_count,
                reply_count=int(reply_count) if reply_count is not None else 0,
                parent_comment_id=parent_comment_id,
                published_ts=self.client.extract_published_ts(article),
            )
            if not row:
                continue

            out.append(row)
            if is_top_level:
                top_level_ids.add(status_id)
                top_level_count += 1

        return out

    def fetch_for_posts(
        self,
        page: Page,
        posts: List[Dict],
        *,
        num_comment_crawl: Optional[int] = None,
        get_replies: Optional[bool] = None,
        on_item=None,
    ) -> List[Dict]:
        all_comments: List[Dict] = []
        for idx, post in enumerate(posts):
            if idx > 0:
                human_delay(X_COMMENT_PAGE_DELAY_MIN, X_COMMENT_PAGE_DELAY_MAX)
            try:
                rows = self.fetch_for_post(
                    page,
                    post,
                    num_comment_crawl=num_comment_crawl,
                    get_replies=get_replies,
                )
                all_comments.extend(rows)
                if on_item:
                    for row in rows:
                        on_item("comment", row)
            except Exception as exc:
                logger.warning(
                    "x_playwright comments failed for url=%s: %s",
                    post.get("url"),
                    exc,
                )
        return all_comments


class XSearchCrawler:
    """Crawl X search results with optional Advanced Search operators."""

    def __init__(
        self,
        client: Optional[XPlaywrightClient] = None,
        comments_crawler: Optional[XCommentsCrawler] = None,
    ):
        self.client = client or XPlaywrightClient()
        self.comments_crawler = comments_crawler or XCommentsCrawler(self.client)

    @staticmethod
    def normalize_search_filter(value: Optional[str]) -> str:
        filt = (value or X_SEARCH_FILTER or "live").strip().lower()
        if filt not in ALLOWED_SEARCH_FILTERS:
            logger.warning(
                "x_playwright: unknown X_SEARCH_FILTER=%r — falling back to 'live'",
                filt,
            )
            return "live"
        return filt

    @staticmethod
    def normalize_date(value: Optional[str], label: str) -> Optional[str]:
        raw = (value or "").strip()
        if not raw:
            return None
        if not DATE_RE.match(raw):
            logger.warning(
                "x_playwright: invalid %s=%r (expected YYYY-MM-DD) — ignoring",
                label,
                raw,
            )
            return None
        return raw

    @staticmethod
    def normalize_min_engagement(
        value: Optional[int],
        global_value: str,
        label: str,
    ) -> Optional[int]:
        if value is not None:
            raw = str(value).strip()
        else:
            raw = (global_value or "").strip()
        if not raw:
            return None
        try:
            parsed = int(raw)
        except ValueError:
            logger.warning(
                "x_playwright: invalid %s=%r (expected integer) — ignoring",
                label,
                raw,
            )
            return None
        if parsed < 0:
            logger.warning(
                "x_playwright: invalid %s=%r (must be >= 0) — ignoring",
                label,
                raw,
            )
            return None
        return parsed

    def build_advanced_query(
        self,
        query: str,
        since: Optional[str] = None,
        until: Optional[str] = None,
        min_replies: Optional[int] = None,
        min_faves: Optional[int] = None,
        min_retweets: Optional[int] = None,
    ) -> str:
        parts = [query.strip()]

        replies = self.normalize_min_engagement(min_replies, X_MIN_REPLIES, "min_replies")
        faves = self.normalize_min_engagement(min_faves, X_MIN_FAVES, "min_faves")
        retweets = self.normalize_min_engagement(min_retweets, X_MIN_RETWEETS, "min_retweets")
        if replies is not None:
            parts.append(f"min_replies:{replies}")
        if faves is not None:
            parts.append(f"min_faves:{faves}")
        if retweets is not None:
            parts.append(f"min_retweets:{retweets}")

        since_date = self.normalize_date(
            since if since is not None else X_SINCE_DATE,
            "since",
        )
        until_date = self.normalize_date(
            until if until is not None else X_UNTIL_DATE,
            "until",
        )
        if since_date:
            parts.append(f"since:{since_date}")
        if until_date:
            parts.append(f"until:{until_date}")
        return " ".join(parts)

    def build_url(
        self,
        query: str,
        *,
        search_filter: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        min_replies: Optional[int] = None,
        min_faves: Optional[int] = None,
        min_retweets: Optional[int] = None,
    ) -> str:
        advanced_query = self.build_advanced_query(
            query,
            since=since,
            until=until,
            min_replies=min_replies,
            min_faves=min_faves,
            min_retweets=min_retweets,
        )
        encoded = quote_plus(advanced_query)
        filt = self.normalize_search_filter(search_filter)
        return f"https://x.com/search?q={encoded}&src=typed_query&f={filt}"

    def fetch(
        self,
        query: str,
        limit: Optional[int] = None,
        *,
        search_filter: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        min_replies: Optional[int] = None,
        min_faves: Optional[int] = None,
        min_retweets: Optional[int] = None,
        include_comments: bool = False,
        num_comment_crawl: Optional[int] = None,
        get_replies: Optional[bool] = None,
        on_item=None,
    ) -> CrawlResult:
        query = (query or "").strip()
        if not query:
            logger.warning("x_playwright search: empty query")
            return empty_crawl_result()

        filt = self.normalize_search_filter(search_filter)
        url = self.build_url(
            query,
            search_filter=filt,
            since=since,
            until=until,
            min_replies=min_replies,
            min_faves=min_faves,
            min_retweets=min_retweets,
        )
        result = self.client.crawl_feed_and_comments(
            url,
            limit,
            SOURCE_KEY_SEARCH,
            include_comments=include_comments,
            num_comment_crawl=num_comment_crawl,
            get_replies=get_replies,
            on_item=on_item,
        )
        logger.info(
            "x_playwright search posts=%d comments=%d query=%r filter=%s",
            len(result["posts"]),
            len(result["comments"]),
            query,
            filt,
        )
        return result


class XProfileCrawler:
    """Crawl posts from a specific X user profile timeline."""

    def __init__(
        self,
        client: Optional[XPlaywrightClient] = None,
        comments_crawler: Optional[XCommentsCrawler] = None,
    ):
        self.client = client or XPlaywrightClient()
        self.comments_crawler = comments_crawler or XCommentsCrawler(self.client)

    @staticmethod
    def normalize_username(username_or_url: str) -> Optional[str]:
        raw = (username_or_url or "").strip()
        if not raw:
            return None

        if raw.startswith("http://") or raw.startswith("https://"):
            path = urlparse(raw).path.strip("/")
            if not path:
                return None
            raw = path.split("/")[0]

        raw = raw.lstrip("@").strip()
        if not raw or not USERNAME_RE.match(raw):
            logger.warning(
                "x_playwright profile: invalid username=%r",
                username_or_url,
            )
            return None
        return raw

    @staticmethod
    def build_url(username: str) -> str:
        return f"https://x.com/{username}"

    def fetch(
        self,
        username_or_url: str,
        limit: Optional[int] = None,
        *,
        include_comments: bool = False,
        num_comment_crawl: Optional[int] = None,
        get_replies: Optional[bool] = None,
    ) -> CrawlResult:
        username = self.normalize_username(username_or_url)
        if not username:
            return empty_crawl_result()

        url = self.build_url(username)
        result = self.client.crawl_feed_and_comments(
            url,
            limit,
            SOURCE_KEY_PROFILE,
            include_comments=include_comments,
            num_comment_crawl=num_comment_crawl,
            get_replies=get_replies,
        )
        logger.info(
            "x_playwright profile posts=%d comments=%d user=@%s",
            len(result["posts"]),
            len(result["comments"]),
            username,
        )
        return result


# --- module-level wrappers ---------------------------------------------------

_default_client = XPlaywrightClient()
_search_crawler = XSearchCrawler(_default_client)
_profile_crawler = XProfileCrawler(_default_client)


def fetch(
    query: str,
    limit: Optional[int] = None,
    *,
    search_filter: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    min_replies: Optional[int] = None,
    min_faves: Optional[int] = None,
    min_retweets: Optional[int] = None,
    include_comments: bool = False,
    num_comment_crawl: Optional[int] = None,
    get_replies: Optional[bool] = None,
    on_item=None,
) -> CrawlResult:
    """Search X — always returns {"posts": [...], "comments": [...]}."""
    return _search_crawler.fetch(
        query,
        limit=limit,
        search_filter=search_filter,
        since=since,
        until=until,
        min_replies=min_replies,
        min_faves=min_faves,
        min_retweets=min_retweets,
        include_comments=include_comments,
        num_comment_crawl=num_comment_crawl,
        get_replies=get_replies,
        on_item=on_item,
    )


def fetch_profile(
    username_or_url: str,
    limit: Optional[int] = None,
    *,
    include_comments: bool = False,
    num_comment_crawl: Optional[int] = None,
    get_replies: Optional[bool] = None,
) -> CrawlResult:
    """Fetch profile timeline — always returns {"posts": [...], "comments": [...]}."""
    return _profile_crawler.fetch(
        username_or_url,
        limit=limit,
        include_comments=include_comments,
        num_comment_crawl=num_comment_crawl,
        get_replies=get_replies,
    )


def _print_payload(label: str, payload: CrawlResult, duration_ms: int) -> None:
    posts = payload.get("posts") or []
    comments = payload.get("comments") or []
    print(
        f"{label} posts={len(posts)} comments={len(comments)} duration_ms={duration_ms}"
    )
    for i, row in enumerate(posts, start=1):
        engagement = row.get("engagement") or {}
        print(
            f"{i}. {row.get('author')} | likes={engagement.get('likes')} "
            f"retweets={engagement.get('retweets')} comments={engagement.get('comments')} "
            f"views={engagement.get('views')}"
        )
        print(f"   title: {row.get('title')}")
        print(f"   url:   {row.get('url')}")
    if comments:
        print("--- comments (sample up to 5) ---")
        for c in comments[:5]:
            print(
                f"  - {c.get('author')} parent={c.get('parent_content_id')} "
                f"nested_under={c.get('parent_comment_id')} likes={c.get('like_count')}"
            )
            text = (c.get("text") or "")[:100]
            print(f"    {text}")


def _add_comment_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--include-comments",
        action="store_true",
        help="Also crawl replies for each collected post",
    )
    parser.add_argument(
        "--num-comment-crawl",
        type=int,
        default=None,
        help=f"Max top-level replies per post (cap {X_COMMENTS_HARD_CAP})",
    )
    parser.add_argument(
        "--get-replies",
        action="store_true",
        help="Also include nested replies (default: X_GET_REPLIES global)",
    )


def _add_search_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--query", default="news", help='Search query (default: "news")')
    parser.add_argument("--limit", type=int, default=5, help="Max tweets to fetch")
    parser.add_argument(
        "--filter",
        dest="search_filter",
        default=None,
        choices=sorted(ALLOWED_SEARCH_FILTERS),
        help='Search tab: "live" (Latest/time) or "top" (default: X_SEARCH_FILTER)',
    )
    parser.add_argument("--since", default=None, help="Advanced Search start YYYY-MM-DD")
    parser.add_argument("--until", default=None, help="Advanced Search end YYYY-MM-DD (exclusive)")
    parser.add_argument("--min-replies", type=int, default=None, help="Min replies")
    parser.add_argument("--min-faves", type=int, default=None, help="Min likes/faves")
    parser.add_argument("--min-retweets", type=int, default=None, help="Min retweets")
    _add_comment_args(parser)
    parser.add_argument("--json", action="store_true", help="Print full JSON output")


def _cli_main(argv: Optional[List[str]] = None) -> int:
    raw_argv = list(argv if argv is not None else sys.argv[1:])
    if not raw_argv:
        raw_argv = ["search"]
    elif raw_argv[0] not in ("search", "profile"):
        raw_argv = ["search"] + raw_argv

    parser = argparse.ArgumentParser(description="X Playwright crawler (search or profile).")
    subparsers = parser.add_subparsers(dest="mode")

    search_parser = subparsers.add_parser("search", help="Crawl X search results")
    _add_search_args(search_parser)

    profile_parser = subparsers.add_parser("profile", help="Crawl a user profile timeline")
    profile_parser.add_argument(
        "--user",
        required=True,
        help="X username, @handle, or profile URL (e.g. Roboticmarketer)",
    )
    profile_parser.add_argument("--limit", type=int, default=5, help="Max tweets to fetch")
    _add_comment_args(profile_parser)
    profile_parser.add_argument("--json", action="store_true", help="Print full JSON output")

    args = parser.parse_args(raw_argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if not _default_client.session_configured():
        print(
            "SKIP: Set X_AUTH_TOKEN and X_CT0 at the top of api/sources/x_playwright.py "
            "after logging into x.com in your browser."
        )
        return 2

    # --get-replies present → True; absent → None (fall back to X_GET_REPLIES)
    get_replies_arg = True if args.get_replies else None

    started = time.perf_counter()

    if args.mode == "profile":
        payload = fetch_profile(
            args.user,
            limit=args.limit,
            include_comments=bool(args.include_comments),
            num_comment_crawl=args.num_comment_crawl,
            get_replies=get_replies_arg,
        )
        duration_ms = int((time.perf_counter() - started) * 1000)
        if args.json:
            print(json.dumps(payload, indent=2, ensure_ascii=False))
        else:
            _print_payload(f"profile user={args.user!r}", payload, duration_ms)
    else:
        payload = fetch(
            args.query,
            limit=args.limit,
            search_filter=args.search_filter,
            since=args.since,
            until=args.until,
            min_replies=args.min_replies,
            min_faves=args.min_faves,
            min_retweets=args.min_retweets,
            include_comments=bool(args.include_comments),
            num_comment_crawl=args.num_comment_crawl,
            get_replies=get_replies_arg,
        )
        duration_ms = int((time.perf_counter() - started) * 1000)
        if args.json:
            print(json.dumps(payload, indent=2, ensure_ascii=False))
        else:
            _print_payload(f"search query={args.query!r}", payload, duration_ms)

    return 0 if payload.get("posts") else 1


if __name__ == "__main__":
    raise SystemExit(_cli_main())
