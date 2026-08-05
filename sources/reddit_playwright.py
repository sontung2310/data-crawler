"""
Reddit crawler via Playwright — keyword search, subreddit feeds, and comments.

Scrapes www.reddit.com (new Reddit / shreddit). Session cookies must be copied
from a logged-in browser.

Both search and subreddit return a payload shaped for downstream persistence:

    {"posts": [...], "comments": [...]}

After collecting feed/search cards, each post page is opened once to enrich
metadata (author, body, published time, domain, engagement). When
--include-comments is set, comments are collected during that same visit.
Persistence to Mongo is out of scope here.

================================================================================
CLI
================================================================================

Search (keyword):
  python -m api.sources.reddit_playwright search [options]

  --query TEXT
  --limit N
  --sort {relevance,hot,top,new,comments}   (default: relevance)
  --time-filter {hour,day,week,month,year}
  --include-comments
  --num-comment-crawl N
  --get-replies
  --json

Subreddit:
  python -m api.sources.reddit_playwright subreddit [options]

  --subreddit TEXT      Name or r/name (e.g. digital_marketing)
  --limit N
  --sort {hot,top,new,best,rising}
  --time-filter {hour,day,week,month,year}
  --include-comments
  --num-comment-crawl N
  --get-replies
  --json

Examples:
  python -m api.sources.reddit_playwright search --query "SEO" --limit 5
      --include-comments --get-replies --json

  python -m api.sources.reddit_playwright search --query "AI in Marketing" --limit 5
      --sort relevance --time-filter week --include-comments --num-comment-crawl 3 --json

  python -m api.sources.reddit_playwright search --query "SEO" --limit 5
      --sort top --time-filter week --include-comments --get-replies --json

  python -m api.sources.reddit_playwright subreddit --subreddit r/digital_marketing
      --limit 5 --sort hot --include-comments --num-comment-crawl 3 --get-replies --json
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
from urllib.parse import urlencode, urlparse

import requests
from playwright.sync_api import Browser, BrowserContext, Page, TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from .utils import (
    clean_html_to_text,
    crawled_at_now,
    filter_rows_by_query,
    human_delay,
    keyword_search_query,
    overfetch_limit,
    parse_engagement_count,
    resolve_limit,
    truncate_title,
)

logger = logging.getLogger(__name__)

# --- session from env (REDDIT_SESSION, REDDIT_TOKEN_V2) ---------------------
from config import REDDIT_SESSION, REDDIT_TOKEN_V2
from exceptions import SessionExpiredError

# --- crawl settings ----------------------------------------------------------
REDDIT_HEADLESS = True
REDDIT_SCROLL_DELAY_MIN = 1.5
REDDIT_SCROLL_DELAY_MAX = 3.5
REDDIT_NAV_TIMEOUT_MS = 45000
REDDIT_MAX_SCROLLS = 50
REDDIT_SETTLE_MS = 4000

REDDIT_COMMENT_PAGE_DELAY_MIN = 2.0
REDDIT_COMMENT_PAGE_DELAY_MAX = 4.5
REDDIT_COMMENT_SCROLL_MAX = 6
REDDIT_MORE_COMMENTS_CLICKS = 8

REDDIT_COMMENTS_HARD_CAP = 200
REDDIT_GET_REPLIES = False

SOURCE_KEY_SEARCH = "reddit_playwright"
SOURCE_KEY_SUBREDDIT = "reddit_playwright_subreddit"
SOURCE_KEY_COMMENTS = "reddit_playwright_comments"

REDDIT_ORIGIN = "https://www.reddit.com"
# Subreddit / post pages use shreddit-post; search results use sdui-post-unit cards.
SHREDDIT_POST_SELECTOR = "shreddit-post"
SEARCH_POST_SELECTOR = "[data-testid='sdui-post-unit']"
POST_READY_SELECTOR = f"{SHREDDIT_POST_SELECTOR}, {SEARCH_POST_SELECTOR}"
COMMENT_SELECTOR = "shreddit-comment"

THING_ID_RE = re.compile(r"^t[13]_([a-z0-9]+)$", re.IGNORECASE)
PERMALINK_ID_RE = re.compile(r"/comments/([a-z0-9]+)/", re.IGNORECASE)
SUBREDDIT_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")

ALLOWED_SEARCH_SORTS = {"relevance", "hot", "top", "new", "comments"}
ALLOWED_SUBREDDIT_SORTS = {"hot", "top", "new", "best", "rising"}
ALLOWED_TIME_FILTERS = {"hour", "day", "week", "month", "year"}
# Default for keyword search (Posts tab). Subreddit listings keep Reddit's own default.
DEFAULT_SEARCH_SORT = "relevance"
SEARCH_TIME_SORTS = {"top", "relevance"}
SUBREDDIT_TIME_SORTS = {"top"}

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


def normalize_subreddit(name: str) -> str:
    raw = (name or "").strip()
    if not raw:
        raise ValueError("subreddit name is required")
    if "reddit.com" in raw:
        path = urlparse(raw if "://" in raw else f"https://{raw}").path
        parts = [p for p in path.split("/") if p]
        if parts and parts[0].lower() == "r" and len(parts) >= 2:
            raw = parts[1]
        else:
            raise ValueError(f"could not parse subreddit from URL: {name!r}")
    raw = raw.lstrip("/")
    if raw.lower().startswith("r/"):
        raw = raw[2:]
    raw = raw.strip("/")
    if not SUBREDDIT_NAME_RE.match(raw):
        raise ValueError(
            f"invalid subreddit name {name!r}; expected e.g. digital_marketing or r/digital_marketing"
        )
    return raw


def normalize_time_filter(time_filter: Optional[str]) -> Optional[str]:
    if time_filter is None:
        return None
    value = str(time_filter).strip().lower()
    if not value:
        return None
    if value not in ALLOWED_TIME_FILTERS:
        raise ValueError(
            f"invalid time_filter {time_filter!r}; "
            f"allowed: {', '.join(sorted(ALLOWED_TIME_FILTERS))}"
        )
    return value


def normalize_sort(sort: Optional[str], allowed: set[str], mode: str) -> Optional[str]:
    if sort is None:
        return None
    value = str(sort).strip().lower()
    if not value:
        return None
    if value not in allowed:
        raise ValueError(
            f"invalid sort {sort!r} for {mode}; "
            f"allowed: {', '.join(sorted(allowed))}"
        )
    return value


def apply_time_filter(
    time_filter: Optional[str],
    sort: Optional[str],
    allowed_sorts: set[str],
    mode: str,
) -> Optional[str]:
    """Return t= value to apply, or None after ignore+warn when unsupported."""
    t = normalize_time_filter(time_filter)
    if t is None:
        return None
    effective_sort = sort or ("hot" if mode == "subreddit" else DEFAULT_SEARCH_SORT)
    if effective_sort not in allowed_sorts:
        logger.warning(
            "reddit_playwright: ignoring time_filter=%s for %s sort=%s "
            "(only applies with %s)",
            t,
            mode,
            effective_sort,
            ", ".join(sorted(allowed_sorts)),
        )
        return None
    return t


def post_id_from_row(row: Dict) -> Optional[str]:
    post_id = (row.get("post_id") or "").strip()
    if ":" in post_id:
        return post_id.rsplit(":", 1)[-1] or None
    url = row.get("url") or ""
    match = PERMALINK_ID_RE.search(url)
    return match.group(1) if match else None


def resolve_comment_limit(num_comment_crawl: Optional[int]) -> int:
    if num_comment_crawl is None:
        return REDDIT_COMMENTS_HARD_CAP
    try:
        n = int(num_comment_crawl)
    except (TypeError, ValueError):
        return REDDIT_COMMENTS_HARD_CAP
    if n <= 0:
        return 0
    return min(n, REDDIT_COMMENTS_HARD_CAP)


def resolve_get_replies(get_replies: Optional[bool]) -> bool:
    if get_replies is None:
        return bool(REDDIT_GET_REPLIES)
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


def thing_fullname_to_id(fullname: Optional[str]) -> Optional[str]:
    if not fullname:
        return None
    match = THING_ID_RE.match(fullname.strip())
    if match:
        return match.group(1)
    bare = fullname.strip()
    if re.fullmatch(r"[a-z0-9]+", bare, re.IGNORECASE):
        return bare
    return None


def to_www_permalink(href: str) -> str:
    """Normalize relative / any-host permalink to https://www.reddit.com/..."""
    if not href:
        return ""
    if href.startswith("//"):
        href = "https:" + href
    if href.startswith("/"):
        return f"{REDDIT_ORIGIN}{href}"
    parsed = urlparse(href)
    path = parsed.path or ""
    query = f"?{parsed.query}" if parsed.query else ""
    fragment = f"#{parsed.fragment}" if parsed.fragment else ""
    return f"{REDDIT_ORIGIN}{path}{query}{fragment}"


def attr_int(raw: Optional[str]) -> Optional[int]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return parse_engagement_count(text)


class RedditPlaywrightClient:
    """Shared Playwright engine: browser session, feed scroll, post/comment parsing."""

    def session_configured(self) -> bool:
        return bool((REDDIT_SESSION or "").strip())

    def build_cookies(self) -> List[Dict]:
        cookies: List[Dict] = [
            {
                "name": "reddit_session",
                "value": REDDIT_SESSION.strip(),
                "domain": ".reddit.com",
                "path": "/",
            }
        ]
        if (REDDIT_TOKEN_V2 or "").strip():
            cookies.append(
                {
                    "name": "token_v2",
                    "value": REDDIT_TOKEN_V2.strip(),
                    "domain": ".reddit.com",
                    "path": "/",
                }
            )
        return cookies

    def check_robots_txt(self) -> None:
        try:
            r = requests.get(
                f"{REDDIT_ORIGIN}/robots.txt",
                headers={"User-Agent": DESKTOP_UA},
                timeout=10,
            )
            logger.info(
                "www.reddit.com robots.txt status=%s (advisory only)", r.status_code
            )
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
        listing_only: bool = False,
    ) -> CrawlResult:
        if not self.session_configured():
            raise SessionExpiredError(
                "reddit_playwright",
                "REDDIT_SESSION must be set in .env",
            )

        limit = resolve_limit(limit, hard_cap=100) or 100
        self.check_robots_txt()
        out = empty_crawl_result()

        try:
            with sync_playwright() as playwright:
                browser: Browser = playwright.chromium.launch(headless=REDDIT_HEADLESS)
                context: BrowserContext = browser.new_context(
                    viewport={"width": 1280, "height": 800},
                    user_agent=DESKTOP_UA,
                )
                context.add_cookies(self.build_cookies())
                page = context.new_page()

                posts = self._navigate_and_collect(page, feed_url, limit, source_key)
                out["posts"] = posts

                # Listing-only: skip post-page visits (used by multi-mode discovery).
                if listing_only:
                    if on_item:
                        for post in posts:
                            on_item("post", post)
                    context.close()
                    browser.close()
                    return out

                # One visit per post: enrich metadata; optionally collect comments.
                # Persist posts after enrich (via on_item inside visit_posts).
                if posts:
                    page_crawler = RedditCommentsCrawler(self)
                    out["comments"] = page_crawler.visit_posts(
                        page,
                        posts,
                        include_comments=include_comments,
                        num_comment_crawl=num_comment_crawl,
                        get_replies=get_replies,
                        on_item=on_item,
                    )

                context.close()
                browser.close()
        except SessionExpiredError:
            raise
        except Exception as exc:
            logger.exception("reddit_playwright crawl failed: %s", exc)
            return out

        return out

    def _navigate_and_collect(
        self, page: Page, url: str, limit: int, source_key: str
    ) -> List[Dict]:
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=REDDIT_NAV_TIMEOUT_MS)
            page.wait_for_timeout(REDDIT_SETTLE_MS)
            page.wait_for_selector(POST_READY_SELECTOR, timeout=REDDIT_NAV_TIMEOUT_MS)
        except PlaywrightTimeoutError:
            if self.is_login_wall(page):
                raise SessionExpiredError(
                    "reddit_playwright",
                    "login wall detected — refresh REDDIT_SESSION in .env",
                )
            logger.warning("reddit_playwright: timed out waiting for posts url=%s", url)
            return []

        if self.is_login_wall(page):
            raise SessionExpiredError(
                "reddit_playwright",
                "login wall detected — refresh REDDIT_SESSION in .env",
            )

        return self.collect_posts(page, limit, source_key)

    def is_login_wall(self, page: Page) -> bool:
        try:
            url = page.url.lower()
            if "/login" in url:
                return True
            if page.locator(POST_READY_SELECTOR).count() == 0:
                body = (page.locator("body").inner_text() or "").lower()
                if any(
                    phrase in body
                    for phrase in (
                        "log in",
                        "you've been blocked",
                        "blocked by network security",
                        "verify you are human",
                    )
                ):
                    return True
        except Exception:
            pass
        return False

    def feed_post_locator(self, page: Page):
        shreddit = page.locator(SHREDDIT_POST_SELECTOR)
        if shreddit.count() > 0:
            return shreddit
        return page.locator(SEARCH_POST_SELECTOR)

    def collect_posts(self, page: Page, limit: int, source_key: str) -> List[Dict]:
        seen_ids: set[str] = set()
        results: List[Dict] = []

        for _ in range(REDDIT_MAX_SCROLLS):
            things = self.feed_post_locator(page)
            count = things.count()
            page_added = 0

            for i in range(count):
                thing = things.nth(i)
                post_id = self.extract_post_id(thing)
                if not post_id or post_id in seen_ids:
                    continue

                mapped = self.map_post(thing, post_id, source_key)
                if not mapped:
                    continue

                seen_ids.add(post_id)
                results.append(mapped)
                page_added += 1
                if len(results) >= limit:
                    return results[:limit]

            human_delay(REDDIT_SCROLL_DELAY_MIN, REDDIT_SCROLL_DELAY_MAX)
            before = self.feed_post_locator(page).count()
            page.evaluate(
                "window.scrollBy(0, Math.floor(window.innerHeight * 0.9));"
            )
            page.wait_for_timeout(1200)
            after = self.feed_post_locator(page).count()
            if after <= before and page_added == 0:
                break

        return results[:limit]

    def try_expand_more_comments(self, page: Page) -> None:
        patterns = [
            "button:has-text('View more comments')",
            "button:has-text('View more replies')",
            "button:has-text('More replies')",
            "button:has-text('Continue this thread')",
            "faceplate-tracker[noun='load_more_comments'] button",
            "a:has-text('Continue this thread')",
        ]
        clicks = 0
        for pattern in patterns:
            if clicks >= REDDIT_MORE_COMMENTS_CLICKS:
                break
            loc = page.locator(pattern)
            try:
                n = min(loc.count(), 3)
            except Exception:
                continue
            for i in range(n):
                if clicks >= REDDIT_MORE_COMMENTS_CLICKS:
                    break
                try:
                    loc.nth(i).click(timeout=1500)
                    clicks += 1
                    human_delay(0.6, 1.2)
                except Exception:
                    continue

    def map_post(self, thing, post_id: str, source_key: str) -> Optional[Dict]:
        title = self.extract_title(thing)
        text_html = self.extract_body_html(thing)
        text = clean_html_to_text(text_html) if text_html else ""
        if not title and not text:
            return None

        permalink = self.extract_permalink(thing, post_id)
        author = self.extract_author(thing)
        subreddit = self.extract_subreddit(thing)

        return {
            "post_id": f"{source_key}:{post_id}",
            "source": source_key,
            "url": permalink,
            "title": title or truncate_title(text),
            "text": text,
            "text_html": text_html or "",
            "published_ts": self.extract_published_ts(thing),
            "crawled_at": crawled_at_now(),
            "author": author,
            "domain": self.extract_domain(thing),
            "subreddit": subreddit,
            "engagement": self.extract_engagement(thing),
        }

    @staticmethod
    def parse_datetime_to_ts(value: Optional[str]) -> Optional[int]:
        if not value:
            return None
        try:
            normalized = value.replace("Z", "+00:00")
            # Reddit sometimes omits colon in offset (+0000)
            if re.search(r"[+-]\d{4}$", normalized):
                normalized = normalized[:-2] + ":" + normalized[-2:]
            dt = datetime.fromisoformat(normalized)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            return None

    def extract_post_id(self, thing) -> Optional[str]:
        try:
            tag = (thing.evaluate("el => el.tagName") or "").lower()
        except Exception:
            tag = ""

        if tag == "shreddit-post":
            try:
                tid = thing_fullname_to_id(thing.get_attribute("id"))
                if tid:
                    return tid
            except Exception:
                pass
            try:
                href = (
                    thing.get_attribute("permalink")
                    or thing.get_attribute("content-href")
                    or ""
                )
                match = PERMALINK_ID_RE.search(href)
                if match:
                    return match.group(1)
            except Exception:
                pass
            return None

        # Search card (sdui-post-unit)
        try:
            href = ""
            title_link = thing.locator(
                "a[data-testid='post-title'], [data-testid='post-title-text']"
            ).first
            if title_link.count() > 0:
                # title text node may not be the <a>; climb to nearest link
                href = title_link.evaluate(
                    """(el) => {
                      const a = el.closest('a') || el.querySelector('a') || el;
                      return a.getAttribute('href') || '';
                    }"""
                )
            if not href:
                link = thing.locator("a[href*='/comments/']").first
                if link.count() > 0:
                    href = link.get_attribute("href") or ""
            match = PERMALINK_ID_RE.search(href or "")
            if match:
                return match.group(1)
        except Exception:
            pass
        return None

    def extract_title(self, thing) -> str:
        try:
            tag = (thing.evaluate("el => el.tagName") or "").lower()
        except Exception:
            tag = ""

        if tag == "shreddit-post":
            try:
                title = thing.get_attribute("post-title") or ""
                if title:
                    return clean_html_to_text(title)
            except Exception:
                pass
            try:
                slot = thing.locator("a[slot='title'], [slot='title']").first
                if slot.count() > 0:
                    return clean_html_to_text(slot.inner_text())
            except Exception:
                pass
            return ""

        try:
            title_node = thing.locator("[data-testid='post-title-text']").first
            if title_node.count() > 0:
                return clean_html_to_text(title_node.inner_text())
        except Exception:
            pass
        return ""

    def extract_body_html(self, thing) -> str:
        """Caption / selftext only — ignore media and outbound link content."""
        try:
            body = thing.locator(
                "[id$='-post-rtjson-content'], "
                "[data-post-click-location='text-body'], "
                "div[slot='text-body']"
            ).first
            if body.count() > 0:
                return body.inner_html() or ""
        except Exception:
            pass
        return ""

    def extract_permalink(self, thing, post_id: str) -> str:
        try:
            tag = (thing.evaluate("el => el.tagName") or "").lower()
        except Exception:
            tag = ""

        if tag == "shreddit-post":
            try:
                href = (
                    thing.get_attribute("permalink")
                    or thing.get_attribute("content-href")
                    or ""
                )
                if href:
                    return to_www_permalink(href)
            except Exception:
                pass
        else:
            try:
                href = thing.evaluate(
                    """(el) => {
                      const t = el.querySelector("[data-testid='post-title-text']");
                      const a = t ? (t.closest('a') || t.querySelector('a')) : null;
                      if (a && a.getAttribute('href')) return a.getAttribute('href');
                      const c = el.querySelector("a[href*='/comments/']");
                      return c ? c.getAttribute('href') : '';
                    }"""
                )
                if href:
                    return to_www_permalink(href)
            except Exception:
                pass

        sub = self.extract_subreddit(thing) or "reddit"
        return f"{REDDIT_ORIGIN}/r/{sub}/comments/{post_id}/"

    def extract_author(self, thing) -> str:
        try:
            tag = (thing.evaluate("el => el.tagName") or "").lower()
        except Exception:
            tag = ""

        if tag == "shreddit-post":
            try:
                author = (thing.get_attribute("author") or "").strip()
                if author and author.lower() not in {"[deleted]", "[removed]"}:
                    return author
            except Exception:
                pass
            return ""

        try:
            author = thing.locator("a[href*='/user/'], a[href*='/u/']").first
            if author.count() > 0:
                text = clean_html_to_text(author.inner_text())
                if text:
                    return text.lstrip("u/")
                href = author.get_attribute("href") or ""
                return href.rstrip("/").split("/")[-1]
        except Exception:
            pass
        return ""

    def extract_subreddit(self, thing) -> str:
        try:
            tag = (thing.evaluate("el => el.tagName") or "").lower()
        except Exception:
            tag = ""

        if tag == "shreddit-post":
            try:
                name = (thing.get_attribute("subreddit-name") or "").strip()
                if name:
                    return name
                prefixed = (thing.get_attribute("subreddit-prefixed-name") or "").strip()
                if prefixed.lower().startswith("r/"):
                    return prefixed[2:]
            except Exception:
                pass

        try:
            link = thing.locator("a[href*='/r/']").first
            if link.count() > 0:
                text = clean_html_to_text(link.inner_text())
                if text.lower().startswith("r/"):
                    return text[2:]
                href = link.get_attribute("href") or ""
                parts = [p for p in urlparse(href).path.split("/") if p]
                if parts and parts[0].lower() == "r" and len(parts) >= 2:
                    return parts[1]
        except Exception:
            pass

        try:
            href = thing.get_attribute("permalink") or ""
            parts = [p for p in urlparse(to_www_permalink(href)).path.split("/") if p]
            if parts and parts[0].lower() == "r" and len(parts) >= 2:
                return parts[1]
        except Exception:
            pass
        return ""

    def extract_published_ts(self, thing) -> Optional[int]:
        try:
            created = thing.get_attribute("created-timestamp")
            ts = self.parse_datetime_to_ts(created)
            if ts is not None:
                return ts
        except Exception:
            pass
        try:
            time_node = thing.locator("faceplate-timeago time, time").first
            if time_node.count() > 0:
                return self.parse_datetime_to_ts(time_node.get_attribute("datetime"))
        except Exception:
            pass
        return None

    def extract_domain(self, thing) -> str:
        try:
            domain = (thing.get_attribute("domain") or "").strip()
            if domain:
                return domain
        except Exception:
            pass
        return "reddit.com"

    def extract_engagement(self, thing) -> Dict[str, Optional[int]]:
        return {
            "upvotes": self.extract_score(thing),
            "comments": self.extract_num_comments(thing),
            "share": self.extract_share_count(thing),
        }

    def extract_score(self, thing) -> Optional[int]:
        try:
            score = attr_int(thing.get_attribute("score"))
            if score is not None:
                return score
        except Exception:
            pass
        # Search cards: first faceplate-number is typically upvotes
        try:
            nums = thing.locator("faceplate-number")
            if nums.count() > 0:
                return attr_int(nums.nth(0).get_attribute("number"))
        except Exception:
            pass
        return None

    def extract_num_comments(self, thing) -> Optional[int]:
        try:
            count = attr_int(thing.get_attribute("comment-count"))
            if count is not None:
                return count
        except Exception:
            pass
        try:
            nums = thing.locator("faceplate-number")
            if nums.count() > 1:
                return attr_int(nums.nth(1).get_attribute("number"))
        except Exception:
            pass
        return None

    @staticmethod
    def extract_share_count(thing) -> Optional[int]:
        """Crosspost / repost count when exposed; else None."""
        for attr in ("number-crossposts", "crosspost-count", "num-crossposts"):
            try:
                value = attr_int(thing.get_attribute(attr))
                if value is not None:
                    return value
            except Exception:
                continue
        try:
            cross = thing.locator(
                "[aria-label*='crosspost' i], button:has-text('crosspost')"
            ).first
            if cross.count() > 0:
                return parse_engagement_count(
                    cross.get_attribute("aria-label") or cross.inner_text() or ""
                )
        except Exception:
            pass
        return None

    def enrich_post_from_page(self, page: Page, post: Dict) -> None:
        """Fill/overwrite sparse feed/search fields from the post page shreddit-post."""
        try:
            thing = page.locator(SHREDDIT_POST_SELECTOR).first
            if thing.count() == 0:
                return
        except Exception:
            return

        title = self.extract_title(thing)
        if title:
            post["title"] = title

        text_html = self.extract_body_html(thing)
        if text_html:
            post["text_html"] = text_html
            post["text"] = clean_html_to_text(text_html)
        elif not (post.get("text") or "").strip():
            # Keep empty body for link/image posts with no caption.
            post.setdefault("text", "")
            post.setdefault("text_html", "")

        author = self.extract_author(thing)
        if author:
            post["author"] = author

        published_ts = self.extract_published_ts(thing)
        if published_ts is not None:
            post["published_ts"] = published_ts

        domain = self.extract_domain(thing)
        if domain and domain != "reddit.com":
            post["domain"] = domain
        elif not post.get("domain"):
            post["domain"] = domain or "reddit.com"

        subreddit = self.extract_subreddit(thing)
        if subreddit:
            post["subreddit"] = subreddit

        page_engagement = self.extract_engagement(thing)
        engagement = dict(post.get("engagement") or {})
        for key in ("upvotes", "comments", "share"):
            value = page_engagement.get(key)
            if value is not None:
                engagement[key] = value
            else:
                engagement.setdefault(key, None)
        post["engagement"] = engagement

        # Prefer canonical permalink from the post page when present.
        post_id = post_id_from_row(post)
        if post_id:
            permalink = self.extract_permalink(thing, post_id)
            if permalink:
                post["url"] = permalink


class RedditCommentsCrawler:
    """Open each post page once: enrich metadata, optionally collect comments."""

    def __init__(self, client: Optional[RedditPlaywrightClient] = None):
        self.client = client or RedditPlaywrightClient()

    def visit_posts(
        self,
        page: Page,
        posts: List[Dict],
        *,
        include_comments: bool = False,
        num_comment_crawl: Optional[int] = None,
        get_replies: Optional[bool] = None,
        on_item=None,
    ) -> List[Dict]:
        """Visit each post once. Always enrich; collect comments when requested."""
        all_comments: List[Dict] = []
        for post in posts:
            human_delay(
                REDDIT_COMMENT_PAGE_DELAY_MIN, REDDIT_COMMENT_PAGE_DELAY_MAX
            )
            rows = self.visit_post(
                page,
                post,
                include_comments=include_comments,
                num_comment_crawl=num_comment_crawl,
                get_replies=get_replies,
            )
            if on_item:
                on_item("post", post)
                for row in rows:
                    on_item("comment", row)
            all_comments.extend(rows)
        return all_comments

    def visit_post(
        self,
        page: Page,
        post: Dict,
        *,
        include_comments: bool = False,
        num_comment_crawl: Optional[int] = None,
        get_replies: Optional[bool] = None,
    ) -> List[Dict]:
        """One navigation: enrich post metadata, then optionally crawl comments."""
        parent_id = post_id_from_row(post)
        if not parent_id:
            return []

        parent_url = (post.get("url") or "").strip() or (
            f"{REDDIT_ORIGIN}/comments/{parent_id}/"
        )
        parent_url = to_www_permalink(parent_url)

        try:
            page.goto(
                parent_url, wait_until="domcontentloaded", timeout=REDDIT_NAV_TIMEOUT_MS
            )
            page.wait_for_timeout(REDDIT_SETTLE_MS)
            page.wait_for_selector(
                f"{SHREDDIT_POST_SELECTOR}, {COMMENT_SELECTOR}",
                timeout=REDDIT_NAV_TIMEOUT_MS,
            )
        except PlaywrightTimeoutError:
            logger.warning("reddit_playwright post page: timeout on %s", parent_url)
            return []

        if self.client.is_login_wall(page):
            raise SessionExpiredError(
                "reddit_playwright",
                f"login wall on post page {parent_url} — refresh REDDIT_SESSION in .env",
            )

        self.client.enrich_post_from_page(page, post)
        if not (post.get("url") or "").strip():
            post["url"] = parent_url

        if not include_comments:
            return []

        limit = resolve_comment_limit(num_comment_crawl)
        if limit <= 0:
            return []

        include_nested = resolve_get_replies(get_replies)
        for _ in range(REDDIT_COMMENT_SCROLL_MAX):
            self.client.try_expand_more_comments(page)
            human_delay(0.8, 1.5)
            page.evaluate("window.scrollBy(0, Math.floor(window.innerHeight * 0.7));")

        return self._collect_comments(
            page,
            parent_content_id=parent_id,
            parent_content_url=post.get("url") or parent_url,
            limit=limit,
            include_nested=include_nested,
        )

    def _collect_comments(
        self,
        page: Page,
        *,
        parent_content_id: str,
        parent_content_url: str,
        limit: int,
        include_nested: bool,
    ) -> List[Dict]:
        results: List[Dict] = []
        seen: set[str] = set()
        top_level_count = 0

        comments = page.locator(COMMENT_SELECTOR)
        count = comments.count()

        for i in range(count):
            node = comments.nth(i)
            comment_id = self.extract_comment_id(node)
            if not comment_id or comment_id in seen:
                continue

            parent_comment_id = self.extract_parent_comment_id(node)
            is_top_level = parent_comment_id is None

            if is_top_level:
                if top_level_count >= limit:
                    if not include_nested:
                        break
                    continue
                top_level_count += 1
            elif not include_nested:
                continue

            text = self.extract_comment_text(node)
            if not text:
                continue

            mapped = map_comment_row(
                comment_id=comment_id,
                parent_content_id=parent_content_id,
                parent_content_url=parent_content_url,
                author=self.extract_comment_author(node),
                author_channel_id=None,
                text=text,
                like_count=self.extract_comment_score(node),
                reply_count=self.count_direct_replies(node) if is_top_level else 0,
                parent_comment_id=parent_comment_id,
                published_ts=self.extract_comment_ts(node),
            )
            if not mapped:
                continue

            seen.add(comment_id)
            results.append(mapped)

        return results

    def extract_comment_id(self, node) -> Optional[str]:
        try:
            return thing_fullname_to_id(node.get_attribute("thingid"))
        except Exception:
            return None

    def extract_parent_comment_id(self, node) -> Optional[str]:
        """Nested replies have an ancestor shreddit-comment; top-level do not."""
        try:
            depth = attr_int(node.get_attribute("depth"))
            if depth is not None and depth <= 0:
                return None
        except Exception:
            pass
        try:
            parent_fullname = node.evaluate(
                """(el) => {
                    let p = el.parentElement;
                    while (p) {
                        if (p.tagName && p.tagName.toLowerCase() === 'shreddit-comment') {
                            return p.getAttribute('thingid') || '';
                        }
                        p = p.parentElement;
                    }
                    return '';
                }"""
            )
            return thing_fullname_to_id(parent_fullname or None)
        except Exception:
            return None

    @staticmethod
    def extract_comment_author(node) -> str:
        try:
            author = (node.get_attribute("author") or "").strip()
            if author:
                return author
        except Exception:
            pass
        return ""

    @staticmethod
    def extract_comment_text(node) -> str:
        try:
            body = node.locator(
                "[slot='comment'], "
                "[id*='-comment-rtjson-content'], "
                "div.md"
            ).first
            if body.count() > 0:
                return clean_html_to_text(body.inner_text())
        except Exception:
            pass
        return ""

    @staticmethod
    def extract_comment_score(node) -> Optional[int]:
        try:
            return attr_int(node.get_attribute("score"))
        except Exception:
            return None

    def extract_comment_ts(self, node) -> Optional[int]:
        try:
            return self.client.parse_datetime_to_ts(node.get_attribute("created"))
        except Exception:
            return None

    @staticmethod
    def count_direct_replies(node) -> int:
        try:
            return node.locator(":scope shreddit-comment").count()
        except Exception:
            return 0


class RedditSearchCrawler:
    """Keyword search on www.reddit.com/search/."""

    def __init__(self, client: Optional[RedditPlaywrightClient] = None):
        self.client = client or RedditPlaywrightClient()

    def build_url(
        self,
        query: str,
        *,
        sort: Optional[str] = None,
        time_filter: Optional[str] = None,
    ) -> str:
        q = (query or "").strip()
        if not q:
            raise ValueError("search query is required")

        sort_n = normalize_sort(sort, ALLOWED_SEARCH_SORTS, "search") or DEFAULT_SEARCH_SORT
        t = apply_time_filter(time_filter, sort_n, SEARCH_TIME_SORTS, "search")

        # type=link keeps the Posts tab (not communities/comments/media).
        params: Dict[str, str] = {"q": q, "type": "link", "sort": sort_n}
        if t:
            params["t"] = t

        return f"{REDDIT_ORIGIN}/search/?{urlencode(params)}"

    def fetch(
        self,
        query: str,
        limit: Optional[int] = None,
        *,
        sort: Optional[str] = None,
        time_filter: Optional[str] = None,
        include_comments: bool = False,
        num_comment_crawl: Optional[int] = None,
        get_replies: Optional[bool] = None,
        on_item=None,
        listing_only: bool = False,
    ) -> CrawlResult:
        search_q = keyword_search_query(query) or (query or "").strip()
        want = resolve_limit(limit, hard_cap=None)
        fetch_n = overfetch_limit(want) if want is not None else limit
        url = self.build_url(search_q, sort=sort, time_filter=time_filter)
        logger.info("reddit_playwright search url=%s shaped=%r", url, search_q)
        result = self.client.crawl_feed_and_comments(
            url,
            fetch_n,
            SOURCE_KEY_SEARCH,
            include_comments=include_comments,
            num_comment_crawl=num_comment_crawl,
            get_replies=get_replies,
            on_item=on_item,
            listing_only=listing_only,
        )
        posts = filter_rows_by_query(result.get("posts") or [], query, limit=want, soft=True)
        return {"posts": posts, "comments": result.get("comments") or []}


class RedditSubredditCrawler:
    """Subreddit listing on www.reddit.com/r/{sub}/[sort]/."""

    def __init__(self, client: Optional[RedditPlaywrightClient] = None):
        self.client = client or RedditPlaywrightClient()

    def build_url(
        self,
        subreddit: str,
        *,
        sort: Optional[str] = None,
        time_filter: Optional[str] = None,
    ) -> str:
        sub = normalize_subreddit(subreddit)
        sort_n = normalize_sort(sort, ALLOWED_SUBREDDIT_SORTS, "subreddit")
        t = apply_time_filter(time_filter, sort_n, SUBREDDIT_TIME_SORTS, "subreddit")

        if sort_n:
            path = f"/r/{sub}/{sort_n}/"
        else:
            path = f"/r/{sub}/"

        if t:
            return f"{REDDIT_ORIGIN}{path}?{urlencode({'t': t})}"
        return f"{REDDIT_ORIGIN}{path}"

    def fetch(
        self,
        subreddit: str,
        limit: Optional[int] = None,
        *,
        sort: Optional[str] = None,
        time_filter: Optional[str] = None,
        include_comments: bool = False,
        num_comment_crawl: Optional[int] = None,
        get_replies: Optional[bool] = None,
    ) -> CrawlResult:
        url = self.build_url(subreddit, sort=sort, time_filter=time_filter)
        logger.info("reddit_playwright subreddit url=%s", url)
        return self.client.crawl_feed_and_comments(
            url,
            limit,
            SOURCE_KEY_SUBREDDIT,
            include_comments=include_comments,
            num_comment_crawl=num_comment_crawl,
            get_replies=get_replies,
        )


_default_client = RedditPlaywrightClient()
_search_crawler = RedditSearchCrawler(_default_client)
_subreddit_crawler = RedditSubredditCrawler(_default_client)


def fetch(
    query: str,
    limit: Optional[int] = None,
    *,
    sort: Optional[str] = None,
    time_filter: Optional[str] = None,
    include_comments: bool = False,
    num_comment_crawl: Optional[int] = None,
    get_replies: Optional[bool] = None,
    on_item=None,
    listing_only: bool = False,
) -> CrawlResult:
    """Keyword search — always returns {"posts": [...], "comments": [...]}."""
    return _search_crawler.fetch(
        query,
        limit=limit,
        sort=sort,
        time_filter=time_filter,
        include_comments=include_comments,
        num_comment_crawl=num_comment_crawl,
        get_replies=get_replies,
        on_item=on_item,
        listing_only=listing_only,
    )


def enrich_posts_with_comments(
    posts: List[Dict],
    *,
    include_comments: bool = True,
    num_comment_crawl: Optional[int] = None,
    get_replies: Optional[bool] = None,
    on_item=None,
) -> CrawlResult:
    """Visit unique posts once: enrich + optional comments (multi-mode enrich pass)."""
    out = empty_crawl_result()
    out["posts"] = list(posts or [])
    if not out["posts"]:
        return out

    client = RedditPlaywrightClient()
    if not client.session_configured():
        raise SessionExpiredError(
            "reddit_playwright",
            "REDDIT_SESSION must be set in .env",
        )

    try:
        with sync_playwright() as playwright:
            browser: Browser = playwright.chromium.launch(headless=REDDIT_HEADLESS)
            context: BrowserContext = browser.new_context(
                viewport={"width": 1280, "height": 800},
                user_agent=DESKTOP_UA,
            )
            context.add_cookies(client.build_cookies())
            page = context.new_page()
            out["comments"] = RedditCommentsCrawler(client).visit_posts(
                page,
                out["posts"],
                include_comments=include_comments,
                num_comment_crawl=num_comment_crawl,
                get_replies=get_replies,
                on_item=on_item,
            )
            context.close()
            browser.close()
    except SessionExpiredError:
        raise
    except Exception as exc:
        logger.exception("reddit_playwright enrich failed: %s", exc)
        raise
    return out


def fetch_subreddit(
    subreddit: str,
    limit: Optional[int] = None,
    *,
    sort: Optional[str] = None,
    time_filter: Optional[str] = None,
    include_comments: bool = False,
    num_comment_crawl: Optional[int] = None,
    get_replies: Optional[bool] = None,
) -> CrawlResult:
    """Subreddit listing — always returns {"posts": [...], "comments": [...]}."""
    return _subreddit_crawler.fetch(
        subreddit,
        limit=limit,
        sort=sort,
        time_filter=time_filter,
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
            f"{i}. {row.get('author')} | r/{row.get('subreddit')} | "
            f"upvotes={engagement.get('upvotes')} "
            f"comments={engagement.get('comments')} "
            f"share={engagement.get('share')}"
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
        help="Also crawl comments for each collected post",
    )
    parser.add_argument(
        "--num-comment-crawl",
        type=int,
        default=None,
        help=f"Max top-level comments per post (cap {REDDIT_COMMENTS_HARD_CAP})",
    )
    parser.add_argument(
        "--get-replies",
        action="store_true",
        help="Also include nested replies (default: REDDIT_GET_REPLIES global)",
    )


def _add_filter_args(
    parser: argparse.ArgumentParser, *, sort_choices: List[str], default_sort: Optional[str] = None
) -> None:
    help_sort = (
        f"Sort order (default: {default_sort})"
        if default_sort
        else "Sort order (default: Reddit default / omit from URL)"
    )
    parser.add_argument(
        "--sort",
        default=default_sort,
        choices=sort_choices,
        help=help_sort,
    )
    parser.add_argument(
        "--time-filter",
        default=None,
        choices=sorted(ALLOWED_TIME_FILTERS),
        help="Time window &t= (for sort=top or sort=relevance; ignored otherwise with a warning)",
    )


def _cli_main(argv: Optional[List[str]] = None) -> int:
    raw_argv = list(argv if argv is not None else sys.argv[1:])
    if not raw_argv:
        raw_argv = ["search"]
    elif raw_argv[0] not in ("search", "subreddit"):
        raw_argv = ["search"] + raw_argv

    parser = argparse.ArgumentParser(
        description="Reddit Playwright crawler (search or subreddit)."
    )
    subparsers = parser.add_subparsers(dest="mode")

    search_parser = subparsers.add_parser("search", help="Crawl Reddit keyword search")
    search_parser.add_argument(
        "--query", default="news", help='Search query (default: "news")'
    )
    search_parser.add_argument("--limit", type=int, default=5, help="Max posts to fetch")
    _add_filter_args(
        search_parser,
        sort_choices=sorted(ALLOWED_SEARCH_SORTS),
        default_sort=DEFAULT_SEARCH_SORT,
    )
    _add_comment_args(search_parser)
    search_parser.add_argument("--json", action="store_true", help="Print full JSON output")

    sub_parser = subparsers.add_parser("subreddit", help="Crawl a subreddit listing")
    sub_parser.add_argument(
        "--subreddit",
        required=True,
        help="Subreddit name or r/name (e.g. digital_marketing)",
    )
    sub_parser.add_argument("--limit", type=int, default=5, help="Max posts to fetch")
    _add_filter_args(sub_parser, sort_choices=sorted(ALLOWED_SUBREDDIT_SORTS))
    _add_comment_args(sub_parser)
    sub_parser.add_argument("--json", action="store_true", help="Print full JSON output")

    args = parser.parse_args(raw_argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if not _default_client.session_configured():
        print(
            "SKIP: Set REDDIT_SESSION at the top of api/sources/reddit_playwright.py "
            "after logging into reddit.com in your browser."
        )
        return 2

    get_replies_arg = True if args.get_replies else None
    started = time.perf_counter()

    try:
        if args.mode == "subreddit":
            payload = fetch_subreddit(
                args.subreddit,
                limit=args.limit,
                sort=args.sort,
                time_filter=args.time_filter,
                include_comments=bool(args.include_comments),
                num_comment_crawl=args.num_comment_crawl,
                get_replies=get_replies_arg,
            )
            duration_ms = int((time.perf_counter() - started) * 1000)
            label = f"subreddit={args.subreddit!r}"
        else:
            payload = fetch(
                args.query,
                limit=args.limit,
                sort=args.sort,
                time_filter=args.time_filter,
                include_comments=bool(args.include_comments),
                num_comment_crawl=args.num_comment_crawl,
                get_replies=get_replies_arg,
            )
            duration_ms = int((time.perf_counter() - started) * 1000)
            label = f"search query={args.query!r}"
    except ValueError as exc:
        print(f"ERROR: {exc}")
        return 2

    if args.json:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    else:
        _print_payload(label, payload, duration_ms)

    return 0 if payload.get("posts") else 1


if __name__ == "__main__":
    raise SystemExit(_cli_main())
