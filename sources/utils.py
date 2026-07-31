import time
import re
import html as htmlmod
import base64
import logging
import random
from typing import Dict, List, Optional
import requests
import feedparser

from config import (
    REDDIT_CLIENT_ID,
    REDDIT_CLIENT_SECRET,
    REDDIT_USER_AGENT,
)

logger = logging.getLogger(__name__)

UA = {"User-Agent": "Mozilla/5.0 (compatible; PaceDiscoveryBot/0.1; +http://localhost)"}

# --- helpers ---------------------------------------------------------------

def crawled_at_now() -> int:
    """Unix epoch seconds when a source row was produced by a crawler."""
    return int(time.time())


def limit_reached(count: int, limit: Optional[int]) -> bool:
    """True when an optional fetch limit has been satisfied. None = no limit."""
    return limit is not None and count >= limit


def resolve_limit(limit: Optional[int], *, hard_cap: Optional[int] = None) -> Optional[int]:
    """
    Normalize a fetch limit.
    - None means no app-side limit; if hard_cap is set, use that as "as many as the API allows".
    - Otherwise clamp to >= 1 and optionally to hard_cap.
    """
    if limit is None:
        return hard_cap
    n = int(limit)
    if n < 1:
        n = 1
    if hard_cap is not None:
        return min(n, hard_cap)
    return n


# --- time filters (aligned with Reddit: hour|day|week|month|year) ----------

ALLOWED_TIME_FILTERS = frozenset({"hour", "day", "week", "month", "year"})

# Approximate day windows for APIs that take integer days (YouTube, NewsAPI).
TIME_FILTER_TO_DAYS = {
    "hour": 1,
    "day": 1,
    "week": 7,
    "month": 30,
    "year": 365,
}

TIME_FILTER_TO_SECONDS = {
    "hour": 3600,
    "day": 86400,
    "week": 7 * 86400,
    "month": 30 * 86400,
    "year": 365 * 86400,
}


def normalize_time_filter(time_filter: Optional[str]) -> Optional[str]:
    """Return canonical time_filter or None. Raises ValueError if invalid."""
    if time_filter is None:
        return None
    value = str(time_filter).strip().lower()
    if not value:
        return None
    # Friendly aliases
    aliases = {"d": "day", "w": "week", "m": "month", "y": "year", "h": "hour"}
    value = aliases.get(value, value)
    if value not in ALLOWED_TIME_FILTERS:
        raise ValueError(
            f"invalid time_filter {time_filter!r}; "
            f"allowed: {', '.join(sorted(ALLOWED_TIME_FILTERS))}"
        )
    return value


def time_filter_to_days(time_filter: Optional[str]) -> Optional[int]:
    """Map time_filter → integer days for APIs (YouTube publishedAfter, NewsAPI)."""
    tf = normalize_time_filter(time_filter)
    if tf is None:
        return None
    return TIME_FILTER_TO_DAYS[tf]


def time_filter_cutoff_ts(time_filter: Optional[str], *, now: Optional[int] = None) -> Optional[int]:
    """Unix epoch lower bound for published_ts, or None if no filter."""
    tf = normalize_time_filter(time_filter)
    if tf is None:
        return None
    base = int(now if now is not None else time.time())
    return base - TIME_FILTER_TO_SECONDS[tf]


def time_filter_since_date(time_filter: Optional[str]) -> Optional[str]:
    """YYYY-MM-DD for X Advanced Search since: operator (until defaults to today)."""
    from datetime import date, timedelta

    tf = normalize_time_filter(time_filter)
    if tf is None:
        return None
    days = TIME_FILTER_TO_DAYS[tf]
    return (date.today() - timedelta(days=days)).isoformat()


def filter_posts_by_time_filter(
    rows: List[Dict],
    time_filter: Optional[str],
) -> List[Dict]:
    """
    Drop posts older than the window when published_ts is known.
    Rows without published_ts are kept (cannot judge age).
    """
    cutoff = time_filter_cutoff_ts(time_filter)
    if cutoff is None:
        return list(rows)
    out: List[Dict] = []
    for row in rows:
        ts = row.get("published_ts")
        if ts is None or ts == "":
            out.append(row)
            continue
        try:
            if int(ts) >= cutoff:
                out.append(row)
        except (TypeError, ValueError):
            out.append(row)
    return out


def time_filter_from_days(days: Optional[int]) -> Optional[str]:
    """Best-effort map of integer days (search API) → social-style time_filter."""
    if days is None:
        return None
    try:
        d = int(days)
    except (TypeError, ValueError):
        return None
    if d <= 0:
        return None
    if d <= 1:
        return "day"
    if d <= 7:
        return "week"
    if d <= 31:
        return "month"
    return "year"

def parse_rss(url: str, timeout: int = 10):
    """Fetch URL with requests (so HTTPS certs work) then parse with feedparser."""
    r = requests.get(url, headers=UA, timeout=timeout)
    r.raise_for_status()
    feed = feedparser.parse(r.content)
    return getattr(feed, "entries", []) or []

def epoch(dt) -> Optional[int]:
    """Convert feedparser's time.struct_time to epoch seconds."""
    if not dt:
        return None
    try:
        return int(time.mktime(dt))
    except Exception:
        return None

_TAG_RE = re.compile(r"<[^>]+>")

def clean_html_to_text(html: str) -> str:
    """Very simple HTML→plain-text: unescape entities, strip tags, collapse spaces."""
    if not html:
        return ""
    unescaped = htmlmod.unescape(html)
    no_tags = _TAG_RE.sub("", unescaped)
    return " ".join(no_tags.split())

def human_delay(min_s: float, max_s: float) -> None:
    """Sleep a random duration between min_s and max_s seconds."""
    if max_s < min_s:
        max_s = min_s
    time.sleep(random.uniform(min_s, max_s))

_ENGAGEMENT_COUNT_RE = re.compile(
    r"([\d,.]+)\s*([KMBkmb])?",
    re.IGNORECASE,
)

def parse_engagement_count(text: str) -> Optional[int]:
    """Parse engagement strings like '1.2K', '3M', '12,345' into integers."""
    if not text:
        return None
    cleaned = text.strip().replace(",", "")
    match = _ENGAGEMENT_COUNT_RE.search(cleaned)
    if not match:
        return None
    try:
        value = float(match.group(1))
    except ValueError:
        return None
    suffix = (match.group(2) or "").upper()
    multiplier = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}.get(suffix, 1)
    return int(value * multiplier)

def truncate_title(text: str, max_len: int = 120) -> str:
    """Return a short title snippet from plain text."""
    if not text:
        return ""
    one_line = " ".join(text.split())
    if len(one_line) <= max_len:
        return one_line
    return one_line[: max_len - 3].rstrip() + "..."

def is_website_url(url: str) -> bool:
    """
    Return True for article/blog/news/etc. URLs; False for media (images/videos/galleries).
    """
    if not url:
        return False

    media_extensions = {
        '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff', '.svg',
        '.mp4', '.mov', '.avi', '.webm', '.mkv', '.flv', '.wmv',
        '.mp3', '.wav', '.ogg', '.m4a', '.flac',
    }
    if any(url.lower().endswith(ext) for ext in media_extensions):
        return False

    media_domains = {
        'i.redd.it', 'v.redd.it', 'reddit.com/gallery/', 'imgur.com',
        'giphy.com', 'gfycat.com', 'streamable.com',
    }
    from urllib.parse import urlparse
    parsed = urlparse(url.lower())
    domain = parsed.netloc
    path = parsed.path

    for m in media_domains:
        if m in domain:
            return False

    if domain in ['reddit.com', 'www.reddit.com']:
        if '/r/' in path and '/comments/' in path:
            return True
        return False

    website_domains = {
        'reddit.com', 'www.reddit.com',
        'youtube.com', 'youtu.be', 'vimeo.com',
        'medium.com', 'substack.com',
        'github.com', 'gitlab.com',
        'twitter.com', 'x.com', 'linkedin.com',
        'nytimes.com', 'washingtonpost.com', 'theguardian.com',
        'bbc.com', 'reuters.com', 'apnews.com', 'bloomberg.com',
        'techcrunch.com', 'wired.com', 'theverge.com', 'arstechnica.com',
        'wordpress.com', 'blogspot.com', 'tumblr.com',
    }
    for w in website_domains:
        if w in domain:
            return True

    if '.' not in domain:
        return False

    common_tlds = {'.com', '.org', '.net', '.edu', '.gov', '.io', '.co'}
    if any(domain.endswith(tld) for tld in common_tlds):
        return True

    return False

# --- mappers ---------------------------------------------------------------

def map_generic_rss_entry(e, source_key: str, post_prefix: str) -> Dict:
    url = e.get("link") or ""
    title = e.get("title") or ""
    summary_html = (e.get("summary") or "").strip()
    text_plain = clean_html_to_text(summary_html)
    published_ts = epoch(e.get("published_parsed") or e.get("updated_parsed"))

    author = ""
    src = e.get("source")
    if isinstance(src, dict):
        author = src.get("title") or ""
    author = (e.get("author") or author or "").strip()

    import time as _t
    return {
        "post_id": f"{post_prefix}:{hash(url) if url else int(_t.time()*1000)}",
        "source": source_key,
        "url": url,
        "title": title,
        "text": text_plain,
        "text_html": summary_html,
        "published_ts": published_ts,
        "crawled_at": crawled_at_now(),
        "author": author,
        "engagement": {
            "score": None,
            "num_comments": None,
            "upvote_ratio": None,
        },
    }

def map_reddit_rss_entry(e) -> Dict:
    return map_generic_rss_entry(e, source_key="reddit_rss", post_prefix="reddit_rss")

# --- reddit oauth ----------------------------------------------------------

def get_reddit_access_token() -> Optional[str]:
    try:
        auth_str = f"{REDDIT_CLIENT_ID}:{REDDIT_CLIENT_SECRET}"
        encoded_auth = base64.b64encode(auth_str.encode()).decode()
        headers = {"User-Agent": REDDIT_USER_AGENT, "Authorization": f"Basic {encoded_auth}"}
        data = {"grant_type": "client_credentials"}
        r = requests.post("https://www.reddit.com/api/v1/access_token", headers=headers, data=data, timeout=10)
        r.raise_for_status()
        return r.json().get("access_token")
    except Exception as e:
        logger.error("Reddit token error: %s", e)
        return None
