import time
import re
import html as htmlmod
import base64
import logging
import random
from dataclasses import dataclass
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


# ---------------------------------------------------------------------------
# Generic query intent (no domain-specific synonym / topic hacks)
# ---------------------------------------------------------------------------

_QUERY_STOPWORDS = frozenset(
    {
        "a",
        "an",
        "and",
        "as",
        "at",
        "by",
        "for",
        "from",
        "in",
        "into",
        "is",
        "of",
        "on",
        "or",
        "the",
        "to",
        "with",
        "about",
        "how",
        "what",
        "why",
        "when",
        "where",
        "which",
        "who",
        "are",
        "was",
        "were",
        "be",
        "been",
        "being",
        "do",
        "does",
        "did",
        "can",
        "could",
        "should",
        "would",
        "will",
        "just",
        "than",
        "then",
        "that",
        "this",
        "these",
        "those",
        "it",
        "its",
        "my",
        "our",
        "your",
        "their",
        "right",
        "really",
        "very",
        "much",
        "more",
        "most",
        "some",
        "any",
        "all",
        "every",
        "also",
        "still",
        "even",
        "only",
        "too",
        "so",
        "if",
        "but",
        "not",
        "no",
        "yes",
        "please",
        "me",
        "us",
        "we",
        "you",
        "they",
        "he",
        "she",
        "his",
        "her",
        "them",
    }
)
# Soft intent words: useful for ranking hints, harmful as hard AND requirements.
_WEAK_QUERY_MODIFIERS = frozenset(
    {
        "trending",
        "trend",
        "trends",
        "latest",
        "news",
        "update",
        "updates",
        "current",
        "hot",
        "viral",
        "today",
        "now",
        "popular",
        "popularity",
        "insights",
        "insight",
        "overview",
        "analysis",
    }
)
_QUERY_TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_+#-]*")
# Over-fetch multiplier when adapters post-filter for relevance.
QUERY_OVERFETCH_FACTOR = 3


@dataclass(frozen=True)
class QueryIntent:
    """
    Structured view of a free-text crawl query.

    - phrases: consecutive content-word spans (len >= 2), e.g. "bubble tea"
    - standalone_tokens: single content words not inside a multi-word phrase
    - must_tokens: all content tokens (phrases flattened + standalones), no weak mods
    - optional_tokens: weak modifiers like trending/latest (never hard-required)
    """

    raw: str
    phrases: tuple  # Tuple[str, ...]
    standalone_tokens: tuple  # Tuple[str, ...]
    must_tokens: tuple  # Tuple[str, ...]
    optional_tokens: tuple  # Tuple[str, ...]


def parse_query_intent(query: str) -> QueryIntent:
    """
    Parse any free-text query into phrases + must/optional tokens.
    Domain-agnostic: no per-topic synonym tables.
    """
    raw = (query or "").strip()
    if not raw:
        return QueryIntent("", (), (), (), ())

    # Walk tokens in order; stopwords/weak words break phrase spans.
    words = _QUERY_TOKEN_RE.findall(raw.lower())
    phrases: List[str] = []
    standalones: List[str] = []
    optional: List[str] = []
    span: List[str] = []

    def _flush_span() -> None:
        nonlocal span
        if not span:
            return
        if len(span) == 2:
            phrases.append(" ".join(span))
        elif len(span) > 2:
            # Prefer a leading bigram phrase; keep remaining words as must tokens.
            # Avoid quoting very long spans that few engines match literally.
            phrases.append(" ".join(span[:2]))
            for tok in span[2:]:
                standalones.append(tok)
        else:
            standalones.append(span[0])
        span = []

    for w in words:
        if w in _QUERY_STOPWORDS:
            _flush_span()
            continue
        if w in _WEAK_QUERY_MODIFIERS:
            _flush_span()
            if w not in optional:
                optional.append(w)
            continue
        span.append(w)
    _flush_span()

    must: List[str] = []
    seen: set[str] = set()
    for phrase in phrases:
        for tok in phrase.split():
            if tok not in seen:
                seen.add(tok)
                must.append(tok)
    for tok in standalones:
        if tok not in seen:
            seen.add(tok)
            must.append(tok)

    return QueryIntent(
        raw=raw,
        phrases=tuple(phrases),
        standalone_tokens=tuple(standalones),
        must_tokens=tuple(must),
        optional_tokens=tuple(optional),
    )


def significant_query_tokens(query: str) -> List[str]:
    """Significant tokens including weak modifiers (lowercased, ordered, unique)."""
    intent = parse_query_intent(query)
    out: List[str] = []
    seen: set[str] = set()
    for tok in list(intent.must_tokens) + list(intent.optional_tokens):
        if tok not in seen:
            seen.add(tok)
            out.append(tok)
    return out


def must_match_tokens(query: str) -> List[str]:
    """Core topic tokens (excludes weak modifiers like 'trending')."""
    return list(parse_query_intent(query).must_tokens)


def _phrase_present(hay: str, phrase: str) -> bool:
    """True if the contiguous phrase appears, or every constituent word appears."""
    hay_l = (hay or "").lower()
    p = (phrase or "").strip().lower()
    if not p:
        return True
    if re.search(rf"\b{re.escape(p)}\b", hay_l):
        return True
    parts = p.split()
    return all(re.search(rf"\b{re.escape(tok)}\b", hay_l) for tok in parts)


def _token_present(hay: str, token: str) -> bool:
    hay_l = hay or ""
    return re.search(rf"\b{re.escape(token)}\b", hay_l, flags=re.IGNORECASE) is not None


def text_matches_intent(text: str, query: str) -> bool:
    """
    Soft relevance gate for any query:
    - each multi-word phrase must match (contiguous OR all words)
    - each standalone must-token must match
    - optional/weak modifiers are ignored
    """
    intent = parse_query_intent(query)
    if not intent.phrases and not intent.standalone_tokens and not intent.must_tokens:
        return True
    hay = text or ""
    for phrase in intent.phrases:
        if not _phrase_present(hay, phrase):
            return False
    for tok in intent.standalone_tokens:
        if not _token_present(hay, tok):
            return False
    # If we only have flattened must_tokens (no phrases/standalones split), still OK:
    # phrases/standalones cover the structured case; when both empty but must exists,
    # require all must tokens (defensive).
    if not intent.phrases and not intent.standalone_tokens:
        return all(_token_present(hay, tok) for tok in intent.must_tokens)
    return True


def text_matches_all_tokens(text: str, query: str) -> bool:
    """Every significant token (including weak modifiers) appears as a word."""
    tokens = significant_query_tokens(query)
    if not tokens:
        return True
    hay = text or ""
    return all(_token_present(hay, tok) for tok in tokens)


def text_matches_must_tokens(text: str, query: str) -> bool:
    """Backward-compatible alias for text_matches_intent."""
    return text_matches_intent(text, query)


def title_has_any_must_token(title: str, query: str) -> bool:
    """True when the title contains at least one core query token."""
    intent = parse_query_intent(query)
    if not intent.must_tokens:
        return True
    return any(_token_present(title or "", tok) for tok in intent.must_tokens)


def keyword_search_query(query: str) -> str:
    """
    Generic keyword/phrase query for X, Reddit, YouTube, etc.
    Quotes multi-word phrases; appends standalone must tokens.
    Example: 'Bubble Tea trending in Australia' -> '"bubble tea" australia'
    """
    intent = parse_query_intent(query)
    parts: List[str] = []
    for phrase in intent.phrases:
        parts.append(f'"{phrase}"')
    parts.extend(intent.standalone_tokens)
    if parts:
        return " ".join(parts)
    return (query or "").strip()


def web_search_query(query: str) -> str:
    """
    Broader query for Google News / DuckDuckGo.
    Combines a loose must-token string with optional quoted phrases (OR),
    so niche topics are less likely to return empty SERPs.
    """
    intent = parse_query_intent(query)
    if not intent.must_tokens:
        return (query or "").strip()
    loose = " ".join(intent.must_tokens)
    if not intent.phrases:
        return loose
    quoted = " OR ".join(f'"{p}"' for p in intent.phrases)
    return f"({loose}) OR ({quoted})"


def shaped_boolean_query(query: str) -> str:
    """
    Boolean query for NewsAPI / Guardian-style engines.
    Requires topic phrases/tokens; does NOT hard-require weak modifiers like 'trending'.
    Example: 'Bubble Tea trending in Australia'
      -> ("bubble tea" OR (bubble AND tea)) AND australia
    """
    intent = parse_query_intent(query)
    clauses: List[str] = []
    for phrase in intent.phrases:
        words = phrase.split()
        clauses.append(f'("{phrase}" OR ({" AND ".join(words)}))')
    for tok in intent.standalone_tokens:
        clauses.append(tok)
    if not clauses:
        # fall back to AND of must tokens, or raw
        if intent.must_tokens:
            return " AND ".join(intent.must_tokens)
        return (query or "").strip()
    if len(clauses) == 1:
        return clauses[0]
    return " AND ".join(f"({c})" if " OR " in c else c for c in clauses)


def google_news_q_from_query(query: str) -> str:
    """Google News / Google-syntax query."""
    return web_search_query(query)


def newsapi_q_from_query(query: str) -> str:
    """NewsAPI advanced-search query."""
    return shaped_boolean_query(query) or (query or "").strip()


def guardian_q_from_query(query: str) -> str:
    """Guardian Content API query (same shaping as other boolean engines)."""
    return shaped_boolean_query(query) or (query or "").strip()


def filter_rows_by_query(
    rows: List[Dict],
    query: str,
    *,
    limit: Optional[int] = None,
    soft: bool = True,
) -> List[Dict]:
    """Drop rows whose title+text miss required query intent; optionally cap length."""
    match = text_matches_intent if soft else text_matches_all_tokens
    out: List[Dict] = []
    for row in rows:
        hay = f"{row.get('title') or ''} {row.get('text') or ''}"
        if not match(hay, query):
            continue
        out.append(row)
        if limit is not None and len(out) >= limit:
            break
    return out


def overfetch_limit(limit: Optional[int], *, hard_cap: Optional[int] = None) -> Optional[int]:
    """Scale a user limit for over-fetch-then-filter; None stays None."""
    if limit is None:
        return hard_cap
    n = max(1, int(limit)) * QUERY_OVERFETCH_FACTOR
    if hard_cap is not None:
        return min(n, hard_cap)
    return n


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
