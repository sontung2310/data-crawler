from typing import List, Dict, Optional
import requests
from .utils import (
    parse_rss,
    map_generic_rss_entry,
    keyword_search_query,
    filter_rows_by_query,
)

def _is_reddit_post_url(url: str) -> bool:
    """Keep post pages; drop bare subreddit / user / search landing URLs."""
    u = (url or "").lower()
    return "/comments/" in u


def fetch(query: str, limit: Optional[int] = None) -> List[Dict]:
    q = keyword_search_query(query) or (query or "").strip()
    url = (
        "https://www.reddit.com/search.rss"
        f"?q={requests.utils.quote(q)}"
        "&sort=relevance&type=link"
    )
    entries = parse_rss(url)
    rows = []
    for e in entries:
        row = map_generic_rss_entry(e, source_key="reddit_rss", post_prefix="reddit_rss")
        if not _is_reddit_post_url(row.get("url") or ""):
            continue
        rows.append(row)
    return filter_rows_by_query(rows, query, limit=limit, soft=True)
