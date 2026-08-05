from typing import List, Dict, Optional
import requests
from .utils import (
    UA,
    crawled_at_now,
    resolve_limit,
    keyword_search_query,
    overfetch_limit,
    filter_rows_by_query,
)

def fetch(query: str, limit: Optional[int] = None) -> List[Dict]:
    want = resolve_limit(limit, hard_cap=50) or 50
    fetch_n = overfetch_limit(want, hard_cap=50) or want
    q = keyword_search_query(query) or (query or "")
    params = {
        "query": q,
        "tags": "story",
        "hitsPerPage": fetch_n,
        "restrictSearchableAttributes": "title,url",
    }
    try:
        r = requests.get("https://hn.algolia.com/api/v1/search", params=params, headers=UA, timeout=10)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return []

    hits = data.get("hits", []) or []
    out: List[Dict] = []
    for h in hits:
        url = h.get("url") or f"https://news.ycombinator.com/item?id={h.get('objectID')}"
        title = h.get("title") or ""
        published_ts = h.get("created_at_i") if isinstance(h.get("created_at_i"), int) else None
        author = h.get("author") or ""

        out.append({
            "post_id": f"hackernews_api:{h.get('objectID','')}",
            "source": "hackernews_api",
            "url": url,
            "title": title,
            "text": "",
            "text_html": "",
            "published_ts": published_ts,
            "crawled_at": crawled_at_now(),
            "author": author,
            "engagement": {"points": h.get("points"), "num_comments": h.get("num_comments")},
        })
    return filter_rows_by_query(out, query, limit=limit if limit is not None else want, soft=True)
