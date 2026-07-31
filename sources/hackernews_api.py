from typing import List, Dict, Optional
import requests
from .utils import UA, crawled_at_now, resolve_limit

def fetch(query: str, limit: Optional[int] = None) -> List[Dict]:
    page_size = resolve_limit(limit, hard_cap=50) or 50
    params = {
        "query": query or "",
        "tags": "story",
        "hitsPerPage": page_size,
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
    return out if limit is None else out[:limit]
