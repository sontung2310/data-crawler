from typing import List, Dict, Optional
import requests
from .utils import (
    parse_rss,
    map_generic_rss_entry,
    google_news_q_from_query,
    filter_rows_by_query,
)

def fetch(query: str, limit: Optional[int] = None) -> List[Dict]:
    q = google_news_q_from_query(query) or (query or "").strip()
    # Over-fetch RSS entries then soft-filter to query core tokens.
    fetch_n = None if limit is None else max(int(limit) * 3, int(limit))
    url = (
        "https://news.google.com/rss/search"
        f"?q={requests.utils.quote(q)}"
        "&hl=en-AU&gl=AU&ceid=AU:en"
    )
    entries = parse_rss(url)
    rows = [
        map_generic_rss_entry(e, source_key="news_rss", post_prefix="gnews_rss")
        for e in entries
    ]
    if fetch_n is not None:
        rows = rows[:fetch_n]
    return filter_rows_by_query(rows, query, limit=limit, soft=True)
