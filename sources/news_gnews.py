from typing import List, Dict, Optional
import requests
from .utils import parse_rss, map_generic_rss_entry

def fetch(query: str, limit: Optional[int] = None) -> List[Dict]:
    url = (
        "https://news.google.com/rss/search"
        f"?q={requests.utils.quote(query)}"
        "&hl=en-AU&gl=AU&ceid=AU:en"
    )
    entries = parse_rss(url)
    rows = [
        map_generic_rss_entry(e, source_key="news_rss", post_prefix="gnews_rss")
        for e in entries
    ]
    return rows if limit is None else rows[:limit]
