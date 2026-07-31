from typing import List, Dict, Optional
import requests
from .utils import parse_rss, map_generic_rss_entry

def fetch(query: str, limit: Optional[int] = None) -> List[Dict]:
    url = f"https://www.reddit.com/search.rss?q={requests.utils.quote(query)}&sort=new"
    entries = parse_rss(url)
    rows = [
        map_generic_rss_entry(e, source_key="reddit_rss", post_prefix="reddit_rss")
        for e in entries
    ]
    return rows if limit is None else rows[:limit]
