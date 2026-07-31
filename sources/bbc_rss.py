from typing import List, Dict, Optional
from .utils import parse_rss, map_generic_rss_entry, limit_reached

def fetch(query: str, limit: Optional[int] = None) -> List[Dict]:
    url = "https://feeds.bbci.co.uk/news/rss.xml"
    entries = parse_rss(url)
    q = (query or "").lower().strip()
    out: List[Dict] = []
    for e in entries:
        p = map_generic_rss_entry(e, source_key="bbc_rss", post_prefix="bbc_rss")
        if q:
            hay = f"{p['title']} {p['text']}".lower()
            if q not in hay:
                continue
        out.append(p)
        if limit_reached(len(out), limit):
            break
    return out
