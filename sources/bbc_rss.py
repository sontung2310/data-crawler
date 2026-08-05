from typing import List, Dict, Optional
from .utils import parse_rss, map_generic_rss_entry, limit_reached, text_matches_intent

def fetch(query: str, limit: Optional[int] = None) -> List[Dict]:
    url = "https://feeds.bbci.co.uk/news/rss.xml"
    entries = parse_rss(url)
    out: List[Dict] = []
    for e in entries:
        p = map_generic_rss_entry(e, source_key="bbc_rss", post_prefix="bbc_rss")
        hay = f"{p['title']} {p['text']}"
        if not text_matches_intent(hay, query):
            continue
        out.append(p)
        if limit_reached(len(out), limit):
            break
    return out
