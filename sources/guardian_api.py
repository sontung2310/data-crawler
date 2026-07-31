from typing import List, Dict, Optional
from datetime import datetime, timezone as dt_tz
import requests

from config import GUARDIAN_API_KEY
from .utils import UA, clean_html_to_text, crawled_at_now, resolve_limit

def fetch(query: str, limit: Optional[int] = None) -> List[Dict]:
    api_key = GUARDIAN_API_KEY or ""
    if not api_key:
        raise RuntimeError("GUARDIAN_API_KEY must be set in .env")

    page_size = resolve_limit(limit, hard_cap=50) or 50
    params = {
        "api-key": api_key,
        "q": query or "",
        "page-size": page_size,
        "show-fields": "trailText,headline",
    }
    try:
        r = requests.get("https://content.guardianapis.com/search", params=params, headers=UA, timeout=10)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return []

    results = data.get("response", {}).get("results", []) or []
    out: List[Dict] = []
    for it in results:
        url = it.get("webUrl") or ""
        title = (it.get("fields", {}) or {}).get("headline") or it.get("webTitle") or ""
        text_html = (it.get("fields", {}) or {}).get("trailText") or ""
        text_plain = clean_html_to_text(text_html)

        published_ts = None
        iso = it.get("webPublicationDate")
        if iso:
            try:
                if iso.endswith("Z"):
                    iso = iso[:-1]
                dt = datetime.fromisoformat(iso).replace(tzinfo=dt_tz.utc)
                published_ts = int(dt.timestamp())
            except Exception:
                published_ts = None

        out.append({
            "post_id": f"guardian_api:{it.get('id', '') or hash(url)}",
            "source": "guardian_api",
            "url": url,
            "title": title,
            "text": text_plain,
            "text_html": text_html,
            "published_ts": published_ts,
            "crawled_at": crawled_at_now(),
            "author": it.get("pillarName") or "",
            "engagement": None,
        })
    return out if limit is None else out[:limit]
