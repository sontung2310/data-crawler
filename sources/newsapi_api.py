import math, time, logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
import requests

from config import NEWSAPI_KEY as CFG_NEWSAPI_KEY
from .utils import crawled_at_now, limit_reached

logger = logging.getLogger(__name__)

ENDPOINT = "https://newsapi.org/v2/everything"
MAX_PAGE_SIZE = 100
# Safety cap when limit=None so we do not page forever on a broad query
UNBOUNDED_MAX_RESULTS = 1000

def _to_ts(iso_str: Optional[str]) -> Optional[int]:
    if not iso_str:
        return None
    try:
        return int(datetime.fromisoformat(iso_str.replace("Z", "+00:00")).timestamp())
    except Exception:
        return None

def fetch(
    query: str,
    limit: Optional[int] = None,
    *,
    days: int = 7,
    language: str = "en",
    sort_by: str = "publishedAt"
) -> List[Dict]:
    """
    Adapter for REGISTRY: fetch(query, limit, **kwargs) -> list[dict].
    limit=None means crawl as many pages as possible (up to UNBOUNDED_MAX_RESULTS).
    """
    api_key = CFG_NEWSAPI_KEY
    if not api_key:
        logger.error("newsapi error: NEWSAPI_KEY not set in config")
        raise RuntimeError("NEWSAPI_KEY not set in config")

    to_dt = datetime.now(timezone.utc)
    from_dt = to_dt - timedelta(days=days)
    target = UNBOUNDED_MAX_RESULTS if limit is None else max(1, int(limit))
    page_size = min(MAX_PAGE_SIZE, target)
    pages = math.ceil(target / page_size)

    out: List[Dict] = []

    for page in range(1, pages + 1):
        params = {
            "q": query,
            "from": from_dt.isoformat().replace("+00:00", "Z"),
            "to": to_dt.isoformat().replace("+00:00", "Z"),
            "language": language,
            "sortBy": sort_by,
            "pageSize": page_size,
            "page": page,
            "apiKey": api_key,
        }
        try:
            r = requests.get(ENDPOINT, params=params, timeout=15)
            if r.status_code == 429:
                logger.warning("NewsAPI rate limited (429) – stopping early.")
                break
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logger.exception("NewsAPI request failed: %s", e)
            break

        articles = data.get("articles") or []
        if not articles:
            break

        for a in articles:
            url = (a.get("url") or "").strip()
            if not url:
                continue

            title = (a.get("title") or "").strip()
            desc = (a.get("description") or "").strip()
            content = (a.get("content") or "").strip()
            if " [+" in content:
                content = content.split(" [+", 1)[0].rstrip()

            combined_text = (desc + "\n\n" + content).strip() if (desc or content) else ""

            out.append({
                "post_id": f"newsapi:{hash(url)}",
                "source": "newsapi",
                "url": url,
                "title": title,
                "text": combined_text,
                "text_html": "",
                "published_ts": _to_ts(a.get("publishedAt")),
                "crawled_at": crawled_at_now(),
                "author": a.get("author") or None,
                "engagement": None,
            })

            if limit_reached(len(out), limit) or len(out) >= target:
                break

        if limit_reached(len(out), limit) or len(out) >= target:
            break

        time.sleep(0.2)

    logger.info("newsapi fetched %d articles for query='%s'", len(out), query)
    return out
