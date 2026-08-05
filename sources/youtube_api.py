"""
YouTube Data API v3 crawler — keyword search and regional trending.

Requires YOUTUBE_API_KEY in credentials/backend.env (Google Cloud Console).

Both search and trending return a payload shaped for downstream persistence:

    {"videos": [...], "comments": [...]}

Comments are a flat list (Option C) — not nested on video rows — so callers can
write videos → raw_insights and comments → comments independently.

================================================================================
CLI
================================================================================

Search:
  python -m api.sources.youtube_api search [options]

  --query TEXT              Search keywords (required)
  --limit N                 Max videos, 1–50 (default: 5)
  --region-code / --location
                            ISO region code (e.g. AU, US). Also accepts AUS, USA, Australia
  --published-after-days N  Only videos newer than N days (default: 60; 0 = no filter)
  --order {viewCount,date,rating,relevance,title,videoCount}
                            Search order (default: relevance)
  --include-comments        Also crawl comments for each video
  --num-comment-crawl N     Max top-level comments per video (default: all up to cap 50)
  --get-replies             Also fetch replies under each top-level comment
                            (default: YOUTUBE_GET_REPLIES global, False = top-level only)
  --json                    Print full JSON instead of summary

Trending:
  python -m api.sources.youtube_api trending [options]

  --limit N                 Max videos, 1–50 (default: 5)
  --region-code / --location
                            ISO region code (e.g. AU, US)
  --video-category-id ID    Optional YouTube category (e.g. 28 = Science & Technology)
  --include-comments        Also crawl comments for each video
  --num-comment-crawl N     Max top-level comments per video (default: all up to cap 50)
  --get-replies             Also fetch replies under each top-level comment
  --json                    Print full JSON instead of summary

Examples:
  python -m api.sources.youtube_api search --query "niche marketing" --limit 10 \\
      --region-code AU --published-after-days 60 --order viewCount
  python -m api.sources.youtube_api trending --limit 5 --region-code US \\
      --include-comments --num-comment-crawl 20 --json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, TypedDict

import requests

from config import YOUTUBE_API_KEY
from .utils import (
    crawled_at_now,
    filter_rows_by_query,
    keyword_search_query,
    overfetch_limit,
    resolve_limit,
)

logger = logging.getLogger(__name__)

# --- optional overrides (None = unset; defaults applied inside fetch) --------
YOUTUBE_REGION_CODE: Optional[str] = None
YOUTUBE_SEARCH_ORDER: Optional[str] = None
YOUTUBE_PUBLISHED_AFTER_DAYS: Optional[int] = None

YOUTUBE_MAX_RESULTS_PER_PAGE = 50
YOUTUBE_TRENDING_AGE_EXPONENT = 0.6
YOUTUBE_DEFAULT_PUBLISHED_AFTER_DAYS = 60
YOUTUBE_DEFAULT_ORDER = "relevance"
# Hard safety cap: max top-level comment threads fetched per video.
YOUTUBE_COMMENTS_HARD_CAP = 50
# When False, only top-level comments are returned (no replies).
YOUTUBE_GET_REPLIES = False

SOURCE_KEY_SEARCH = "youtube_search"
SOURCE_KEY_TRENDING = "youtube_trending"
SOURCE_KEY_COMMENTS = "youtube"

API_BASE = "https://www.googleapis.com/youtube/v3"
ALLOWED_SEARCH_ORDERS = {
    "viewCount",
    "date",
    "rating",
    "relevance",
    "title",
    "videoCount",
}

# Common aliases → ISO 3166-1 alpha-2 region codes used by YouTube.
REGION_ALIASES: Dict[str, str] = {
    "AU": "AU",
    "AUS": "AU",
    "AUSTRALIA": "AU",
    "US": "US",
    "USA": "US",
    "UNITED STATES": "US",
    "UNITED STATES OF AMERICA": "US",
    "GB": "GB",
    "UK": "GB",
    "UNITED KINGDOM": "GB",
    "CA": "CA",
    "CANADA": "CA",
    "NZ": "NZ",
    "NEW ZEALAND": "NZ",
}

# YouTube video category IDs (US catalog; widely reused across regions).
VIDEO_CATEGORY_LABELS: Dict[str, str] = {
    "1": "Film & Animation",
    "2": "Autos & Vehicles",
    "10": "Music",
    "15": "Pets & Animals",
    "17": "Sports",
    "19": "Travel & Events",
    "20": "Gaming",
    "22": "People & Blogs",
    "23": "Comedy",
    "24": "Entertainment",
    "25": "News & Politics",
    "26": "Howto & Style",
    "27": "Education",
    "28": "Science & Technology",
    "29": "Nonprofits & Activism",
}


class CrawlResult(TypedDict):
    """Payload shaped so downstream can persist videos and comments separately."""

    videos: List[Dict]
    comments: List[Dict]


def empty_crawl_result() -> CrawlResult:
    return {"videos": [], "comments": []}


class YouTubeApiClient:
    """Shared YouTube Data API v3 client: HTTP, region helpers, batch enrichment."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = (api_key if api_key is not None else YOUTUBE_API_KEY) or ""

    def api_configured(self) -> bool:
        return bool(self.api_key.strip())

    def _request(self, endpoint: str, params: Dict[str, Any]) -> Optional[Dict]:
        if not self.api_configured():
            logger.warning("youtube_api: YOUTUBE_API_KEY not set")
            return None

        url = f"{API_BASE}/{endpoint.lstrip('/')}"
        query = {**params, "key": self.api_key.strip()}
        try:
            r = requests.get(url, params=query, timeout=20)
            if r.status_code in (403, 429):
                body = {}
                try:
                    body = r.json()
                except Exception:
                    pass
                reason = (
                    (body.get("error") or {}).get("errors") or [{}]
                )[0].get("reason") or r.text[:200]
                logger.warning(
                    "youtube_api: %s %s — %s",
                    r.status_code,
                    endpoint,
                    reason,
                )
                return None
            r.raise_for_status()
            return r.json()
        except requests.RequestException as exc:
            logger.exception("youtube_api request failed endpoint=%s: %s", endpoint, exc)
            return None

    @staticmethod
    def normalize_region_code(value: Optional[str]) -> Optional[str]:
        raw = (value or "").strip()
        if not raw:
            return None
        mapped = REGION_ALIASES.get(raw.upper())
        if mapped:
            return mapped
        if len(raw) == 2 and raw.isalpha():
            return raw.upper()
        logger.warning("youtube_api: unknown region_code=%r — ignoring", value)
        return None

    @staticmethod
    def parse_rfc3339_to_ts(iso: Optional[str]) -> Optional[int]:
        if not iso:
            return None
        try:
            normalized = iso.replace("Z", "+00:00")
            dt = datetime.fromisoformat(normalized)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            return None

    @staticmethod
    def resolve_published_after_days(
        explicit_days: Optional[int],
        global_days: Optional[int] = YOUTUBE_PUBLISHED_AFTER_DAYS,
    ) -> Optional[int]:
        if explicit_days is not None:
            if explicit_days <= 0:
                return None
            return int(explicit_days)
        if global_days is not None:
            if global_days <= 0:
                return None
            return int(global_days)
        return YOUTUBE_DEFAULT_PUBLISHED_AFTER_DAYS

    @staticmethod
    def days_to_published_after_rfc3339(days: Optional[int]) -> Optional[str]:
        if days is None or days <= 0:
            return None
        dt = datetime.now(timezone.utc) - timedelta(days=int(days))
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
    def resolve_order(
        explicit: Optional[str],
        global_order: Optional[str] = YOUTUBE_SEARCH_ORDER,
    ) -> str:
        candidate = (explicit or global_order or YOUTUBE_DEFAULT_ORDER).strip()
        if candidate not in ALLOWED_SEARCH_ORDERS:
            logger.warning(
                "youtube_api: unknown order=%r — falling back to %s",
                candidate,
                YOUTUBE_DEFAULT_ORDER,
            )
            return YOUTUBE_DEFAULT_ORDER
        return candidate

    @staticmethod
    def resolve_comment_limit(num_comment_crawl: Optional[int]) -> int:
        """None = all up to hard cap; N = min(N, hard cap)."""
        if num_comment_crawl is None:
            return YOUTUBE_COMMENTS_HARD_CAP
        try:
            n = int(num_comment_crawl)
        except (TypeError, ValueError):
            return YOUTUBE_COMMENTS_HARD_CAP
        if n <= 0:
            return 0
        return min(n, YOUTUBE_COMMENTS_HARD_CAP)

    @staticmethod
    def resolve_get_replies(explicit: Optional[bool] = None) -> bool:
        """Arg overrides global YOUTUBE_GET_REPLIES (default False)."""
        if explicit is not None:
            return bool(explicit)
        return bool(YOUTUBE_GET_REPLIES)

    def resolve_region(
        self,
        region_code: Optional[str] = None,
        location: Optional[str] = None,
    ) -> Optional[str]:
        raw = region_code if region_code is not None else location
        if raw is None or (isinstance(raw, str) and not raw.strip()):
            raw = YOUTUBE_REGION_CODE
        return self.normalize_region_code(raw)

    def videos_list(
        self,
        video_ids: List[str],
        *,
        parts: str = "snippet,statistics,topicDetails,contentDetails",
    ) -> List[Dict]:
        if not video_ids:
            return []
        out: List[Dict] = []
        for i in range(0, len(video_ids), YOUTUBE_MAX_RESULTS_PER_PAGE):
            batch = video_ids[i : i + YOUTUBE_MAX_RESULTS_PER_PAGE]
            data = self._request(
                "videos",
                {
                    "part": parts,
                    "id": ",".join(batch),
                    "maxResults": YOUTUBE_MAX_RESULTS_PER_PAGE,
                },
            )
            if not data:
                break
            out.extend(data.get("items") or [])
        return out

    def channels_list(self, channel_ids: List[str]) -> Dict[str, int]:
        unique = [c for c in dict.fromkeys(channel_ids) if c]
        if not unique:
            return {}
        result: Dict[str, int] = {}
        for i in range(0, len(unique), YOUTUBE_MAX_RESULTS_PER_PAGE):
            batch = unique[i : i + YOUTUBE_MAX_RESULTS_PER_PAGE]
            data = self._request(
                "channels",
                {
                    "part": "statistics",
                    "id": ",".join(batch),
                    "maxResults": YOUTUBE_MAX_RESULTS_PER_PAGE,
                },
            )
            if not data:
                break
            for item in data.get("items") or []:
                cid = item.get("id") or ""
                stats = item.get("statistics") or {}
                try:
                    result[cid] = int(stats.get("subscriberCount") or 0)
                except (TypeError, ValueError):
                    result[cid] = 0
        return result

    def search_video_ids(
        self,
        query: str,
        *,
        limit: int,
        order: str,
        region_code: Optional[str],
        published_after: Optional[str],
    ) -> List[str]:
        ids: List[str] = []
        page_token: Optional[str] = None
        remaining = max(1, min(int(limit), 50))

        while remaining > 0:
            page_size = min(remaining, YOUTUBE_MAX_RESULTS_PER_PAGE)
            params: Dict[str, Any] = {
                "part": "snippet",
                "type": "video",
                "q": query,
                "order": order,
                "maxResults": page_size,
            }
            if region_code:
                params["regionCode"] = region_code
            if published_after:
                params["publishedAfter"] = published_after
            if page_token:
                params["pageToken"] = page_token

            data = self._request("search", params)
            if not data:
                break

            for item in data.get("items") or []:
                vid = ((item.get("id") or {}).get("videoId") or "").strip()
                if vid and vid not in ids:
                    ids.append(vid)
                    remaining -= 1
                    if remaining <= 0:
                        break

            page_token = data.get("nextPageToken")
            if not page_token or remaining <= 0:
                break

        return ids[:limit]

    def trending_videos(
        self,
        *,
        limit: int,
        region_code: Optional[str],
        video_category_id: Optional[str] = None,
        chart: str = "mostPopular",
    ) -> List[Dict]:
        params: Dict[str, Any] = {
            "part": "snippet,statistics,topicDetails,contentDetails",
            "chart": chart or "mostPopular",
            "maxResults": max(1, min(int(limit), YOUTUBE_MAX_RESULTS_PER_PAGE)),
        }
        if region_code:
            params["regionCode"] = region_code
        if video_category_id:
            params["videoCategoryId"] = str(video_category_id).strip()

        data = self._request("videos", params)
        if not data:
            return []
        return (data.get("items") or [])[:limit]

    def comment_threads_list(
        self,
        video_id: str,
        *,
        max_top_level: int,
        order: str = "relevance",
    ) -> List[Dict]:
        if max_top_level <= 0 or not video_id:
            return []

        threads: List[Dict] = []
        page_token: Optional[str] = None

        while len(threads) < max_top_level:
            page_size = min(max_top_level - len(threads), YOUTUBE_MAX_RESULTS_PER_PAGE)
            params: Dict[str, Any] = {
                "part": "snippet,replies",
                "videoId": video_id,
                "order": order,
                "maxResults": page_size,
                "textFormat": "plainText",
            }
            if page_token:
                params["pageToken"] = page_token

            data = self._request("commentThreads", params)
            if not data:
                break

            items = data.get("items") or []
            if not items:
                break

            threads.extend(items)
            page_token = data.get("nextPageToken")
            if not page_token:
                break

        return threads[:max_top_level]

    def comments_list_replies(self, parent_id: str) -> List[Dict]:
        if not parent_id:
            return []
        replies: List[Dict] = []
        page_token: Optional[str] = None
        while True:
            params: Dict[str, Any] = {
                "part": "snippet",
                "parentId": parent_id,
                "maxResults": YOUTUBE_MAX_RESULTS_PER_PAGE,
                "textFormat": "plainText",
            }
            if page_token:
                params["pageToken"] = page_token
            data = self._request("comments", params)
            if not data:
                break
            items = data.get("items") or []
            if not items:
                break
            replies.extend(items)
            page_token = data.get("nextPageToken")
            if not page_token:
                break
        return replies


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def compute_engagement(
    *,
    views: int,
    likes: int,
    comments: int,
    published_ts: Optional[int],
    subscriber_count: Optional[int],
    now_ts: Optional[int] = None,
) -> Dict[str, Any]:
    now = now_ts if now_ts is not None else int(time.time())
    if published_ts and published_ts > 0:
        age_days = max((now - published_ts) / 86400.0, 1.0)
    else:
        age_days = 1.0

    views_safe = max(views, 0)
    likes_safe = max(likes, 0)
    comments_safe = max(comments, 0)
    subscribers = max(subscriber_count or 0, 1)

    views_per_day = views_safe / age_days
    like_rate = likes_safe / max(views_safe, 1)
    comment_rate = comments_safe / max(views_safe, 1)
    trending_score = views_safe / (age_days ** YOUTUBE_TRENDING_AGE_EXPONENT)
    breakout_score = views_per_day / subscribers

    return {
        "likes": likes_safe,
        "views": views_safe,
        "comments": comments_safe,
        "like_rate": round(like_rate, 6),
        "comment_rate": round(comment_rate, 6),
        "views_per_day": round(views_per_day, 2),
        "trending_score": round(trending_score, 2),
        "breakout_score": round(breakout_score, 6),
    }


def map_video_row(
    item: Dict,
    source_key: str,
    subscriber_count: Optional[int] = None,
) -> Optional[Dict]:
    video_id = (item.get("id") or "").strip()
    if not video_id:
        return None

    snippet = item.get("snippet") or {}
    statistics = item.get("statistics") or {}
    topic_details = item.get("topicDetails") or {}

    title = (snippet.get("title") or "").strip()
    description = snippet.get("description") or ""
    if not title and not description:
        return None

    published_ts = YouTubeApiClient.parse_rfc3339_to_ts(snippet.get("publishedAt"))
    views = _safe_int(statistics.get("viewCount"))
    likes = _safe_int(statistics.get("likeCount"))
    comments = _safe_int(statistics.get("commentCount"))
    category_id = (snippet.get("categoryId") or "").strip() or None
    tags = snippet.get("tags") or []
    if not isinstance(tags, list):
        tags = []

    topic_categories = topic_details.get("topicCategories") or []
    if not isinstance(topic_categories, list):
        topic_categories = []

    engagement = compute_engagement(
        views=views,
        likes=likes,
        comments=comments,
        published_ts=published_ts,
        subscriber_count=subscriber_count,
    )

    return {
        "post_id": f"{source_key}:{video_id}",
        "source": source_key,
        "url": f"https://www.youtube.com/watch?v={video_id}",
        "title": title,
        "text": description,
        "text_html": "",
        "published_ts": published_ts,
        "crawled_at": crawled_at_now(),
        "author": (snippet.get("channelTitle") or "").strip(),
        "channel_id": snippet.get("channelId") or None,
        "channel_subscriber_count": subscriber_count,
        "video_category_id": category_id,
        "video_category_label": (
            VIDEO_CATEGORY_LABELS.get(category_id) if category_id else None
        ),
        "tags": tags,
        "topic_categories": topic_categories,
        "engagement": engagement,
    }


def map_comment_row(
    comment_item: Dict,
    *,
    video_id: str,
    parent_comment_id: Optional[str] = None,
    reply_count: int = 0,
) -> Optional[Dict]:
    comment_id = (comment_item.get("id") or "").strip()
    snippet = comment_item.get("snippet") or {}
    if not comment_id:
        return None

    author_channel = snippet.get("authorChannelId")
    if isinstance(author_channel, dict):
        author_channel_id = author_channel.get("value") or None
    else:
        author_channel_id = author_channel or None

    text = snippet.get("textOriginal") or snippet.get("textDisplay") or ""
    vid = (snippet.get("videoId") or video_id or "").strip()
    parent_id = parent_comment_id
    if parent_id is None and snippet.get("parentId"):
        parent_id = snippet.get("parentId")

    return {
        "comment_id": comment_id,
        "source": SOURCE_KEY_COMMENTS,
        "parent_content_id": vid,
        "parent_content_url": f"https://www.youtube.com/watch?v={vid}" if vid else "",
        "author": (snippet.get("authorDisplayName") or "").strip(),
        "author_channel_id": author_channel_id,
        "text": text,
        "like_count": _safe_int(snippet.get("likeCount")),
        "reply_count": reply_count if parent_id is None else 0,
        "parent_comment_id": parent_id,
        "published_ts": YouTubeApiClient.parse_rfc3339_to_ts(snippet.get("publishedAt")),
        "crawled_at": crawled_at_now(),
    }


def enrich_and_map(
    client: YouTubeApiClient,
    video_items: List[Dict],
    source_key: str,
) -> List[Dict]:
    if not video_items:
        return []

    channel_ids = []
    for item in video_items:
        cid = ((item.get("snippet") or {}).get("channelId") or "").strip()
        if cid:
            channel_ids.append(cid)

    subscribers = client.channels_list(channel_ids)
    rows: List[Dict] = []
    for item in video_items:
        cid = ((item.get("snippet") or {}).get("channelId") or "").strip()
        mapped = map_video_row(
            item,
            source_key,
            subscriber_count=subscribers.get(cid),
        )
        if mapped:
            rows.append(mapped)

    rows.sort(
        key=lambda r: ((r.get("engagement") or {}).get("trending_score") or 0),
        reverse=True,
    )
    return rows


def video_id_from_row(row: Dict) -> Optional[str]:
    post_id = (row.get("post_id") or "").strip()
    if ":" in post_id:
        return post_id.rsplit(":", 1)[-1] or None
    url = row.get("url") or ""
    if "v=" in url:
        return url.split("v=", 1)[-1].split("&", 1)[0] or None
    return None


class YouTubeCommentsCrawler:
    """Crawl comments for videos via commentThreads.list (+ replies)."""

    def __init__(self, client: Optional[YouTubeApiClient] = None):
        self.client = client or YouTubeApiClient()

    def fetch_for_video(
        self,
        video_id: str,
        *,
        num_comment_crawl: Optional[int] = None,
        get_replies: Optional[bool] = None,
    ) -> List[Dict]:
        video_id = (video_id or "").strip()
        if not video_id:
            return []

        limit = self.client.resolve_comment_limit(num_comment_crawl)
        if limit <= 0:
            return []

        include_replies = self.client.resolve_get_replies(get_replies)

        threads = self.client.comment_threads_list(
            video_id,
            max_top_level=limit,
            order="relevance",
        )
        out: List[Dict] = []
        seen: set = set()

        for thread in threads:
            snippet = thread.get("snippet") or {}
            top = snippet.get("topLevelComment") or {}
            total_replies = _safe_int(snippet.get("totalReplyCount"))
            top_row = map_comment_row(
                top,
                video_id=video_id,
                parent_comment_id=None,
                reply_count=total_replies,
            )
            if top_row and top_row["comment_id"] not in seen:
                seen.add(top_row["comment_id"])
                out.append(top_row)

            if not include_replies:
                continue

            parent_id = (top.get("id") or "").strip()
            embedded = ((thread.get("replies") or {}).get("comments")) or []
            reply_items = list(embedded)

            if parent_id and total_replies > len(embedded):
                reply_items = self.client.comments_list_replies(parent_id)

            for reply in reply_items:
                reply_row = map_comment_row(
                    reply,
                    video_id=video_id,
                    parent_comment_id=parent_id or None,
                    reply_count=0,
                )
                if reply_row and reply_row["comment_id"] not in seen:
                    seen.add(reply_row["comment_id"])
                    out.append(reply_row)

        return out

    def fetch_for_videos(
        self,
        videos: List[Dict],
        *,
        num_comment_crawl: Optional[int] = None,
        get_replies: Optional[bool] = None,
    ) -> List[Dict]:
        all_comments: List[Dict] = []
        for row in videos:
            vid = video_id_from_row(row)
            if not vid:
                continue
            try:
                all_comments.extend(
                    self.fetch_for_video(
                        vid,
                        num_comment_crawl=num_comment_crawl,
                        get_replies=get_replies,
                    )
                )
            except Exception as exc:
                logger.warning(
                    "youtube_api comments failed for video_id=%s: %s", vid, exc
                )
        return all_comments


class YouTubeSearchCrawler:
    """Crawl YouTube via search.list, then enrich with videos.list + channels.list."""

    def __init__(
        self,
        client: Optional[YouTubeApiClient] = None,
        comments_crawler: Optional[YouTubeCommentsCrawler] = None,
    ):
        self.client = client or YouTubeApiClient()
        self.comments_crawler = comments_crawler or YouTubeCommentsCrawler(self.client)

    def fetch(
        self,
        query: str,
        limit: Optional[int] = None,
        *,
        region_code: Optional[str] = None,
        published_after_days: Optional[int] = None,
        order: Optional[str] = None,
        location: Optional[str] = None,
        include_comments: bool = False,
        num_comment_crawl: Optional[int] = None,
        get_replies: Optional[bool] = None,
        on_item=None,
    ) -> CrawlResult:
        query = (query or "").strip()
        if not query:
            logger.warning("youtube_api search: empty query")
            return empty_crawl_result()

        if not self.client.api_configured():
            raise RuntimeError("YOUTUBE_API_KEY must be set in .env")

        want = resolve_limit(limit, hard_cap=50) or 50
        fetch_n = overfetch_limit(want, hard_cap=50) or want
        search_q = keyword_search_query(query) or query
        resolved_order = self.client.resolve_order(order)
        resolved_region = self.client.resolve_region(region_code, location)
        days = self.client.resolve_published_after_days(published_after_days)
        published_after = self.client.days_to_published_after_rfc3339(days)

        video_ids = self.client.search_video_ids(
            search_q,
            limit=fetch_n,
            order=resolved_order,
            region_code=resolved_region,
            published_after=published_after,
        )
        if not video_ids:
            logger.info(
                "youtube_api search returned 0 ids for query=%r shaped=%r region=%s",
                query,
                search_q,
                resolved_region,
            )
            return empty_crawl_result()

        video_items = self.client.videos_list(video_ids)
        by_id = {item.get("id"): item for item in video_items if item.get("id")}
        ordered_items = [by_id[vid] for vid in video_ids if vid in by_id]

        videos = enrich_and_map(self.client, ordered_items, SOURCE_KEY_SEARCH)
        videos = filter_rows_by_query(videos, query, limit=want, soft=True)
        if on_item:
            for video in videos:
                on_item("post", video)

        comments: List[Dict] = []
        if include_comments and videos:
            comments = self.comments_crawler.fetch_for_videos(
                videos,
                num_comment_crawl=num_comment_crawl,
                get_replies=get_replies,
            )
            if on_item:
                for row in comments:
                    on_item("comment", row)

        logger.info(
            "youtube_api search fetched %d videos / %d comments for query=%r "
            "shaped=%r order=%s region=%s days=%s include_comments=%s get_replies=%s",
            len(videos),
            len(comments),
            query,
            search_q,
            resolved_order,
            resolved_region,
            days,
            include_comments,
            self.client.resolve_get_replies(get_replies),
        )
        return {"videos": videos, "comments": comments}


class YouTubeTrendingCrawler:
    """Crawl YouTube mostPopular chart for a region (no keyword)."""

    def __init__(
        self,
        client: Optional[YouTubeApiClient] = None,
        comments_crawler: Optional[YouTubeCommentsCrawler] = None,
    ):
        self.client = client or YouTubeApiClient()
        self.comments_crawler = comments_crawler or YouTubeCommentsCrawler(self.client)

    def fetch(
        self,
        limit: Optional[int] = None,
        *,
        region_code: Optional[str] = None,
        video_category_id: Optional[str] = None,
        chart: str = "mostPopular",
        location: Optional[str] = None,
        include_comments: bool = False,
        num_comment_crawl: Optional[int] = None,
        get_replies: Optional[bool] = None,
    ) -> CrawlResult:
        if not self.client.api_configured():
            logger.warning(
                "youtube_api: YOUTUBE_API_KEY must be set in credentials/backend.env"
            )
            return empty_crawl_result()

        limit = resolve_limit(limit, hard_cap=50) or 50
        resolved_region = self.client.resolve_region(region_code, location)

        video_items = self.client.trending_videos(
            limit=limit,
            region_code=resolved_region,
            video_category_id=video_category_id,
            chart=chart,
        )
        videos = enrich_and_map(self.client, video_items, SOURCE_KEY_TRENDING)[:limit]
        comments: List[Dict] = []
        if include_comments and videos:
            comments = self.comments_crawler.fetch_for_videos(
                videos,
                num_comment_crawl=num_comment_crawl,
                get_replies=get_replies,
            )

        logger.info(
            "youtube_api trending fetched %d videos / %d comments region=%s "
            "category=%s include_comments=%s get_replies=%s",
            len(videos),
            len(comments),
            resolved_region,
            video_category_id,
            include_comments,
            self.client.resolve_get_replies(get_replies),
        )
        return {"videos": videos, "comments": comments}


_default_client = YouTubeApiClient()
_comments_crawler = YouTubeCommentsCrawler(_default_client)
_search_crawler = YouTubeSearchCrawler(_default_client, _comments_crawler)
_trending_crawler = YouTubeTrendingCrawler(_default_client, _comments_crawler)


def fetch(
    query: str,
    limit: Optional[int] = None,
    *,
    region_code: Optional[str] = None,
    published_after_days: Optional[int] = None,
    order: Optional[str] = None,
    location: Optional[str] = None,
    include_comments: bool = False,
    num_comment_crawl: Optional[int] = None,
    get_replies: Optional[bool] = None,
    on_item=None,
) -> CrawlResult:
    """Search YouTube. Returns {"videos": [...], "comments": [...]}."""
    return _search_crawler.fetch(
        query,
        limit=limit,
        region_code=region_code,
        published_after_days=published_after_days,
        order=order,
        location=location,
        include_comments=include_comments,
        num_comment_crawl=num_comment_crawl,
        get_replies=get_replies,
        on_item=on_item,
    )


def fetch_trending(
    limit: Optional[int] = None,
    *,
    region_code: Optional[str] = None,
    video_category_id: Optional[str] = None,
    chart: str = "mostPopular",
    location: Optional[str] = None,
    include_comments: bool = False,
    num_comment_crawl: Optional[int] = None,
    get_replies: Optional[bool] = None,
) -> CrawlResult:
    """Fetch regional mostPopular videos. Returns {"videos": [...], "comments": [...]}."""
    return _trending_crawler.fetch(
        limit=limit,
        region_code=region_code,
        video_category_id=video_category_id,
        chart=chart,
        location=location,
        include_comments=include_comments,
        num_comment_crawl=num_comment_crawl,
        get_replies=get_replies,
    )


def _print_result(label: str, result: CrawlResult, duration_ms: int) -> None:
    videos = result.get("videos") or []
    comments = result.get("comments") or []
    print(
        f"{label} videos={len(videos)} comments={len(comments)} duration_ms={duration_ms}"
    )
    for i, row in enumerate(videos, start=1):
        engagement = row.get("engagement") or {}
        print(
            f"{i}. {row.get('author')} | views={engagement.get('views')} "
            f"likes={engagement.get('likes')} comments={engagement.get('comments')} "
            f"trending={engagement.get('trending_score')} "
            f"breakout={engagement.get('breakout_score')}"
        )
        print(f"   title: {row.get('title')}")
        print(f"   url:   {row.get('url')}")
        print(
            f"   category: {row.get('video_category_label')} "
            f"| subs={row.get('channel_subscriber_count')}"
        )

    if comments:
        print(f"--- comments ({len(comments)}) ---")
        for i, c in enumerate(comments[:15], start=1):
            parent = c.get("parent_comment_id")
            kind = "reply" if parent else "top"
            print(
                f"{i}. [{kind}] {c.get('author')} | likes={c.get('like_count')} "
                f"replies={c.get('reply_count')} video={c.get('parent_content_id')}"
            )
            text = (c.get("text") or "").replace("\n", " ")
            if len(text) > 100:
                text = text[:97] + "..."
            print(f"   {text}")
        if len(comments) > 15:
            print(f"   … {len(comments) - 15} more comments omitted from summary")


def _add_region_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--region-code",
        default=None,
        help="ISO region code (AU, US). Aliases: AUS, USA, Australia",
    )
    parser.add_argument(
        "--location",
        default=None,
        help="Alias for --region-code",
    )


def _add_comment_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--include-comments",
        action="store_true",
        help="Also crawl comments for each returned video",
    )
    parser.add_argument(
        "--num-comment-crawl",
        type=int,
        default=None,
        help=(
            "Max top-level comments per video by relevance "
            f"(default: all up to hard cap {YOUTUBE_COMMENTS_HARD_CAP})"
        ),
    )
    parser.add_argument(
        "--get-replies",
        action="store_true",
        default=None,
        help=(
            "Also fetch replies under each top-level comment "
            f"(default: YOUTUBE_GET_REPLIES={YOUTUBE_GET_REPLIES})"
        ),
    )


def _cli_main(argv: Optional[List[str]] = None) -> int:
    raw_argv = list(argv if argv is not None else sys.argv[1:])
    if not raw_argv:
        raw_argv = ["search"]
    elif raw_argv[0] not in ("search", "trending"):
        raw_argv = ["search"] + raw_argv

    parser = argparse.ArgumentParser(
        description="YouTube Data API crawler (search or trending)."
    )
    subparsers = parser.add_subparsers(dest="mode")

    search_parser = subparsers.add_parser("search", help="Keyword search videos")
    search_parser.add_argument("--query", default="news", help='Search query (default: "news")')
    search_parser.add_argument("--limit", type=int, default=5, help="Max videos to fetch")
    _add_region_args(search_parser)
    search_parser.add_argument(
        "--published-after-days",
        type=int,
        default=None,
        help="Only videos newer than N days (default: 60; 0 = no filter)",
    )
    search_parser.add_argument(
        "--order",
        default=None,
        choices=sorted(ALLOWED_SEARCH_ORDERS),
        help=f"Search order (default: {YOUTUBE_DEFAULT_ORDER})",
    )
    _add_comment_args(search_parser)
    search_parser.add_argument("--json", action="store_true", help="Print full JSON output")

    trending_parser = subparsers.add_parser(
        "trending", help="Regional mostPopular chart"
    )
    trending_parser.add_argument("--limit", type=int, default=5, help="Max videos to fetch")
    _add_region_args(trending_parser)
    trending_parser.add_argument(
        "--video-category-id",
        default=None,
        help="Optional YouTube category ID (e.g. 28 = Science & Technology)",
    )
    _add_comment_args(trending_parser)
    trending_parser.add_argument("--json", action="store_true", help="Print full JSON output")

    args = parser.parse_args(raw_argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if not _default_client.api_configured():
        print(
            "SKIP: Set YOUTUBE_API_KEY in credentials/backend.env "
            "(Google Cloud Console → YouTube Data API v3)."
        )
        return 2

    started = time.perf_counter()

    if args.mode == "trending":
        result = fetch_trending(
            limit=args.limit,
            region_code=args.region_code,
            location=args.location,
            video_category_id=args.video_category_id,
            include_comments=args.include_comments,
            num_comment_crawl=args.num_comment_crawl,
            get_replies=args.get_replies,
        )
        duration_ms = int((time.perf_counter() - started) * 1000)
        if args.json:
            print(json.dumps(result, indent=2, ensure_ascii=False))
        else:
            _print_result("trending", result, duration_ms)
    else:
        result = fetch(
            args.query,
            limit=args.limit,
            region_code=args.region_code,
            location=args.location,
            published_after_days=args.published_after_days,
            order=args.order,
            include_comments=args.include_comments,
            num_comment_crawl=args.num_comment_crawl,
            get_replies=args.get_replies,
        )
        duration_ms = int((time.perf_counter() - started) * 1000)
        if args.json:
            print(json.dumps(result, indent=2, ensure_ascii=False))
        else:
            _print_result(f"search query={args.query!r}", result, duration_ms)

    videos = result.get("videos") or []
    return 0 if videos else 1


if __name__ == "__main__":
    raise SystemExit(_cli_main())
