"""Unified crawl REGISTRY — every adapter, tagged by category."""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from . import (
    bbc_rss,
    duckduckgo_web,
    guardian_api,
    hackernews_api,
    news_gnews,
    newsapi_api,
    reddit_official,
    reddit_playwright,
    reddit_rss,
    techcrunch_rss,
    x_playwright,
    youtube_api,
)

FetchFn = Callable[..., Any]

# Each entry: fetch callable, API category, whether comments are supported.
REGISTRY: Dict[str, Dict[str, Any]] = {
    "x_playwright": {
        "fetch": x_playwright.fetch,
        "category": "x",
        "supports_comments": True,
    },
    "reddit_playwright": {
        "fetch": reddit_playwright.fetch,
        "category": "reddit",
        "supports_comments": True,
    },
    "youtube_api": {
        "fetch": youtube_api.fetch,
        "category": "youtube",
        "supports_comments": True,
    },
    "duckduckgo_web": {
        "fetch": duckduckgo_web.fetch,
        "category": "duckduckgo",
        "supports_comments": False,
    },
    "google_news": {
        "fetch": news_gnews.fetch,
        "category": "news",
        "supports_comments": False,
    },
    "bbc": {
        "fetch": bbc_rss.fetch,
        "category": "news",
        "supports_comments": False,
    },
    "techcrunch": {
        "fetch": techcrunch_rss.fetch,
        "category": "news",
        "supports_comments": False,
    },
    "guardian": {
        "fetch": guardian_api.fetch,
        "category": "news",
        "supports_comments": False,
    },
    "hackernews": {
        "fetch": hackernews_api.fetch,
        "category": "news",
        "supports_comments": False,
    },
    "newsapi": {
        "fetch": newsapi_api.fetch,
        "category": "news",
        "supports_comments": False,
    },
    "reddit_official": {
        "fetch": reddit_official.fetch,
        "category": "news",
        "supports_comments": False,
    },
    "reddit_rss": {
        "fetch": reddit_rss.fetch,
        "category": "news",
        "supports_comments": False,
    },
}

CATEGORIES = ("x", "reddit", "youtube", "duckduckgo", "news", "all")


def resolve_adapters(source: Optional[str]) -> List[str]:
    """Map API source category → registry keys. None/all → every adapter."""
    if source is None or str(source).strip() == "" or str(source).strip().lower() == "all":
        return list(REGISTRY.keys())
    cat = str(source).strip().lower()
    if cat not in CATEGORIES:
        raise ValueError(
            f"unknown source {source!r}; allowed: {', '.join(CATEGORIES)}"
        )
    if cat == "all":
        return list(REGISTRY.keys())
    return [k for k, meta in REGISTRY.items() if meta["category"] == cat]


def list_sources() -> Dict[str, Any]:
    return {
        "categories": {
            cat: [k for k, m in REGISTRY.items() if m["category"] == cat]
            for cat in ("x", "reddit", "youtube", "duckduckgo", "news")
        },
        "adapters": {
            k: {"category": m["category"], "supports_comments": m["supports_comments"]}
            for k, m in REGISTRY.items()
        },
    }
