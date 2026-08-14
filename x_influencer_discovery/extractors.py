from __future__ import annotations

import html as html_lib
import re
from urllib.parse import urlparse

from .models import CountValue, RecentActivity, XProfile

NOISE_HANDLES = {
    "intent", "share", "home", "search", "i", "hashtag", "explore", "settings",
    "privacy", "tos", "download", "login", "signup", "notifications", "messages",
    "compose", "x", "twitter", "premium", "jobs", "communities", "verified_orgs",
    "gmail", "youtube", "linkedin", "facebook", "instagram", "tiktok", "medium",
    "nasa", "realdonaldtrump", "barackobama", "cristiano", "justinbieber",
    "katyperry", "ladygaga", "narendramodi", "elonmusk",
}


def normalize_handle(value: str | None) -> str | None:
    if not value:
        return None
    value = html_lib.unescape(value.strip())
    if value.startswith("@"):
        value = value[1:]
    if "x.com/" in value or "twitter.com/" in value:
        parsed = urlparse(value if value.startswith("http") else "https://" + value)
        parts = [p for p in parsed.path.split("/") if p]
        if not parts:
            return None
        value = parts[0]
    value = value.strip().strip("/")
    if not re.fullmatch(r"[A-Za-z0-9_]{1,15}", value):
        return None
    if value.lower() in NOISE_HANDLES:
        return None
    return value


def estimate_count(raw: str | None) -> int | None:
    if not raw:
        return None
    cleaned = html_lib.unescape(raw).replace(",", "").strip()
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)([KMB]?)", cleaned, re.I)
    if not match:
        return None
    value = float(match.group(1))
    suffix = match.group(2).upper()
    multiplier = {"": 1, "K": 1_000, "M": 1_000_000, "B": 1_000_000_000}[suffix]
    return int(value * multiplier)


def _strip_tags(value: str) -> str:
    value = re.sub(r"<script.*?</script>|<style.*?</style>", " ", value, flags=re.S | re.I)
    value = re.sub(r"<[^>]+>", " ", value)
    return re.sub(r"\s+", " ", html_lib.unescape(value)).strip()


def _meta(html: str, property_name: str) -> str | None:
    pattern = rf'<meta\s+(?:property|name)=["\']{re.escape(property_name)}["\']\s+content=["\'](.*?)["\']'
    match = re.search(pattern, html, re.S | re.I)
    if match:
        return html_lib.unescape(match.group(1)).strip()
    pattern2 = rf'<meta\s+content=["\'](.*?)["\']\s+(?:property|name)=["\']{re.escape(property_name)}["\']'
    match = re.search(pattern2, html, re.S | re.I)
    return html_lib.unescape(match.group(1)).strip() if match else None


def _title(html: str) -> str | None:
    match = re.search(r"<title>(.*?)</title>", html, re.S | re.I)
    return _strip_tags(match.group(1)) if match else None


def _name_from_title(title: str | None, handle: str) -> str | None:
    if not title:
        return None
    if "(@" in title:
        return title.split("(@", 1)[0].strip()
    title = title.replace("/ X", "").replace("/ Twitter", "").strip()
    return title or handle


def _count_before_label(html: str, label: str) -> str | None:
    pattern = rf'<div[^>]*font-bold[^>]*>([^<]+)</div>\s*<div[^>]*>{re.escape(label)}</div>'
    match = re.search(pattern, html, re.S | re.I)
    if match:
        return html_lib.unescape(match.group(1)).strip()
    idx = html.lower().find(label.lower())
    if idx != -1:
        pre = html[max(0, idx - 1200):idx]
        values = re.findall(r'<div[^>]*font-bold[^>]*>([^<]+)</div>', pre, re.S | re.I)
        if values:
            return html_lib.unescape(values[-1]).strip()
    return None


def extract_handles_from_html(page_html: str) -> set[str]:
    handles: set[str] = set()
    for raw in re.findall(r"https?://(?:www\.)?(?:x|twitter)\.com/([A-Za-z0-9_]{1,15})", page_html, re.I):
        handle = normalize_handle(raw)
        if handle:
            handles.add(handle)
    return handles


def extract_x_profile_from_html(handle: str, page_html: str) -> XProfile:
    clean_handle = normalize_handle(handle) or handle
    title = _title(page_html)
    bio = _meta(page_html, "og:description")
    profile_img_url = _meta(page_html, "og:image")
    followers_raw = _count_before_label(page_html, "Followers")
    following_raw = _count_before_label(page_html, "Following")
    times = re.findall(r'<time[^>]*datetime=["\']([^"\']+)["\']', page_html, re.S | re.I)
    if times:
        activity = RecentActivity(
            status="unknown_unverified_static_timestamp",
            latest_visible_post_at=None,
            evidence=(
                f"Static public X HTML exposed timestamp {times[0]}, but it was not "
                "verified as the profile owner's non-pinned post."
            ),
        )
    else:
        activity = RecentActivity(
            status="unknown_public_html_limited",
            latest_visible_post_at=None,
            evidence="Static public X HTML did not expose reliable recent posts.",
        )
    return XProfile(
        name=_name_from_title(title, clean_handle),
        handle=clean_handle,
        profile_url=f"https://x.com/{clean_handle}",
        bio=bio,
        profile_img_url=profile_img_url,
        followers=CountValue(followers_raw, estimate_count(followers_raw)),
        following=CountValue(following_raw, estimate_count(following_raw)),
        source_status="ok",
        title=title,
        recent_activity=activity,
    )

