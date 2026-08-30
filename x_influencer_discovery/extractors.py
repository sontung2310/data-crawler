from __future__ import annotations

import html as html_lib
import re
from dataclasses import dataclass
from typing import Any, Iterable
from urllib.parse import urlparse

from .models import CountValue, RecentActivity, RenderedProfileSurface, XProfile
from .platforms import normalize_profile_url
from .querying import term_matches_text

NOISE_HANDLES = {
    "intent", "share", "home", "search", "i", "hashtag", "explore", "settings",
    "privacy", "tos", "download", "login", "signup", "notifications", "messages",
    "compose", "x", "twitter", "premium", "jobs", "communities", "verified_orgs",
    "gmail", "youtube", "linkedin", "facebook", "instagram", "tiktok", "medium",
    "nasa", "realdonaldtrump", "barackobama", "cristiano", "justinbieber",
    "katyperry", "ladygaga", "narendramodi", "elonmusk",
}

_X_HOSTS = {"x.com", "www.x.com", "twitter.com", "www.twitter.com"}


@dataclass(frozen=True)
class SnowballRelationships:
    """Transient relationship URLs found while expanding one qualified profile."""

    profile_urls: tuple[str, ...]
    following_incomplete: bool = False


class ProfileMarkupError(ValueError):
    """An identity-valid X response no longer exposes required profile fields."""


def normalize_handle(value: str | None) -> str | None:
    if not value:
        return None
    value = html_lib.unescape(value.strip())
    if value.startswith("@"):
        value = value[1:]
    if "://" in value or value.casefold().startswith(("x.com/", "www.x.com/", "twitter.com/", "www.twitter.com/")):
        parsed = urlparse(value if "://" in value else "https://" + value)
        if (parsed.hostname or "").casefold() not in _X_HOSTS:
            return None
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


def _profile_url_for_handle(handle: str | None, source_handle: str | None = None) -> str | None:
    clean_handle = normalize_handle(handle)
    if not clean_handle or clean_handle.casefold() == (source_handle or "").casefold():
        return None
    return normalize_profile_url(f"https://x.com/{clean_handle}")


def extract_mention_profile_urls(text: str, *, source_handle: str | None = None) -> set[str]:
    """Return valid, non-self X handles directly mentioned in post text."""
    urls: set[str] = set()
    for handle in re.findall(r"(?<![A-Za-z0-9_])@([A-Za-z0-9_]{1,15})(?![A-Za-z0-9_])", text or ""):
        profile_url = _profile_url_for_handle(handle, source_handle)
        if profile_url:
            urls.add(profile_url)
    return urls


def extract_following_profile_urls(hrefs: Iterable[str], *, source_handle: str | None = None) -> set[str]:
    """Return only direct profile links from a Following surface."""
    urls: set[str] = set()
    for href in hrefs:
        value = html_lib.unescape(str(href or "").strip())
        if value.startswith("/"):
            value = f"https://x.com{value}"
        try:
            profile_url = normalize_profile_url(value)
        except ValueError:
            continue
        handle = profile_url.rsplit("/", 1)[-1]
        if normalize_handle(handle) and handle.casefold() != (source_handle or "").casefold():
            urls.add(profile_url)
    return urls


def extract_quote_target_urls(hrefs: Iterable[str], *, source_handle: str | None = None) -> set[str]:
    """Extract the author of an outbound quoted status, not the status itself."""
    urls: set[str] = set()
    for href in hrefs:
        value = html_lib.unescape(str(href or "").strip())
        parsed = urlparse(value if "://" in value else f"https://x.com{value if value.startswith('/') else '/' + value}")
        if (parsed.hostname or "").casefold() not in _X_HOSTS:
            continue
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) < 3 or parts[1].casefold() != "status" or not parts[2].isdigit():
            continue
        profile_url = _profile_url_for_handle(parts[0], source_handle)
        if profile_url:
            urls.add(profile_url)
    return urls


def extract_relevant_relationship_urls(
    post_signals: Iterable[dict[str, Any]],
    *,
    related_terms: list[str],
    source_handle: str | None = None,
) -> set[str]:
    """Extract relationship URLs only from posts with an explicit related term."""
    urls: set[str] = set()
    terms = [term for term in related_terms if term.strip()]
    for signal in post_signals:
        text = str(signal.get("text") or "")
        if not terms or not any(term_matches_text(term, text) for term in terms):
            continue
        urls.update(extract_mention_profile_urls(text, source_handle=source_handle))
        urls.update(extract_quote_target_urls(signal.get("quote_hrefs") or [], source_handle=source_handle))
    return urls


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


def _parse_aria_metrics(label: str) -> dict[str, int | None]:
    """Extract engagement counts from X's accessible metric labels."""
    lower = label.lower()
    return {
        "replies": _metric(lower, ["replies", "reply"]),
        "reposts": _metric(lower, ["reposts", "repost"]),
        "likes": _metric(lower, ["likes", "like"]),
        "views": _metric(lower, ["views", "view"]),
    }


def _metric(text: str, names: list[str]) -> int | None:
    for name in names:
        patterns = [
            rf"([0-9][0-9,.]*\s*[km]?)\s+{name}",
            rf"{name}\s+([0-9][0-9,.]*\s*[km]?)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                return estimate_count(match.group(1).strip())
    return None


def _strip_tags(value: str) -> str:
    value = re.sub(r"<script.*?</script>|<style.*?</style>", " ", value, flags=re.S | re.I)
    value = re.sub(r"<[^>]+>", " ", value)
    return re.sub(r"\s+", " ", html_lib.unescape(value)).strip()


def _meta(html: str, property_name: str) -> str | None:
    """Read one meta tag without allowing a regex match to cross tag bounds."""
    target = property_name.casefold()
    for tag in re.findall(r"<meta\b[^>]*>", html, re.S | re.I):
        attributes = {
            name.casefold(): html_lib.unescape(value).strip()
            for name, _quote, value in re.findall(
                r"([^\s=/>]+)\s*=\s*(['\"])(.*?)\2", tag, re.S | re.I
            )
        }
        if (attributes.get("property") or attributes.get("name") or "").casefold() != target:
            continue
        content = attributes.get("content")
        if content:
            return content
    return None


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
    # Authenticated X profiles render counts as link text such as
    # ``69.3K Followers`` rather than the older ``font-bold`` sibling layout.
    # Read only a count immediately adjacent to the requested label so an
    # unrelated number elsewhere in the page cannot become profile metadata.
    count = r"([0-9][0-9,]*(?:\.[0-9]+)?\s*[KMB]?)"
    for inner_html in re.findall(r"<a\b[^>]*>(.*?)</a>", html, re.S | re.I):
        text = _strip_tags(inner_html)
        match = re.search(rf"{count}\s+{re.escape(label)}\b", text, re.I)
        if match:
            return re.sub(r"\s+", "", match.group(1))
    return None


def extract_handles_from_html(page_html: str) -> set[str]:
    handles: set[str] = set()
    for raw in re.findall(r"https?://(?:www\.)?(?:x|twitter)\.com/([A-Za-z0-9_]{1,15})", page_html, re.I):
        handle = normalize_handle(raw)
        if handle:
            handles.add(handle)
    return handles


def extract_x_profile_from_html(
    handle: str,
    page_html: str,
    *,
    rendered_surface: RenderedProfileSurface | None = None,
) -> XProfile:
    clean_handle = normalize_handle(handle) or handle
    title = _title(page_html)
    # An authenticated rendered surface is authoritative, even when a user
    # intentionally has no bio/avatar. Current X Open Graph metadata is often
    # page-generic (for example, X's default social-share image), so falling
    # back to it in that case would persist incorrect profile data.
    bio = rendered_surface.bio if rendered_surface is not None else _meta(page_html, "og:description")
    profile_img_url = rendered_surface.profile_img_url if rendered_surface is not None else _meta(page_html, "og:image")
    followers_raw = _count_before_label(page_html, "Followers")
    following_raw = _count_before_label(page_html, "Following")
    if not title:
        raise ProfileMarkupError("profile markup is missing the title/identity surface")
    if followers_raw is None:
        raise ProfileMarkupError("profile markup is missing the followers surface")
    followers_estimated = estimate_count(followers_raw)
    if followers_estimated is None:
        raise ProfileMarkupError("profile markup contains an unparseable followers value")
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
    profile_url = f"https://x.com/{clean_handle}"
    if rendered_surface and rendered_surface.canonical_url:
        try:
            canonical_url = normalize_profile_url(rendered_surface.canonical_url)
            if canonical_url.rsplit("/", 1)[-1].casefold() == clean_handle.casefold():
                profile_url = canonical_url
        except ValueError:
            pass
    return XProfile(
        name=_name_from_title(title, clean_handle),
        handle=clean_handle,
        profile_url=profile_url,
        bio=bio,
        profile_img_url=profile_img_url,
        followers=CountValue(followers_raw, followers_estimated),
        following=CountValue(following_raw, estimate_count(following_raw)),
        source_status="ok",
        title=title,
        recent_activity=activity,
    )
