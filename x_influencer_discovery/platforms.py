"""Small platform boundary shared by queue, storage, and X integration.

Only X fetching is implemented in this feature.  Instagram and TikTok are
still accepted as contract values so the shared collection and queue naming
scheme does not need another redesign when their adapters are added.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from .models import AccountRef

SUPPORTED_PLATFORMS = ("x", "instagram", "tiktok")
IMPLEMENTED_PLATFORMS = frozenset({"x"})
_X_PROFILE_HOSTS = {"x.com", "www.x.com", "twitter.com", "www.twitter.com"}
_X_HANDLE = re.compile(r"^[A-Za-z0-9_]{1,15}$")
_INSTAGRAM_HANDLE = re.compile(r"^[A-Za-z0-9._]{1,30}$")
_TIKTOK_HANDLE = re.compile(r"^@[A-Za-z0-9._-]{1,24}$")


def validate_platform(value: str) -> str:
    platform = str(value or "").strip().casefold()
    if platform not in SUPPORTED_PLATFORMS:
        raise ValueError(f"unsupported platform: {value!r}")
    return platform


def queue_name(platform: str) -> str:
    return f"{validate_platform(platform)}-profile-jobs.fifo"


def dlq_name(platform: str) -> str:
    return f"{validate_platform(platform)}-profile-jobs-dlq.fifo"


def normalize_profile_url(value: str, platform: str = "x") -> str:
    """Canonicalize a profile URL for a supported platform.

    X retains the historical ``https://x.com/<handle>`` form.  The other
    platforms only need deterministic URL normalization until their fetchers
    are implemented.
    """

    platform = validate_platform(platform)
    raw = str(value).strip()
    if not raw:
        raise ValueError("profile URL must not be empty")
    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError(f"not a {platform} profile URL: {value!r}")
    parts = [part for part in parsed.path.split("/") if part]

    if platform == "x":
        if parsed.hostname not in _X_PROFILE_HOSTS or len(parts) != 1 or parsed.query or parsed.fragment:
            raise ValueError(f"not an X profile URL: {value!r}")
        if not _X_HANDLE.fullmatch(parts[0]):
            raise ValueError(f"not an X profile URL: {value!r}")
        return f"https://x.com/{parts[0].lower()}"

    if platform == "instagram":
        if parsed.hostname not in {"instagram.com", "www.instagram.com"} or len(parts) != 1 or parsed.query or parsed.fragment:
            raise ValueError(f"not an Instagram profile URL: {value!r}")
        if not _INSTAGRAM_HANDLE.fullmatch(parts[0]):
            raise ValueError(f"not an Instagram profile URL: {value!r}")
        return f"https://instagram.com/{parts[0]}"

    if parsed.hostname not in {"tiktok.com", "www.tiktok.com"} or len(parts) != 1 or parsed.query or parsed.fragment:
        raise ValueError(f"not a TikTok profile URL: {value!r}")
    if not _TIKTOK_HANDLE.fullmatch(parts[0]):
        raise ValueError(f"not a TikTok profile URL: {value!r}")
    return f"https://www.tiktok.com/{parts[0]}"


def account_id_for(
    platform: str,
    *,
    account_id: str | None = None,
    profile_url: str | None = None,
    handle: str | None = None,
) -> str:
    """Return the stable native ID, or a canonical URL fallback."""

    platform = validate_platform(platform)
    if account_id is not None and str(account_id).strip():
        return str(account_id).strip()
    if profile_url is not None and str(profile_url).strip():
        return f"url:{normalize_profile_url(profile_url, platform)}"
    if handle is not None and str(handle).strip():
        return f"handle:{platform}:{str(handle).strip().casefold()}"
    raise ValueError("account requires account_id, profile_url, or handle")


def account_ref(
    platform: str,
    *,
    account_id: str | None = None,
    handle: str | None = None,
    profile_url: str | None = None,
) -> AccountRef:
    platform = validate_platform(platform)
    canonical_url = None
    if profile_url:
        canonical_url = normalize_profile_url(profile_url, platform)
    resolved_handle = handle
    if not resolved_handle and canonical_url:
        resolved_handle = canonical_url.rstrip("/").rsplit("/", 1)[-1].lstrip("@")
    return AccountRef(
        account_id=account_id_for(
            platform,
            account_id=account_id,
            profile_url=canonical_url,
            handle=resolved_handle,
        ),
        handle=resolved_handle,
        profile_url=canonical_url,
    )


@dataclass(frozen=True)
class PlatformAdapter:
    """Contract metadata for a platform implementation."""

    platform: str
    implemented: bool

    def normalize_profile_url(self, value: str) -> str:
        return normalize_profile_url(value, self.platform)

    def account_ref(self, **kwargs: Any) -> AccountRef:
        return account_ref(self.platform, **kwargs)


_ADAPTERS = {
    platform: PlatformAdapter(platform, platform in IMPLEMENTED_PLATFORMS)
    for platform in SUPPORTED_PLATFORMS
}


def get_platform_adapter(platform: str) -> PlatformAdapter:
    return _ADAPTERS[validate_platform(platform)]


def account_group_id(platform: str, account_id: str) -> str:
    """Stable FIFO group derived from platform/account, never one global group."""

    identity = f"{validate_platform(platform)}:{account_id}"
    return f"account-{hashlib.sha256(identity.encode('utf-8')).hexdigest()}"
