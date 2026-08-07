"""Event envelope helpers for raw_collected (SQS / downstream ingest)."""
from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse, urlunparse

SCHEMA_VERSION = 1
SUPPORTED_SCHEMA_VERSIONS = {1}

CONTENT_POST = "post"
CONTENT_COMMENT = "comment"
EVENT_RAW_COLLECTED = "raw_collected"


def canonical_url(url: str) -> str:
    """Normalize URL for stable hashing (strip fragment, lowercase host)."""
    raw = (url or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
        netloc = (parsed.netloc or "").lower()
        path = parsed.path or ""
        if path != "/" and path.endswith("/"):
            path = path.rstrip("/")
        return urlunparse((parsed.scheme.lower(), netloc, path, "", parsed.query, ""))
    except Exception:
        return raw


def stable_url_id(url: str) -> str:
    """Stable short id from URL (sha256); replaces process-unstable hash()."""
    canon = canonical_url(url)
    if not canon:
        return ""
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()[:16]


def platform_id_from_prefixed(value: Optional[str]) -> str:
    """Strip optional `source:id` prefix → platform id."""
    text = (value or "").strip()
    if not text:
        return ""
    if ":" in text:
        return text.rsplit(":", 1)[-1].strip()
    return text


def external_id_for_post(row: Dict[str, Any]) -> str:
    """
    Prefer platform id from post_id; else stable URL hash.
    Returns the bare id (source is a separate envelope field).
    """
    post_id = platform_id_from_prefixed(row.get("post_id"))
    if post_id and not post_id.startswith("hash") and len(post_id) > 0:
        # newsapi:{hash(url)} and similar — if it looks like only digits from hash(),
        # still accept when post_id present; for empty platform slice fall through.
        if post_id:
            # Prefer URL-derived id when post_id was built with unstable hash()
            # Heuristic: numeric-only or negative (Python hash) → rebuild from url
            if post_id.lstrip("-").isdigit():
                url_id = stable_url_id(row.get("url") or "")
                if url_id:
                    return url_id
            return post_id
    url_id = stable_url_id(row.get("url") or "")
    if url_id:
        return url_id
    return ""


def external_id_for_comment(row: Dict[str, Any]) -> str:
    return (row.get("comment_id") or "").strip()


def partition_key(source: str, external_id: str) -> str:
    return f"{(source or '').strip()}:{(external_id or '').strip()}"


def build_event(
    *,
    content_type: str,
    source: str,
    external_id: str,
    history_id: Optional[str],
    payload: Dict[str, Any],
    parent_content_id: Optional[str] = None,
    event_type: str = EVENT_RAW_COLLECTED,
    event_id: Optional[str] = None,
) -> Dict[str, Any]:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    eid = (event_id or "").strip() or str(uuid.uuid4())
    return {
        "event_id": eid,
        "schema_version": SCHEMA_VERSION,
        "event_type": event_type,
        "content_type": content_type,
        "source": (source or "").strip(),
        "external_id": (external_id or "").strip(),
        "parent_content_id": (parent_content_id or None),
        "history_id": str(history_id) if history_id else None,
        "occurred_at": now,
        "payload": payload or {},
    }


def validate_event(event: Any) -> Tuple[bool, str]:
    if not isinstance(event, dict):
        return False, "event must be an object"
    version = event.get("schema_version")
    if version not in SUPPORTED_SCHEMA_VERSIONS:
        return False, f"unsupported schema_version={version!r}"
    content_type = event.get("content_type")
    if content_type not in (CONTENT_POST, CONTENT_COMMENT):
        return False, f"invalid content_type={content_type!r}"
    source = (event.get("source") or "").strip()
    if not source:
        return False, "source is required"
    external_id = (event.get("external_id") or "").strip()
    if not external_id:
        return False, "external_id is required"
    payload = event.get("payload")
    if payload is not None and not isinstance(payload, dict):
        return False, "payload must be an object"
    return True, ""


def post_event_from_row(row: Dict[str, Any], history_id: Optional[str]) -> Optional[Dict[str, Any]]:
    source = (row.get("source") or "").strip()
    external_id = external_id_for_post(row)
    if not source or not external_id:
        return None
    return build_event(
        content_type=CONTENT_POST,
        source=source,
        external_id=external_id,
        history_id=history_id,
        payload=dict(row),
        parent_content_id=None,
    )


def comment_event_from_row(row: Dict[str, Any], history_id: Optional[str]) -> Optional[Dict[str, Any]]:
    source = (row.get("source") or "").strip()
    external_id = external_id_for_comment(row)
    if not source or not external_id:
        return None
    parent = (row.get("parent_content_id") or "").strip() or None
    return build_event(
        content_type=CONTENT_COMMENT,
        source=source,
        external_id=external_id,
        history_id=history_id,
        payload=dict(row),
        parent_content_id=parent,
    )
