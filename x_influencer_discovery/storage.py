from __future__ import annotations

import json
import logging
import os
import re
from copy import deepcopy
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .models import AccountRef
from .platforms import account_ref, normalize_profile_url, validate_platform

logger = logging.getLogger(__name__)


class CompanyCandidateStore:
    """Current-state candidate persistence for one company collection only."""

    def __init__(self, collection: Any):
        self.collection = collection
        self.collection.create_index([("profile_url", 1)], unique=True)
        self.collection.create_index([("latest_final_score", -1)])

    @staticmethod
    def collection_name(company_name: str) -> str:
        slug = re.sub(r"[^a-z0-9]+", "_", company_name.casefold()).strip("_")
        if not slug:
            raise ValueError("company name must contain letters or digits")
        return f"{slug}_candidates"

    @classmethod
    def for_company(cls, database: Any, company_name: str) -> "CompanyCandidateStore":
        return cls(database[cls.collection_name(company_name)])

    def upsert(
        self,
        profile: Any,
        recent_posts: list[dict[str, Any]],
        *,
        final_score: int,
        updated_at: datetime | None = None,
    ) -> str:
        """Replace current observable candidate state without retaining evidence."""
        profile_url = normalize_profile_url(profile.profile_url)
        document = {
            "profile_url": profile_url,
            "handle": profile_url.rsplit("/", 1)[-1],
            "profile": {
                "name": profile.name,
                "bio": profile.bio,
                "profile_img_url": profile.profile_img_url,
                "followers": profile.followers.estimated,
                "following": profile.following.estimated,
                "recent_posts": deepcopy(recent_posts[:5]),
            },
            "latest_final_score": int(final_score),
            "updated_at": updated_at or datetime.now(timezone.utc),
        }
        result = self.collection.replace_one({"profile_url": profile_url}, document, upsert=True)
        return "updated" if getattr(result, "matched_count", 0) else "inserted"

    def is_fresh(
        self,
        value: str,
        *,
        refresh_after_hours: int,
        now: datetime | None = None,
    ) -> bool:
        """Return whether an eligible candidate was updated within the interval."""
        profile_url = normalize_profile_url(value)
        document = self.collection.find_one({"profile_url": profile_url}, {"updated_at": 1})
        updated_at = document.get("updated_at") if isinstance(document, dict) else None
        if not isinstance(updated_at, datetime):
            return False
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)
        reference = now or datetime.now(timezone.utc)
        if reference.tzinfo is None:
            reference = reference.replace(tzinfo=timezone.utc)
        return updated_at >= reference - timedelta(hours=refresh_after_hours)

    def delete(self, value: str) -> bool:
        """Remove a no-longer-eligible current record, if one exists."""
        profile_url = normalize_profile_url(value)
        result = self.collection.delete_one({"profile_url": profile_url})
        return bool(getattr(result, "deleted_count", 0))

    def leaderboard(self, limit: int | None = None) -> list[dict[str, Any]]:
        cursor = self.collection.find({})
        if not isinstance(cursor, list) and hasattr(cursor, "sort"):
            cursor = cursor.sort([("latest_final_score", -1), ("profile_url", 1)])
            if limit is not None and hasattr(cursor, "limit"):
                cursor = cursor.limit(limit)
            return [self._public_document(document) for document in cursor]
        documents = sorted(
            cursor,
            key=lambda document: (-int(document.get("latest_final_score") or 0), document.get("profile_url", "")),
        )
        documents = documents[:limit] if limit is not None else documents
        return [self._public_document(document) for document in documents]

    @staticmethod
    def _public_document(document: dict[str, Any]) -> dict[str, Any]:
        """Return a JSON-ready view without Mongo's private implementation fields."""
        return {
            key: _json_ready(value)
            for key, value in document.items()
            if key != "_id"
        }


class CandidateStore:
    """Shared current-state storage for all company/platform relationships."""

    collection_name = "influencer_candidates"

    def __init__(self, collection: Any):
        self.collection = collection
        self.collection.create_index(
            [("company_id", 1), ("platform", 1), ("account.account_id", 1)],
            unique=True,
        )
        self.collection.create_index(
            [("company_id", 1), ("platform", 1), ("evaluation.latest_final_score", -1)],
        )
        self.collection.create_index(
            [("company_id", 1), ("platform", 1), ("updated_at", -1)],
        )

    def upsert(
        self,
        profile: Any,
        recent_posts: list[dict[str, Any]] | None = None,
        *,
        company_id: str,
        platform: str = "x",
        company_name: str | None = None,
        final_score: int,
        evaluation: dict[str, Any] | None = None,
        platform_data: dict[str, Any] | None = None,
        discovery_evidence: list[dict[str, Any]] | None = None,
        updated_at: datetime | None = None,
        account: Any | None = None,
    ) -> str:
        platform = validate_platform(platform)
        if not company_id or not str(company_id).strip():
            raise ValueError("company_id must not be empty")
        profile_url = _profile_value(profile, "profile_url")
        handle = _profile_value(profile, "handle")
        supplied_account_id = _profile_value(profile, "account_id")
        if account is not None:
            if isinstance(account, dict):
                supplied_account_id = account.get("account_id") or supplied_account_id
                handle = account.get("handle") or handle
                profile_url = account.get("profile_url") or profile_url
            else:
                supplied_account_id = getattr(account, "account_id", supplied_account_id)
                handle = getattr(account, "handle", handle) or handle
                profile_url = getattr(account, "profile_url", profile_url) or profile_url
        reference = account_ref(
            platform,
            account_id=supplied_account_id,
            handle=handle,
            profile_url=profile_url,
        )

        followers = _profile_count(profile, "followers")
        following = _profile_count(profile, "following")
        common_profile = _without_none({
            "display_name": _profile_value(profile, "display_name", "name"),
            "bio": _profile_value(profile, "bio"),
            "avatar_url": _profile_value(profile, "avatar_url", "profile_img_url"),
            "followers": followers,
            "following": following,
        })
        x_posts = deepcopy((recent_posts or [])[:5])
        data = deepcopy(platform_data or {})
        if x_posts and "recent_posts" not in data:
            data["recent_posts"] = x_posts
        elif "recent_posts" not in data and recent_posts is not None:
            data["recent_posts"] = x_posts
        data = _without_none(data)

        stored_evaluation = deepcopy(evaluation or {})
        stored_evaluation.setdefault("status", "eligible")
        stored_evaluation["latest_final_score"] = int(final_score)
        selector = {
            "company_id": str(company_id),
            "platform": platform,
            "account.account_id": reference.account_id,
        }
        document: dict[str, Any] = {
            "company_id": str(company_id),
            "platform": platform,
            "account": reference.to_dict(),
            "profile": common_profile,
            "platform_data": data,
            "evaluation": stored_evaluation,
            "updated_at": updated_at or datetime.now(timezone.utc),
            "schema_version": 1,
        }
        if discovery_evidence is not None:
            document["discovery_evidence"] = deepcopy(discovery_evidence)
        else:
            existing = self.collection.find_one(selector, {"discovery_evidence": 1})
            if isinstance(existing, dict) and existing.get("discovery_evidence"):
                document["discovery_evidence"] = deepcopy(existing["discovery_evidence"])
        if company_name:
            document["company_name"] = company_name

        result = self.collection.replace_one(selector, document, upsert=True)
        return "updated" if getattr(result, "matched_count", 0) else "inserted"

    def seed_accounts(
        self,
        *,
        company_id: str,
        platform: str = "x",
        minimum_score: int = 60,
        limit: int | None = None,
    ) -> list[AccountRef]:
        """Return an ordered snapshot of eligible accounts for snowball expansion."""

        platform = validate_platform(platform)
        if not company_id or not str(company_id).strip():
            raise ValueError("company_id must not be empty")
        if minimum_score < 0:
            raise ValueError("minimum_score must not be negative")
        if limit is not None and limit < 1:
            raise ValueError("limit must be at least 1")

        selector = {
            "company_id": str(company_id),
            "platform": platform,
            "evaluation.latest_final_score": {"$gt": int(minimum_score)},
        }
        cursor = self.collection.find(selector)
        if isinstance(cursor, list):
            # Lightweight test/fake collections do not implement Mongo's
            # comparison operators, so enforce the same predicate locally.
            documents = [
                document for document in cursor
                if int(document.get("evaluation", {}).get("latest_final_score") or 0) > minimum_score
            ]
            documents.sort(key=lambda document: (
                -int(document.get("evaluation", {}).get("latest_final_score") or 0),
                document.get("account", {}).get("account_id", ""),
            ))
            if limit is not None:
                documents = documents[:limit]
        else:
            cursor = cursor.sort([
                ("evaluation.latest_final_score", -1),
                ("account.account_id", 1),
            ])
            if limit is not None and hasattr(cursor, "limit"):
                cursor = cursor.limit(limit)
            documents = list(cursor)

        accounts: list[AccountRef] = []
        for document in documents:
            account = document.get("account") if isinstance(document, dict) else None
            if not isinstance(account, dict) or not account.get("profile_url"):
                continue
            accounts.append(account_ref(
                platform,
                account_id=str(account["account_id"]) if account.get("account_id") is not None else None,
                handle=str(account["handle"]) if account.get("handle") is not None else None,
                profile_url=str(account["profile_url"]),
            ))
        return accounts

    def is_fresh(
        self,
        value: str | None = None,
        *,
        company_id: str,
        platform: str = "x",
        account_id: str | None = None,
        refresh_after_hours: int,
        now: datetime | None = None,
    ) -> bool:
        platform = validate_platform(platform)
        resolved_id = account_id or account_ref(platform, profile_url=value).account_id
        selector = {
            "company_id": str(company_id),
            "platform": platform,
            "account.account_id": resolved_id,
        }
        document = self.collection.find_one(selector, {"updated_at": 1})
        updated_at = document.get("updated_at") if isinstance(document, dict) else None
        if not isinstance(updated_at, datetime):
            return False
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)
        reference = now or datetime.now(timezone.utc)
        if reference.tzinfo is None:
            reference = reference.replace(tzinfo=timezone.utc)
        return updated_at >= reference - timedelta(hours=refresh_after_hours)

    def delete(
        self,
        value: str | None = None,
        *,
        company_id: str,
        platform: str = "x",
        account_id: str | None = None,
    ) -> bool:
        platform = validate_platform(platform)
        resolved_id = account_id or account_ref(platform, profile_url=value).account_id
        result = self.collection.delete_one({
            "company_id": str(company_id),
            "platform": platform,
            "account.account_id": resolved_id,
        })
        return bool(getattr(result, "deleted_count", 0))

    def leaderboard(
        self,
        *,
        company_id: str,
        platform: str = "x",
        limit: int | None = None,
        max_age_days: int | None = 30,
        now: datetime | None = None,
    ) -> list[dict[str, Any]]:
        cutoff = _leaderboard_cutoff(max_age_days, now=now)
        platform = validate_platform(platform)
        selector = {"company_id": str(company_id), "platform": platform}
        if cutoff is not None:
            selector["updated_at"] = {"$gte": cutoff}
        cursor = self.collection.find(selector)
        if not isinstance(cursor, list) and hasattr(cursor, "sort"):
            cursor = cursor.sort([
                ("evaluation.latest_final_score", -1),
                ("account.account_id", 1),
            ])
            documents = list(cursor)
            if cutoff is not None:
                documents = [
                    document
                    for document in documents
                    if _is_recent(document.get("updated_at"), cutoff)
                ]
            if limit is not None:
                documents = documents[:limit]
            return [self._public_document(document) for document in documents]
        # Lightweight test/fake collections may return a list while ignoring
        # Mongo comparison operators. Re-read the base selector and enforce
        # both the company/platform and timestamp predicates locally.
        source_documents = (
            self.collection.find({"company_id": str(company_id), "platform": platform})
            if cutoff is not None
            else cursor
        )
        cursor = [
            document
            for document in source_documents
            if document.get("company_id") == str(company_id)
            and document.get("platform") == platform
            and (cutoff is None or _is_recent(document.get("updated_at"), cutoff))
        ]
        documents = sorted(
            cursor,
            key=lambda document: (
                -int(document.get("evaluation", {}).get("latest_final_score") or 0),
                document.get("account", {}).get("account_id", ""),
            ),
        )
        documents = documents[:limit] if limit is not None else documents
        return [self._public_document(document) for document in documents]

    @staticmethod
    def _public_document(document: dict[str, Any]) -> dict[str, Any]:
        return {
            key: _json_ready(value)
            for key, value in document.items()
            if key != "_id"
        }


def _profile_value(profile: Any, *names: str) -> Any:
    if isinstance(profile, dict):
        for name in names:
            if profile.get(name) is not None:
                return profile[name]
        return None
    for name in names:
        value = getattr(profile, name, None)
        if value is not None:
            return value
    return None


def _leaderboard_cutoff(
    max_age_days: int | None,
    *,
    now: datetime | None = None,
) -> datetime | None:
    if max_age_days is None:
        return None
    if max_age_days < 0:
        raise ValueError("max_age_days must not be negative")
    reference = now or datetime.now(timezone.utc)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    return reference - timedelta(days=max_age_days)


def _is_recent(value: Any, cutoff: datetime) -> bool:
    if not isinstance(value, datetime):
        return False
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value >= cutoff


def _profile_count(profile: Any, name: str) -> int | None:
    value = _profile_value(profile, name)
    if isinstance(value, dict):
        return value.get("estimated")
    estimated = getattr(value, "estimated", None)
    return estimated if estimated is not None else value if isinstance(value, int) else None


def _without_none(value: dict[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if item is not None}


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value


@contextmanager
def open_company_candidate_store(
    mongodb_url: str,
    company_name: str | None = None,
    *,
    company_id: str | None = None,
    platform: str = "x",
):
    """Open the shared candidate collection for one company/platform view.

    The historical function name is retained so existing callers can migrate
    without a flag-day rename.  ``company_name`` remains display metadata;
    ``company_id`` is the relationship identity and defaults to a stable slug
    only for legacy callers.
    """
    try:
        from pymongo import MongoClient
    except Exception as exc:  # pragma: no cover - dependency/environment specific
        raise RuntimeError("pymongo is required for durable candidate persistence") from exc
    client = MongoClient(mongodb_url, serverSelectionTimeoutMS=5_000)
    try:
        try:
            database = client.get_default_database()
        except Exception:
            database = client[os.getenv("MONGODB_DBNAME", "influencer_discovery")]
        resolved_company_id = company_id or _company_id_from_name(company_name)
        yield CandidateStore(database[CandidateStore.collection_name])
    finally:
        client.close()


def _company_id_from_name(company_name: str | None) -> str:
    if not company_name or not company_name.strip():
        raise ValueError("company_id or company_name is required")
    slug = re.sub(r"[^a-z0-9]+", "_", company_name.casefold()).strip("_")
    if not slug:
        raise ValueError("company name must contain letters or digits")
    return slug


def save_json(output: dict[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    return path


def maybe_save_mongo(output: dict[str, Any], mongodb_url: str | None, collection_name: str = "x_influencer_runs") -> None:
    """Deprecated compatibility hook retained as a no-op.

    The durable design permits only the company-scoped current-state candidate
    collection. Persisting legacy run/audit documents would violate that data
    boundary, so callers may keep invoking this historical helper safely but it
    never writes MongoDB data.
    """
    del output, collection_name
    if mongodb_url:
        logger.warning("legacy run-document Mongo persistence is disabled; use the durable company leaderboard")
