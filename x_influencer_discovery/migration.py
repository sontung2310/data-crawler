"""Non-destructive migration helpers for the previous company-scoped design."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Iterable

from .platforms import account_ref, normalize_profile_url, validate_platform


@dataclass
class MigrationReport:
    source_records: int = 0
    target_inserted: int = 0
    target_updated: int = 0
    target_identity_collisions: int = 0
    source_messages_inspected: int = 0
    target_messages_sent: int = 0
    source_messages_acknowledged: int = 0
    validation_status: str = "not_run"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def legacy_document_to_candidate(
    document: dict[str, Any],
    *,
    company_id: str,
    company_name: str | None = None,
    platform: str = "x",
) -> dict[str, Any]:
    """Map one old company candidate document to the shared schema."""

    platform = validate_platform(platform)
    profile_url = normalize_profile_url(str(document.get("profile_url", "")), platform)
    old_profile = document.get("profile") or {}
    handle = document.get("handle") or profile_url.rstrip("/").rsplit("/", 1)[-1]
    reference = account_ref(
        platform,
        handle=str(handle),
        profile_url=profile_url,
    )
    profile = {
        "display_name": old_profile.get("display_name") or old_profile.get("name"),
        "bio": old_profile.get("bio"),
        "avatar_url": old_profile.get("avatar_url") or old_profile.get("profile_img_url"),
        "followers": old_profile.get("followers"),
        "following": old_profile.get("following"),
    }
    profile = {key: value for key, value in profile.items() if value is not None}
    platform_data: dict[str, Any] = {}
    if old_profile.get("recent_posts") is not None:
        platform_data["recent_posts"] = deepcopy(old_profile["recent_posts"])
    for key in ("verified", "location", "website"):
        if key in document:
            platform_data[key] = deepcopy(document[key])
    old_score = document.get("latest_final_score")
    if old_score is None:
        old_score = (document.get("evaluation") or {}).get("latest_final_score", 0)
    updated_at = document.get("updated_at") or datetime.now(timezone.utc)
    evaluation = deepcopy(document.get("evaluation") or {})
    evaluation.setdefault("status", "eligible")
    evaluation["latest_final_score"] = int(old_score or 0)
    mapped: dict[str, Any] = {
        "company_id": str(company_id),
        "platform": platform,
        "account": reference.to_dict(),
        "profile": profile,
        "platform_data": platform_data,
        "evaluation": evaluation,
        "updated_at": updated_at,
        "schema_version": 1,
    }
    if company_name:
        mapped["company_name"] = company_name
    return mapped


def migrate_candidate_documents(
    source_documents: Iterable[dict[str, Any]],
    target_collection: Any,
    *,
    company_id: str,
    company_name: str | None = None,
    platform: str = "x",
    dry_run: bool = False,
    report: MigrationReport | None = None,
) -> MigrationReport:
    """Repeatably upsert legacy documents while reporting identity conflicts."""

    report = report or MigrationReport()
    for source in source_documents:
        report.source_records += 1
        mapped = legacy_document_to_candidate(
            source,
            company_id=company_id,
            company_name=company_name,
            platform=platform,
        )
        selector = {
            "company_id": mapped["company_id"],
            "platform": mapped["platform"],
            "account.account_id": mapped["account"]["account_id"],
        }
        existing = target_collection.find_one(selector) if hasattr(target_collection, "find_one") else None
        if existing and _meaningful(existing) != _meaningful(mapped):
            report.target_identity_collisions += 1
            continue
        if dry_run:
            if existing:
                report.target_updated += 1
            else:
                report.target_inserted += 1
            continue
        result = target_collection.replace_one(selector, mapped, upsert=True)
        if getattr(result, "matched_count", 0):
            report.target_updated += 1
        else:
            report.target_inserted += 1
    return report


def migrate_legacy_collections(
    database: Any,
    mappings: Iterable[dict[str, str]],
    *,
    target_collection_name: str = "influencer_candidates",
    dry_run: bool = False,
) -> MigrationReport:
    """Migrate mapped old collections while leaving every source untouched."""

    target_collection = database[target_collection_name]
    report = MigrationReport()
    for mapping in mappings:
        source = database[mapping["collection_name"]]
        migrate_candidate_documents(
            source.find({}),
            target_collection,
            company_id=mapping["company_id"],
            company_name=mapping.get("company_name"),
            platform=mapping.get("platform", "x"),
            dry_run=dry_run,
            report=report,
        )
    report.validation_status = "dry_run" if dry_run else "completed"
    return report


def handoff_queue_messages(
    source_queue: Any,
    target_queue: Any,
    *,
    company_id: str,
    platform: str = "x",
    max_messages: int | None = None,
    report: MigrationReport | None = None,
) -> MigrationReport:
    """Send to the target before acknowledging the source delivery."""

    report = report or MigrationReport()
    inspected = 0
    while max_messages is None or inspected < max_messages:
        messages = source_queue.receive()
        if not messages:
            break
        for message in messages:
            if max_messages is not None and inspected >= max_messages:
                break
            inspected += 1
            report.source_messages_inspected += 1
            if not message.profile_url or message.parse_error:
                continue
            try:
                sent = target_queue.send(
                    message.profile_url,
                    company_id=company_id,
                    platform=platform,
                    account_id=message.account_id,
                    handle=message.handle,
                )
            except TypeError:
                sent = target_queue.send(message.profile_url)
            if sent or sent is False:
                # False means the target's idempotent producer already has the
                # message, which is still a validated handoff.
                report.target_messages_sent += int(bool(sent))
                source_queue.acknowledge(message)
                report.source_messages_acknowledged += 1
    return report


def _meaningful(document: dict[str, Any]) -> dict[str, Any]:
    return {
        key: deepcopy(value)
        for key, value in document.items()
        if key not in {"_id", "updated_at"}
    }
