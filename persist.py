"""Mongo persistence for crawl data, influencer work, and task state."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional, Tuple

from bson import ObjectId
from pymongo import MongoClient

from config import MONGODB_DBNAME, MONGODB_TLS, MONGODB_URI

_mongo_client = None
_mongo_db = None
_RAW_INDEXES_READY = False


def get_mongo_db():
    global _mongo_client, _mongo_db
    if _mongo_db is not None:
        return _mongo_db
    if not MONGODB_URI or not MONGODB_DBNAME:
        raise RuntimeError("Mongo settings missing: MONGODB_URI / MONGODB_DBNAME")

    kwargs: Dict[str, Any] = {
        "serverSelectionTimeoutMS": 10000,
        "connectTimeoutMS": 10000,
    }
    uri = MONGODB_URI
    use_tls = MONGODB_TLS or uri.startswith("mongodb+srv://")
    if use_tls:
        import certifi
        from pymongo.server_api import ServerApi

        kwargs["server_api"] = ServerApi("1")
        kwargs["tlsCAFile"] = certifi.where()

    _mongo_client = MongoClient(uri, **kwargs)
    _mongo_db = _mongo_client[MONGODB_DBNAME]
    return _mongo_db


def ensure_raw_indexes() -> None:
    global _RAW_INDEXES_READY
    if _RAW_INDEXES_READY:
        return
    db = get_mongo_db()
    db["raw_posts"].create_index(
        [("source", 1), ("external_id", 1)],
        unique=True,
        name="uniq_source_external_id",
    )
    db["raw_comments"].create_index(
        [("source", 1), ("external_id", 1)],
        unique=True,
        name="uniq_source_external_id",
    )
    db["raw_comments"].create_index(
        [("source", 1), ("parent_content_id", 1)],
        name="idx_parent_content",
    )
    db["crawl_tasks"].create_index("task_id", unique=True, name="uniq_task_id")
    db["influencer_candidate_evidence"].create_index(
        [("task_id", 1), ("company_id", 1), ("platform", 1), ("account_id", 1)],
        unique=True,
        name="uniq_influencer_candidate_evidence",
    )
    _RAW_INDEXES_READY = True


def persist_raw_posts(rows: Iterable[dict]) -> Tuple[int, int]:
    ensure_raw_indexes()
    db = get_mongo_db()
    col = db["raw_posts"]
    created = 0
    updated = 0
    now = datetime.now(timezone.utc)

    for p in rows:
        source = (p.get("source") or "").strip()
        external_id = (p.get("external_id") or "").strip()
        if not source or not external_id:
            continue

        payload = p.get("payload") if isinstance(p.get("payload"), dict) else None
        body = dict(payload) if payload else {k: v for k, v in p.items() if k not in ("payload",)}

        query = (p.get("query") or body.get("query") or "").strip() or None
        doc_set = {
            "query": query,
            "source": source,
            "external_id": external_id,
            "url": (body.get("url") or p.get("url") or "").strip() or None,
            "title": body.get("title") if "title" in body else p.get("title") or "",
            "text": body.get("text") if "text" in body else p.get("text") or "",
            "text_html": body.get("text_html") if "text_html" in body else p.get("text_html") or "",
            "author": body.get("author") if "author" in body else p.get("author"),
            "published_ts": body.get("published_ts") if "published_ts" in body else p.get("published_ts"),
            "crawled_at": body.get("crawled_at") if "crawled_at" in body else p.get("crawled_at"),
            "engagement": body.get("engagement") if "engagement" in body else p.get("engagement"),
            "payload": body,
            "updated_at": now,
        }
        task_id = (p.get("task_id") or "").strip()
        if task_id:
            doc_set["task_id"] = task_id
        history_id = p.get("history_id")
        if history_id is not None:
            try:
                doc_set["history_id"] = (
                    ObjectId(str(history_id))
                    if not isinstance(history_id, ObjectId)
                    else history_id
                )
            except Exception:
                doc_set["history_id"] = str(history_id)
        event_id = (p.get("event_id") or "").strip()
        if event_id:
            doc_set["event_id"] = event_id

        for extra in ("post_id", "subreddit", "domain", "channel_id", "tags", "topic_categories"):
            if extra in body and body[extra] is not None:
                doc_set[extra] = body[extra]

        res = col.update_one(
            {"source": source, "external_id": external_id},
            {"$set": doc_set, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
        if res.upserted_id is not None:
            created += 1
        elif res.matched_count:
            updated += 1

    return created, updated


def persist_raw_comments(rows: Iterable[dict]) -> Tuple[int, int]:
    ensure_raw_indexes()
    db = get_mongo_db()
    col = db["raw_comments"]
    created = 0
    updated = 0
    now = datetime.now(timezone.utc)

    for p in rows:
        source = (p.get("source") or "").strip()
        external_id = (p.get("external_id") or "").strip()
        if not source or not external_id:
            continue

        payload = p.get("payload") if isinstance(p.get("payload"), dict) else None
        body = dict(payload) if payload else {k: v for k, v in p.items() if k not in ("payload",)}

        query = (p.get("query") or body.get("query") or "").strip() or None
        doc_set = {
            "query": query,
            "source": source,
            "external_id": external_id,
            "comment_id": body.get("comment_id") or external_id,
            "parent_content_id": (
                body.get("parent_content_id") or p.get("parent_content_id") or ""
            ).strip()
            or None,
            "parent_content_url": (body.get("parent_content_url") or "").strip() or None,
            "parent_comment_id": body.get("parent_comment_id"),
            "author": body.get("author"),
            "author_channel_id": body.get("author_channel_id"),
            "text": body.get("text") or "",
            "like_count": body.get("like_count"),
            "reply_count": body.get("reply_count"),
            "published_ts": body.get("published_ts"),
            "crawled_at": body.get("crawled_at"),
            "payload": body,
            "updated_at": now,
        }
        task_id = (p.get("task_id") or "").strip()
        if task_id:
            doc_set["task_id"] = task_id
        history_id = p.get("history_id")
        if history_id is not None:
            try:
                doc_set["history_id"] = (
                    ObjectId(str(history_id))
                    if not isinstance(history_id, ObjectId)
                    else history_id
                )
            except Exception:
                doc_set["history_id"] = str(history_id)
        event_id = (p.get("event_id") or "").strip()
        if event_id:
            doc_set["event_id"] = event_id

        res = col.update_one(
            {"source": source, "external_id": external_id},
            {"$set": doc_set, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
        if res.upserted_id is not None:
            created += 1
        elif res.matched_count:
            updated += 1

    return created, updated


def persist_influencer(row: Dict[str, Any]) -> Tuple[int, int]:
    """Upsert one influencer document, keyed by its normalized X handle."""
    ensure_raw_indexes()
    source = (row.get("source") or "").strip()
    handle = (row.get("handle") or "").strip().lstrip("@").lower()
    if not source or not handle:
        return 0, 0

    now = datetime.now(timezone.utc)
    doc = {
        "platform": "x",
        "name": row.get("name"),
        "handle": handle,
        "bio": row.get("bio"),
        "profile_img_url": row.get("profile_img_url"),
        "followers_count": row.get("followers_count"),
        "following_count": row.get("following_count"),
        "updated_at": now,
    }
    update: Dict[str, Any] = {
        "$set": doc,
        "$setOnInsert": {"created_at": now},
    }
    topic = (row.get("topic") or "").strip()
    if topic:
        update["$addToSet"] = {"topics": topic}
    res = get_mongo_db()["influencers"].update_one(
        {"_id": f"x:{handle}"},
        update,
        upsert=True,
    )
    return (1, 0) if res.upserted_id is not None else (0, 1)


def save_influencer_run(
    task_id: str,
    company_domain_id: str,
    output: Dict[str, Any],
) -> None:
    """Store the final ranked influencer output and diagnostics for one SQS task."""
    ensure_raw_indexes()
    now = datetime.now(timezone.utc)
    get_mongo_db()["x_influencer_runs"].update_one(
        {"task_id": task_id},
        {
            "$set": {
                "task_id": task_id,
                "company_domain_id": company_domain_id.strip().lower(),
                "query": output.get("query"),
                "platform": output.get("platform"),
                "generated_at": output.get("generated_at"),
                "results": deepcopy(output.get("results") or []),
                "diagnostics": deepcopy(output.get("diagnostics") or {}),
                "updated_at": now,
            },
            "$setOnInsert": {"created_at": now},
        },
        upsert=True,
    )


def upsert_influencer_candidate_evidence(
    *,
    task_id: str,
    company_id: str,
    platform: str,
    account_id: str,
    handle: str | None,
    evidence: Iterable[dict[str, Any]],
) -> None:
    """Persist discovery evidence before profile work enters the durable queue."""
    ensure_raw_indexes()
    evidence_rows = [deepcopy(dict(item)) for item in evidence if isinstance(item, dict)]
    if not evidence_rows:
        return
    now = datetime.now(timezone.utc)
    get_mongo_db()["influencer_candidate_evidence"].update_one(
        {
            "task_id": str(task_id),
            "company_id": str(company_id),
            "platform": str(platform),
            "account_id": str(account_id),
        },
        {
            "$set": {
                "task_id": str(task_id),
                "company_id": str(company_id),
                "platform": str(platform),
                "account_id": str(account_id),
                "handle": handle,
                "updated_at": now,
            },
            "$addToSet": {"evidence": {"$each": evidence_rows}},
            "$setOnInsert": {"created_at": now},
        },
        upsert=True,
    )


def get_influencer_candidate_evidence(
    *,
    task_id: str,
    company_id: str,
    platform: str,
    account_id: str,
) -> list[dict[str, Any]]:
    """Read the durable evidence accumulated for one queued account."""
    ensure_raw_indexes()
    document = get_mongo_db()["influencer_candidate_evidence"].find_one(
        {
            "task_id": str(task_id),
            "company_id": str(company_id),
            "platform": str(platform),
            "account_id": str(account_id),
        },
        {"_id": 0, "evidence": 1},
    )
    return list(document.get("evidence") or []) if isinstance(document, dict) else []


def save_crawl_task(doc: Dict[str, Any]) -> None:
    ensure_raw_indexes()
    db = get_mongo_db()
    task_id = doc["task_id"]
    now = datetime.now(timezone.utc)
    payload = dict(doc)
    payload["updated_at"] = now
    db["crawl_tasks"].update_one(
        {"task_id": task_id},
        {"$set": payload, "$setOnInsert": {"created_at": now}},
        upsert=True,
    )


def get_crawl_task(task_id: str) -> Optional[Dict[str, Any]]:
    ensure_raw_indexes()
    db = get_mongo_db()
    doc = db["crawl_tasks"].find_one({"task_id": task_id}, {"_id": 0})
    return doc
