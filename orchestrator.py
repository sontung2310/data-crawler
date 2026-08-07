"""Time-delta helpers and crawl orchestration."""
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from config import (
    CRAWL_MAX_WORKERS,
    CRAWL_PW_REDDIT_SLOTS,
    CRAWL_PW_X_SLOTS,
    CRAWL_TASK_TIMEOUT_SEC,
)
from events import (
    comment_event_from_row,
    external_id_for_comment,
    external_id_for_post,
    post_event_from_row,
    validate_event,
)
from persist import persist_raw_comments, persist_raw_posts, save_crawl_task
from publishers import get_publisher
from sources import REGISTRY, resolve_adapters
from sources.utils import (
    filter_posts_by_time_filter,
    normalize_time_filter,
    time_filter_from_days,
    time_filter_to_days,
)

from app.alerts import notify_session_expired
from exceptions import SessionExpiredError

logger = logging.getLogger(__name__)

_x_slots = threading.Semaphore(max(1, CRAWL_PW_X_SLOTS))
_reddit_slots = threading.Semaphore(max(1, CRAWL_PW_REDDIT_SLOTS))
_executor = ThreadPoolExecutor(max_workers=max(1, CRAWL_MAX_WORKERS))

# Fresh modes (X live): only keep posts with enough discussion.
_MIN_COMMENTS_FRESH = 10


def normalize_time_delta(value: Any) -> Optional[str]:
    """API time_delta → canonical hour|day|week|month|year (or day-count soft filter via days)."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError("invalid time_delta")
    if isinstance(value, int):
        mapping = {1: "day", 7: "week", 30: "month", 365: "year"}
        if value in mapping:
            return mapping[value]
        if value > 0:
            return time_filter_from_days(value)
        raise ValueError(f"invalid time_delta int {value!r}")
    text = str(value).strip().lower()
    # allow bare day counts as strings
    if text.isdigit():
        return normalize_time_delta(int(text))
    return normalize_time_filter(text)


def _normalize_fetch_result(result: Any) -> tuple[List[dict], List[dict]]:
    if result is None:
        return [], []
    if isinstance(result, list):
        return list(result), []
    if isinstance(result, dict):
        posts = result.get("posts") or result.get("videos") or []
        comments = result.get("comments") or []
        return list(posts), list(comments)
    return [], []


class TaskState:
    """Thread-safe crawl task progress (also flushed to Mongo)."""

    def __init__(
        self,
        task_id: str,
        query: str,
        sources: List[str],
        params: Dict[str, Any],
        *,
        status: str = "running",
        persist_now: bool = True,
    ):
        self.lock = threading.Lock()
        self.task_id = task_id
        self.query = query
        self.sources = sources
        self.params = params
        self.status = status
        self.posts_written = 0
        self.comments_written = 0
        self.per_source: Dict[str, Dict[str, Any]] = {
            s: {"status": "pending", "posts": 0, "comments": 0} for s in sources
        }
        self.errors: List[Dict[str, str]] = []
        self.warnings: List[Dict[str, str]] = []
        self.completed_at: Optional[str] = None
        self._cancel = threading.Event()
        self._done = False
        if persist_now:
            self._flush()

    def cancelled(self) -> bool:
        return self._cancel.is_set()

    def request_cancel(self) -> None:
        self._cancel.set()

    def _snapshot_unlocked(self) -> Dict[str, Any]:
        """Build a snapshot. Caller must hold self.lock (or be single-threaded)."""
        return {
            "task_id": self.task_id,
            "status": self.status,
            "query": self.query,
            "sources": list(self.sources),
            "params": dict(self.params),
            "progress": {
                "posts_written": self.posts_written,
                "comments_written": self.comments_written,
            },
            "per_source": {k: dict(v) for k, v in self.per_source.items()},
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "completed_at": self.completed_at,
        }

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return self._snapshot_unlocked()

    def _flush(self) -> None:
        """Copy state under lock, then write Mongo outside the lock (avoids deadlock)."""
        with self.lock:
            doc = self._snapshot_unlocked()
        try:
            save_crawl_task(doc)
        except Exception as exc:
            logger.warning("save_crawl_task failed task=%s: %s", self.task_id, exc)

    def set_source_status(self, name: str, status: str) -> None:
        with self.lock:
            if self._done:
                return
            if name in self.per_source:
                self.per_source[name]["status"] = status
        logger.info("task=%s source=%s status=%s", self.task_id, name, status)
        self._flush()

    def add_error(self, source: str, code: str, message: str) -> None:
        with self.lock:
            if self._done:
                return
            self.errors.append({"source": source, "code": code, "message": message})
            if source in self.per_source:
                self.per_source[source]["status"] = "failed"
        logger.error("task=%s source=%s error=%s msg=%s", self.task_id, source, code, message)
        self._flush()

    def add_warning(self, source: str, code: str, message: str) -> None:
        with self.lock:
            if self._done:
                return
            self.warnings.append({"source": source, "code": code, "message": message})
        logger.warning("task=%s source=%s warn=%s msg=%s", self.task_id, source, code, message)
        self._flush()

    def bump(self, name: str, kind: str) -> None:
        should_flush = False
        with self.lock:
            if self._done:
                return
            if kind == "post":
                self.posts_written += 1
                if name in self.per_source:
                    self.per_source[name]["posts"] += 1
                count = self.per_source.get(name, {}).get("posts", 0)
            else:
                self.comments_written += 1
                if name in self.per_source:
                    self.per_source[name]["comments"] += 1
                count = self.per_source.get(name, {}).get("comments", 0)
            total = self.posts_written + self.comments_written
            should_flush = total % 10 == 0
        logger.info(
            "task=%s source=%s new_%s total_for_source=%s posts=%s comments=%s",
            self.task_id,
            name,
            kind,
            count,
            self.posts_written,
            self.comments_written,
        )
        if should_flush:
            self._flush()

    def finish(self, status: str) -> None:
        with self.lock:
            if self._done:
                return
            self._done = True
            self.status = status
            self.completed_at = (
                datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            )
        logger.info(
            "task=%s finished status=%s posts=%s comments=%s errors=%s",
            self.task_id,
            status,
            self.posts_written,
            self.comments_written,
            len(self.errors),
        )
        self._flush()


def create_accepted_task(
    task_id: str,
    query: str,
    source: Optional[str],
    time_delta: Any,
    limit: int,
) -> List[str]:
    """Write crawl_tasks row immediately so GET works before workers start."""
    adapters = resolve_adapters(source)
    time_filter = normalize_time_delta(time_delta)
    logger.info(
        "task=%s accepted query=%r source=%s adapters=%s limit=%s",
        task_id,
        query,
        source,
        adapters,
        limit,
    )
    TaskState(
        task_id,
        query,
        adapters,
        {
            "source": source,
            "time_delta": time_delta,
            "time_filter": time_filter,
            "limit": limit,
            "search_modes": {
                "x_playwright": ["top", "live"],
                "reddit_playwright": ["relevance"],
            },
            "min_comments_fresh": _MIN_COMMENTS_FRESH,
        },
        status="accepted",
        persist_now=True,
    )
    return adapters


def _make_on_item(task: TaskState, adapter_name: str) -> Callable[[str, dict], None]:
    publisher = get_publisher()

    def on_item(kind: str, row: dict) -> None:
        if task.cancelled() or task._done:
            return
        try:
            if kind == "post":
                row = dict(row)
                row["query"] = task.query
                external_id = external_id_for_post(row)
                source = (row.get("source") or "").strip()
                if not source or not external_id:
                    return
                doc = {
                    "query": task.query,
                    "source": source,
                    "external_id": external_id,
                    "task_id": task.task_id,
                    "payload": dict(row),
                    **{k: row.get(k) for k in ("url", "title", "text", "text_html", "author", "published_ts", "crawled_at", "engagement")},
                }
                persist_raw_posts([doc])
                task.bump(adapter_name, "post")
                event = post_event_from_row(row, task.task_id)
                if event:
                    ok, reason = validate_event(event)
                    if not ok:
                        task.add_warning(adapter_name, "sqs_invalid_event", reason)
                    else:
                        try:
                            publisher.publish_post(event)
                        except Exception as exc:
                            task.add_warning(adapter_name, "sqs_error", str(exc))
            elif kind == "comment":
                row = dict(row)
                row["query"] = task.query
                external_id = external_id_for_comment(row)
                source = (row.get("source") or "").strip()
                if not source or not external_id:
                    return
                doc = {
                    "query": task.query,
                    "source": source,
                    "external_id": external_id,
                    "task_id": task.task_id,
                    "payload": dict(row),
                    **{
                        k: row.get(k)
                        for k in (
                            "comment_id",
                            "parent_content_id",
                            "parent_content_url",
                            "parent_comment_id",
                            "author",
                            "text",
                            "like_count",
                            "reply_count",
                            "published_ts",
                            "crawled_at",
                        )
                    },
                }
                persist_raw_comments([doc])
                task.bump(adapter_name, "comment")
                event = comment_event_from_row(row, task.task_id)
                if event:
                    ok, reason = validate_event(event)
                    if not ok:
                        task.add_warning(adapter_name, "sqs_invalid_event", reason)
                    else:
                        try:
                            publisher.publish_comment(event)
                        except Exception as exc:
                            task.add_warning(adapter_name, "sqs_error", str(exc))
        except Exception as exc:
            logger.exception("on_item failed source=%s: %s", adapter_name, exc)
            # Persist failure is a real error — do not report the crawl as completed success
            task.add_error(adapter_name, "persist_error", str(exc))

    return on_item


def _post_id_key(row: dict) -> str:
    """Stable id for in-memory dedupe across search modes."""
    post_id = (row.get("post_id") or "").strip()
    if ":" in post_id:
        post_id = post_id.rsplit(":", 1)[-1]
    if post_id:
        return post_id
    return (row.get("url") or "").strip()


def _dedupe_posts(posts: List[dict]) -> List[dict]:
    seen: set[str] = set()
    unique: List[dict] = []
    for row in posts:
        key = _post_id_key(row)
        if not key or key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique


def _fetch_x_multi_mode(
    query: str,
    limit: int,
    time_filter: Optional[str],
    on_item: Callable[[str, dict], None],
) -> None:
    from sources import x_playwright
    from sources.utils import time_filter_since_date

    since = time_filter_since_date(time_filter) if time_filter else None
    modes = ("top", "live")
    collected: List[dict] = []

    for filt in modes:
        kwargs: Dict[str, Any] = {
            "search_filter": filt,
            "since": since,
            "include_comments": False,
            "limit": limit,
        }
        if filt == "live":
            kwargs["min_replies"] = _MIN_COMMENTS_FRESH
        result = x_playwright.fetch(query, **kwargs)
        posts, _ = _normalize_fetch_result(result)
        collected.extend(posts)

    unique = _dedupe_posts(collected)[:limit]
    logger.info(
        "x_playwright multi-mode modes=%s collected=%d unique=%d limit=%d",
        list(modes),
        len(collected),
        len(unique),
        limit,
    )
    x_playwright.enrich_posts_with_comments(unique, on_item=on_item)


def _fetch_reddit_multi_mode(
    query: str,
    limit: int,
    time_filter: Optional[str],
    on_item: Callable[[str, dict], None],
) -> None:
    from sources import reddit_playwright

    # Query path: relevance-only (hot/top/new search ranks weakly for intent match).
    result = reddit_playwright.fetch(
        query,
        limit=limit,
        sort="relevance",
        time_filter=time_filter,
        listing_only=True,
    )
    posts, _ = _normalize_fetch_result(result)
    unique = _dedupe_posts(posts)[:limit]
    logger.info(
        "reddit_playwright relevance-only collected=%d unique=%d limit=%d",
        len(posts),
        len(unique),
        limit,
    )
    reddit_playwright.enrich_posts_with_comments(
        unique, include_comments=True, on_item=on_item
    )


def _call_adapter(
    name: str,
    query: str,
    limit: int,
    time_filter: Optional[str],
    on_item: Callable[[str, dict], None],
) -> None:
    meta = REGISTRY[name]
    fetch = meta["fetch"]
    days = time_filter_to_days(time_filter)

    if name == "x_playwright":
        _fetch_x_multi_mode(query, limit, time_filter, on_item)
        return

    if name == "reddit_playwright":
        _fetch_reddit_multi_mode(query, limit, time_filter, on_item)
        return

    if name == "youtube_api":
        result = fetch(
            query,
            limit=limit,
            published_after_days=days,
            include_comments=True,
            on_item=on_item,
        )
        _ = result
        return

    if name == "duckduckgo_web":
        # map week→w etc.
        tl_map = {"day": "d", "week": "w", "month": "m", "year": "y", "hour": "d"}
        timelimit = tl_map.get(time_filter or "", "w")
        result = fetch(query, limit=limit, timelimit=timelimit)
        posts, comments = _normalize_fetch_result(result)
        posts = filter_posts_by_time_filter(posts, time_filter)
        for row in posts:
            on_item("post", row)
        for row in comments:
            on_item("comment", row)
        return

    if name == "newsapi":
        result = fetch(query, limit=limit, days=days or 7)
    else:
        # news / rss / official reddit
        try:
            result = fetch(query, limit=limit)
        except TypeError:
            result = fetch(query, limit)

    posts, comments = _normalize_fetch_result(result)
    posts = filter_posts_by_time_filter(posts, time_filter)
    for row in posts:
        on_item("post", row)
    for row in comments:
        on_item("comment", row)


def _run_one_adapter(task: TaskState, name: str, query: str, limit: int, time_filter: Optional[str]) -> None:
    if task.cancelled():
        task.set_source_status(name, "cancelled")
        return

    task.set_source_status(name, "running")
    on_item = _make_on_item(task, name)
    sem = None
    if name == "x_playwright":
        sem = _x_slots
    elif name == "reddit_playwright":
        sem = _reddit_slots

    try:
        if sem is not None:
            sem.acquire()
        try:
            if task.cancelled():
                task.set_source_status(name, "cancelled")
                return
            _call_adapter(name, query, limit, time_filter, on_item)
            if task.cancelled():
                task.set_source_status(name, "cancelled")
            else:
                with task.lock:
                    already_failed = task.per_source.get(name, {}).get("status") == "failed"
                if not already_failed:
                    task.set_source_status(name, "completed")
                    logger.info("task=%s source=%s crawl finished OK", task.task_id, name)
        finally:
            if sem is not None:
                sem.release()
    except SessionExpiredError as exc:
        notify_session_expired(exc.source or name, exc.message)
        task.add_error(name, "session_expired", exc.message)
    except Exception as exc:
        logger.exception("adapter %s failed: %s", name, exc)
        msg = str(exc)
        if "login wall" in msg.lower() or "session" in msg.lower() and "must be set" in msg.lower():
            notify_session_expired(name, msg)
            task.add_error(name, "session_expired", msg)
        else:
            task.add_error(name, "adapter_error", msg)


def run_crawl_task(
    task_id: str,
    query: str,
    source: Optional[str],
    time_delta: Any,
    limit: int,
) -> None:
    adapters = resolve_adapters(source)
    time_filter = normalize_time_delta(time_delta)
    logger.info(
        "task=%s starting workers adapters=%s time_filter=%s limit=%s",
        task_id,
        adapters,
        time_filter,
        limit,
    )
    task = TaskState(
        task_id,
        query,
        adapters,
        {
            "source": source,
            "time_delta": time_delta,
            "time_filter": time_filter,
            "limit": limit,
            "search_modes": {
                "x_playwright": ["top", "live"],
                "reddit_playwright": ["relevance"],
            },
            "min_comments_fresh": _MIN_COMMENTS_FRESH,
        },
        status="running",
        persist_now=True,
    )

    deadline = time.monotonic() + max(60, CRAWL_TASK_TIMEOUT_SEC)
    futures = []
    for name in adapters:
        if time.monotonic() > deadline:
            task.request_cancel()
            break
        logger.info("task=%s queue source=%s", task_id, name)
        futures.append(
            _executor.submit(_run_one_adapter, task, name, query, limit, time_filter)
        )

    timed_out = False
    try:
        for fut in as_completed(futures, timeout=max(1, int(deadline - time.monotonic()))):
            if time.monotonic() > deadline:
                task.request_cancel()
                timed_out = True
                break
            try:
                fut.result()
            except Exception as exc:
                logger.exception("task=%s worker future failed: %s", task_id, exc)
    except TimeoutError:
        timed_out = True
        task.request_cancel()
        for fut in futures:
            fut.cancel()
        logger.warning("task=%s timed out after %ss", task_id, CRAWL_TASK_TIMEOUT_SEC)

    if timed_out:
        task.add_error("_task", "timeout", f"task exceeded {CRAWL_TASK_TIMEOUT_SEC}s")

    snap = task.snapshot()
    wrote = snap["progress"]["posts_written"] or snap["progress"]["comments_written"]
    if timed_out or task.cancelled():
        task.finish("completed_with_errors" if wrote or snap["errors"] else "failed")
    elif snap["errors"]:
        task.finish("completed_with_errors" if wrote else "failed")
    else:
        task.finish("completed")


def submit_crawl(
    task_id: str,
    query: str,
    source: Optional[str],
    time_delta: Any,
    limit: int,
) -> None:
    """Start crawl on a daemon thread; adapters use the shared ThreadPoolExecutor."""
    threading.Thread(
        target=run_crawl_task,
        args=(task_id, query, source, time_delta, limit),
        daemon=True,
        name=f"crawl-{task_id[:8]}",
    ).start()
