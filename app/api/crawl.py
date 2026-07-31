"""Crawl HTTP routes."""
from __future__ import annotations

import uuid
from typing import Optional

from fastapi import APIRouter, Header, HTTPException

from app.schemas import CrawlAccepted, CrawlRequest, CrawlStatus
from config import CRAWL_API_KEY, DEFAULT_FETCH_LIMIT
from orchestrator import create_accepted_task, normalize_time_delta, submit_crawl
from persist import get_crawl_task
from sources import list_sources, resolve_adapters

router = APIRouter()


def _check_api_key(x_api_key: Optional[str]) -> None:
    if not CRAWL_API_KEY:
        return
    if (x_api_key or "").strip() != CRAWL_API_KEY:
        raise HTTPException(status_code=401, detail="invalid or missing X-API-Key")


@router.get("/health")
def health():
    return {"status": "ok"}


@router.get("/sources")
def sources():
    return list_sources()


@router.post("/crawl", response_model=CrawlAccepted)
def start_crawl(
    body: CrawlRequest,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
):
    _check_api_key(x_api_key)
    query = (body.query or "").strip()
    if not query:
        raise HTTPException(status_code=422, detail="query is required")

    try:
        resolve_adapters(body.source)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    try:
        normalize_time_delta(body.time_delta)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    task_id = str(uuid.uuid4())
    limit = body.limit if body.limit is not None else DEFAULT_FETCH_LIMIT
    try:
        create_accepted_task(task_id, query, body.source, body.time_delta, limit)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"cannot create task (Mongo?): {exc}") from exc
    submit_crawl(task_id, query, body.source, body.time_delta, limit)
    return CrawlAccepted(task_id=task_id, status="accepted")


@router.get("/crawl/{task_id}", response_model=CrawlStatus)
def crawl_status(
    task_id: str,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
):
    _check_api_key(x_api_key)
    doc = get_crawl_task(task_id)
    if not doc:
        raise HTTPException(status_code=404, detail="task not found")
    return CrawlStatus(
        task_id=doc["task_id"],
        status=doc.get("status") or "unknown",
        query=doc.get("query") or "",
        sources=doc.get("sources") or [],
        progress=doc.get("progress") or {"posts_written": 0, "comments_written": 0},
        per_source=doc.get("per_source") or {},
        errors=doc.get("errors") or [],
        warnings=doc.get("warnings") or [],
        completed_at=doc.get("completed_at"),
        params=doc.get("params"),
    )
