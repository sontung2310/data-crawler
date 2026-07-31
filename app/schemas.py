"""FastAPI request/response models."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field

from config import DEFAULT_FETCH_LIMIT


class CrawlRequest(BaseModel):
    query: str = Field(..., min_length=1)
    source: Optional[str] = Field(
        default=None,
        description="x|reddit|youtube|duckduckgo|news|all (omit = all)",
    )
    time_delta: Optional[Union[int, str]] = Field(
        default=None,
        description="1|7|30|365 or hour|day|week|month|year",
    )
    limit: int = Field(default=DEFAULT_FETCH_LIMIT, ge=1, le=100)


class CrawlAccepted(BaseModel):
    task_id: str
    status: str = "accepted"


class CrawlStatus(BaseModel):
    task_id: str
    status: str
    query: str
    sources: List[str]
    progress: Dict[str, int]
    per_source: Dict[str, Any]
    errors: List[Dict[str, str]]
    warnings: List[Dict[str, str]]
    completed_at: Optional[str] = None
    params: Optional[Dict[str, Any]] = None
