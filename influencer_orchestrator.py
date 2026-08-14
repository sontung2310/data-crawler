"""Influencer discovery task orchestration."""
from __future__ import annotations

import logging
import threading

from config import (
    BASE_DIR,
    X_AUTH_TOKEN,
    X_CT0,
)
from app.alerts import notify_session_expired
from events import influencer_event_from_row, validate_event
from exceptions import SessionExpiredError
from orchestrator import TaskState
from persist import persist_influencer
from publishers import get_publisher
from x_influencer_discovery.config import Settings
from x_influencer_discovery.pipeline import run
from x_resource import x_session_slot

logger = logging.getLogger(__name__)
SOURCE = "x_influencer_discovery"


def _settings() -> Settings:
    session = None
    if X_AUTH_TOKEN and X_CT0:
        session = f"auth_token={X_AUTH_TOKEN}; ct0={X_CT0}"
    return Settings(
        mongodb_url=None,
        classification_backend="none",
        x_session=session,
        embedding_enabled=False,
        headless=True,
        data_dir=BASE_DIR / "data" / "influencers",
    )


def create_accepted_influencer_task(task_id: str, topic: str, limit: int) -> None:
    TaskState(
        task_id,
        topic,
        [SOURCE],
        {
            "task_type": "influencer_discovery",
            "topic": topic,
            "limit": limit,
        },
        status="accepted",
        persist_now=True,
    )


def run_influencer_task(task_id: str, topic: str, limit: int) -> None:
    task = TaskState(
        task_id,
        topic,
        [SOURCE],
        {
            "task_type": "influencer_discovery",
            "topic": topic,
            "limit": limit,
        },
        status="running",
        persist_now=True,
    )
    task.set_source_status(SOURCE, "running")

    try:
        if not (X_AUTH_TOKEN and X_CT0):
            raise SessionExpiredError(
                SOURCE,
                "X_AUTH_TOKEN and X_CT0 must be set for influencer discovery",
            )
        output = run(
            topic,
            limit,
            _settings(),
            persist_output=False,
            x_slot_factory=x_session_slot,
        )

        publisher = get_publisher()
        for item in output.get("results") or []:
            followers = item.get("followers") or {}
            following = item.get("following") or {}
            row = {
                "source": SOURCE,
                "topic": topic,
                "name": item.get("name"),
                "handle": (item.get("handle") or "").strip().lstrip("@").lower(),
                "bio": item.get("bio"),
                "profile_img_url": item.get("profile_img_url"),
                "followers_count": followers.get("estimated"),
                "following_count": following.get("estimated"),
                "task_id": task_id,
            }
            if not row["handle"]:
                continue
            persist_influencer(row)
            task.bump(SOURCE, "influencer")

            event = influencer_event_from_row(row, task_id)
            if event:
                ok, reason = validate_event(event)
                if not ok:
                    task.add_warning(SOURCE, "sqs_invalid_event", reason)
                else:
                    try:
                        publisher.publish_influencer(event)
                    except Exception as exc:
                        task.add_warning(SOURCE, "sqs_error", str(exc))

        task.set_source_status(SOURCE, "completed")
        task.finish("completed")
    except SessionExpiredError as exc:
        notify_session_expired(exc.source or SOURCE, exc.message)
        task.add_error(SOURCE, "session_expired", exc.message)
        task.finish("failed")
    except Exception as exc:
        logger.exception("influencer discovery failed task=%s: %s", task_id, exc)
        task.add_error(SOURCE, "influencer_error", str(exc))
        wrote = task.snapshot()["progress"]["influencers_written"]
        task.finish("completed_with_errors" if wrote else "failed")


def submit_influencer_task(task_id: str, topic: str, limit: int) -> None:
    threading.Thread(
        target=run_influencer_task,
        args=(task_id, topic, limit),
        daemon=True,
        name=f"influencer-{task_id[:8]}",
    ).start()
