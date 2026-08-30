#!/usr/bin/env python3
"""Live end-to-end smoke test for the influencer_discovery workflow.

This test deliberately uses no mocks. It exercises the real command parser,
task routing, X discovery pipeline, MongoDB persistence, and task tracking.

Required environment variables are the same as the running service:
  X_AUTH_TOKEN, X_CT0, MONGODB_URI, MONGODB_DBNAME

Run from the project root:
  .venv/bin/python tests/e2e_influencer_pipeline.py
"""

from __future__ import annotations

import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from consumers.command_sqs import handle_command_message  # noqa: E402
from persist import get_crawl_task, get_mongo_db  # noqa: E402


TOPIC = "Marketing"
LIMIT = 10
TIMEOUT_SECONDS = int(os.getenv("E2E_INFLUENCER_TIMEOUT_SECONDS", "1800"))
POLL_SECONDS = float(os.getenv("E2E_INFLUENCER_POLL_SECONDS", "5"))


def _require_environment() -> None:
    required = ("X_AUTH_TOKEN", "X_CT0", "MONGODB_URI", "MONGODB_DBNAME")
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise RuntimeError(
            "Missing required live-test environment variables: "
            + ", ".join(missing)
        )


def _wait_for_task(task_id: str) -> dict:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        task = get_crawl_task(task_id)
        if task:
            status = task.get("status")
            if status in {"completed", "completed_with_errors", "failed"}:
                return task
            progress = task.get("progress") or {}
            print(
                f"status={status!r} "
                f"influencers_written={progress.get('influencers_written', 0)}",
                flush=True,
            )
        time.sleep(POLL_SECONDS)
    raise TimeoutError(
        f"Task {task_id} did not finish within {TIMEOUT_SECONDS} seconds"
    )


def _verify_database(task_id: str, task: dict, started_at: datetime) -> int:
    progress = task.get("progress") or {}
    written = int(progress.get("influencers_written") or 0)
    if written < 1 or written > LIMIT:
        raise AssertionError(
            f"Expected 1..{LIMIT} influencers_written, got {written}"
        )

    collection = get_mongo_db()["influencers"]
    documents = list(
        collection.find(
            {
                "platform": "x",
                "topics": TOPIC,
                "updated_at": {"$gte": started_at},
            },
            {
                "_id": 1,
                "platform": 1,
                "handle": 1,
                "name": 1,
                "followers_count": 1,
                "following_count": 1,
                "topics": 1,
            },
        )
    )
    task_handles = {
        item.get("handle")
        for item in documents
        if item.get("handle")
    }

    # The influencer collection is intentionally upserted by handle. The
    # updated_at filter ties the MongoDB check to this run, including records
    # whose handles already existed before the test. Do not delete records.
    if not task_handles:
        raise AssertionError(
            "The task reported influencers, but no Marketing influencer "
            "documents were found in MongoDB"
        )
    for document in documents:
        if document.get("platform") != "x":
            raise AssertionError(f"Unexpected influencer document: {document}")
        if not document.get("handle"):
            raise AssertionError(f"Influencer document has no handle: {document}")

    print(
        f"MongoDB verification: found {len(documents)} Marketing X influencer "
        f"documents; task_id={task_id}",
        flush=True,
    )
    return len(documents)


def main() -> int:
    _require_environment()
    task_id = f"e2e-influencer-marketing-{uuid.uuid4().hex[:12]}"
    started_at = datetime.now(timezone.utc)
    command = {
        "job_id": task_id,
        "task_type": "influencer_discovery",
        "input": {"topic": TOPIC, "limit": LIMIT},
    }

    print(f"Submitting live command: {command}", flush=True)
    accepted_task_id = handle_command_message(command)
    if accepted_task_id != task_id:
        raise AssertionError(
            f"Expected task_id {task_id}, received {accepted_task_id}"
        )

    task = _wait_for_task(task_id)
    print(
        f"Final task status={task.get('status')!r} "
        f"progress={task.get('progress')!r}",
        flush=True,
    )
    if task.get("status") != "completed":
        raise AssertionError(
            f"Influencer workflow did not complete successfully: "
            f"status={task.get('status')!r}, errors={task.get('errors')!r}"
        )

    count = _verify_database(task_id, task, started_at)
    print(
        f"PASS: influencer_discovery completed and wrote to MongoDB "
        f"(task_id={task_id}, task_count={task['progress']['influencers_written']}, "
        f"matching_documents={count})",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
