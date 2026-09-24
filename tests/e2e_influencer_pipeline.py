#!/usr/bin/env python3
"""Live end-to-end run for the influencer_discovery workflow.

This script uses no mocks. It submits the current command through
``handle_command_message``, which starts the real X discovery pipeline,
shared FIFO queue, and MongoDB candidate store.

Required environment, same as the service:
  X_AUTH_TOKEN, X_CT0, MONGODB_URI (or MONGODB_URL), MONGODB_DBNAME
  A reachable influencer FIFO (INFLUENCER_SQS_ENDPOINT_URL or AWS SQS)

The X scroll budget is lowered only inside this process so the live run
stays bounded. Ranking, fetch policy, and the queue contract are unchanged.

Run from the project root:
  .venv/bin/python tests/e2e_influencer_pipeline.py
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config  # noqa: E402
from consumers.command_sqs import handle_command_message  # noqa: E402
from persist import get_crawl_task, get_mongo_db  # noqa: E402


FIELD = "Marketing"
RELATED_TERM = "content marketing"
LIMIT = 2
TIMEOUT_SECONDS = int(os.getenv("E2E_INFLUENCER_TIMEOUT_SECONDS", "1800"))
POLL_SECONDS = float(os.getenv("E2E_INFLUENCER_POLL_SECONDS", "5"))
SUCCESS_STATUSES = {
    "completed",
    "completed_with_no_results",
    "stopped_x_access_errors",
    "stopped_x_rate_limit",
}


def _require_environment() -> None:
    required = ("X_AUTH_TOKEN", "X_CT0", "MONGODB_DBNAME")
    missing = [name for name in required if not os.getenv(name)]
    if not (os.getenv("MONGODB_URI") or os.getenv("MONGODB_URL")):
        missing.append("MONGODB_URI")
    if missing:
        raise RuntimeError(
            "Missing required live-test environment variables: " + ", ".join(missing)
        )


def _bound_discovery_budget() -> None:
    """Shorten only this process's X search budget."""
    config.INFLUENCER_X_AUTHORS_PER_QUERY = int(os.getenv("E2E_X_AUTHORS_PER_QUERY", "5"))
    config.INFLUENCER_X_MAX_SCROLLS_PER_QUERY = int(os.getenv("E2E_X_MAX_SCROLLS_PER_QUERY", "1"))
    config.INFLUENCER_ENABLE_SNOWBALL = False


def _wait_for_task(task_id: str) -> dict:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        task = get_crawl_task(task_id)
        if task:
            status = task.get("status")
            if status in SUCCESS_STATUSES or status == "failed":
                return task
            progress = task.get("progress") or {}
            print(
                f"status={status!r} "
                f"profiles_fetched={progress.get('profiles_fetched', 0)} "
                f"eligible={progress.get('eligible', 0)}",
                flush=True,
            )
        time.sleep(POLL_SECONDS)
    raise TimeoutError(f"Task {task_id} did not finish within {TIMEOUT_SECONDS} seconds")


def _output_path(task_id: str) -> Path:
    return config.INFLUENCER_DATA_DIR / f"marketing_{task_id}_x_influencers.json"


def _load_report(task_id: str) -> tuple[dict, dict]:
    output_path = _output_path(task_id)
    if not output_path.is_file():
        raise AssertionError(f"Pipeline output was not written: {output_path}")
    output = json.loads(output_path.read_text())
    report_path = output.get("x_fetch_report")
    if not report_path or not Path(report_path).is_file():
        raise AssertionError(f"X fetch report was not written: {report_path}")
    report = json.loads(Path(report_path).read_text())
    return output, report


def _discovered_handles(report: dict) -> list[dict]:
    discovery = report.get("discovery") or {}
    return list(discovery.get("x_post_handles") or []) + list(discovery.get("public_search_handles") or [])


def _verify_report(task_id: str, task: dict) -> dict:
    _output, report = _load_report(task_id)
    discovery = report.get("discovery") or {}
    terms = discovery.get("terms") or []
    if terms != [RELATED_TERM]:
        raise AssertionError(
            f"Discovery terms must be only the submitted related term, got {terms!r}"
        )
    if FIELD.casefold() in {term.casefold() for term in terms}:
        raise AssertionError("field was searched as its own discovery lane")

    handles = _discovered_handles(report)
    if not handles:
        raise AssertionError(
            "X fetch report has no handles from live X Latest or public search: "
            f"discovery={discovery!r}"
        )
    received = int((report.get("queue") or {}).get("received") or 0)
    if received < 1:
        raise AssertionError(f"Expected queue.received > 0, got {received}")

    rejected = (report.get("rejected") or {}).get("handles") or []
    low_score = (report.get("evaluated") or {}).get("low_score") or []
    eligible = (report.get("evaluated") or {}).get("eligible") or []
    retries = (report.get("retry") or {}).get("handles") or []
    if not eligible and not rejected and not low_score and not retries:
        raise AssertionError(
            "Live profiles were discovered but the report has no fetch, rejection, "
            f"or score outcome. status={task.get('status')!r}"
        )
    print(
        f"Report verification: terms={terms} discovered={len(handles)} "
        f"queue_received={received} rejected={len(rejected)} eligible={len(eligible)} "
        f"report_status={report.get('status')!r}",
        flush=True,
    )
    return report


def _verify_candidates(company_id: str) -> int:
    documents = list(
        get_mongo_db()["influencer_candidates"].find(
            {"company_id": company_id, "platform": "x"},
            {
                "_id": 0,
                "company_id": 1,
                "platform": 1,
                "account": 1,
                "evaluation": 1,
            },
        )
    )
    for document in documents:
        breakdown = ((document.get("evaluation") or {}).get("score_breakdown") or {})
        if "frequently_appeared" not in breakdown:
            raise AssertionError(
                f"Eligible candidate is missing frequently_appeared: {document.get('account')}"
            )
        followers = breakdown.get("followers")
        if not isinstance(followers, int) or followers < 0 or followers > 20:
            raise AssertionError(
                f"Follower score {followers!r} is outside the max-20 scale "
                f"for {document.get('account')}"
            )
    print(
        f"MongoDB verification: {len(documents)} influencer_candidates "
        f"for company_id={company_id}",
        flush=True,
    )
    return len(documents)


def main() -> int:
    _require_environment()
    _bound_discovery_budget()
    task_id = f"e2e-influencer-marketing-{uuid.uuid4().hex[:12]}"
    company_id = f"e2e-company-{uuid.uuid4().hex[:12]}"
    command = {
        "job_id": task_id,
        "task_type": "influencer_discovery",
        "input": {
            "company_id": company_id,
            "company_name": "Marketing Eye",
            "company_domain": "marketingeye.com.au",
            "company_summary": "A marketing agency helping brands grow through content marketing.",
            "field": FIELD,
            "related_terms": [RELATED_TERM],
            "platform": "x",
            "limit": LIMIT,
        },
    }

    print(f"Submitting live command: job_id={task_id} company_id={company_id}", flush=True)
    accepted_task_id = handle_command_message(command)
    if accepted_task_id != task_id:
        raise AssertionError(f"Expected task_id {task_id}, received {accepted_task_id}")

    task = _wait_for_task(task_id)
    print(
        f"Final task status={task.get('status')!r} errors={task.get('errors')!r} "
        f"progress={task.get('progress')!r}",
        flush=True,
    )
    if task.get("status") not in SUCCESS_STATUSES:
        raise AssertionError(
            "Influencer workflow did not finish after real browser work: "
            f"status={task.get('status')!r}, errors={task.get('errors')!r}"
        )

    _verify_report(task_id, task)
    count = _verify_candidates(company_id)
    print(
        f"PASS: influencer_discovery completed against live X, SQS, and MongoDB "
        f"(task_id={task_id}, company_id={company_id}, candidates={count})",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
