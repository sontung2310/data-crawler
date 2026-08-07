"""SQS command consumer — long-polls Pace-Unit crawl commands into the orchestrator."""
from __future__ import annotations

import json
import logging
import threading
import uuid
from typing import Any, Dict, Optional

import boto3

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_REGION,
    AWS_SECRET_ACCESS_KEY,
    AWS_SQS_COMMAND_QUEUE_URL,
    DEFAULT_FETCH_LIMIT,
    SQS_COMMAND_CONSUMER_ENABLED,
    SQS_WAIT_TIME_SECONDS,
)
from orchestrator import create_accepted_task, normalize_time_delta, submit_crawl
from persist import get_crawl_task
from sources import resolve_adapters

logger = logging.getLogger(__name__)


def _sqs_client():
    kwargs = {"region_name": AWS_REGION}
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        kwargs["aws_access_key_id"] = AWS_ACCESS_KEY_ID
        kwargs["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
    return boto3.client("sqs", **kwargs)


def _parse_command(body: Dict[str, Any]) -> Dict[str, Any]:
    query = (body.get("query") or "").strip()
    if not query:
        raise ValueError("query is required")
    source = body.get("source")
    time_delta = body.get("time_delta")
    try:
        limit = int(body.get("limit", DEFAULT_FETCH_LIMIT))
    except (TypeError, ValueError) as exc:
        raise ValueError("limit must be an integer") from exc
    limit = max(1, min(limit, 100))
    resolve_adapters(source)
    normalize_time_delta(time_delta)
    job_id = (body.get("job_id") or "").strip() or str(uuid.uuid4())
    return {
        "job_id": job_id,
        "query": query,
        "source": source,
        "time_delta": time_delta,
        "limit": limit,
    }


def handle_command_message(body: Dict[str, Any]) -> str:
    """Accept + submit crawl; returns task_id. Idempotent on job_id."""
    cmd = _parse_command(body)
    task_id = cmd["job_id"]
    existing = get_crawl_task(task_id)
    if existing:
        logger.info(
            "[CommandSQS] skip duplicate task_id=%s status=%s",
            task_id,
            existing.get("status"),
        )
        return task_id
    create_accepted_task(task_id, cmd["query"], cmd["source"], cmd["time_delta"], cmd["limit"])
    submit_crawl(task_id, cmd["query"], cmd["source"], cmd["time_delta"], cmd["limit"])
    logger.info(
        "[CommandSQS] accepted task_id=%s query=%r source=%s limit=%s",
        task_id,
        cmd["query"],
        cmd["source"],
        cmd["limit"],
    )
    return task_id


def _loop() -> None:
    url = AWS_SQS_COMMAND_QUEUE_URL
    wait = max(0, min(20, int(SQS_WAIT_TIME_SECONDS)))
    client = _sqs_client()
    logger.info("[CommandSQS] consumer started url=%s wait=%ss", url, wait)
    while True:
        try:
            resp = client.receive_message(
                QueueUrl=url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=wait,
                VisibilityTimeout=60,
            )
            for msg in resp.get("Messages") or []:
                handle = msg["ReceiptHandle"]
                raw = msg.get("Body") or "{}"
                try:
                    body = json.loads(raw)
                    handle_command_message(body)
                    client.delete_message(QueueUrl=url, ReceiptHandle=handle)
                    logger.info("[CommandSQS] deleted message_id=%s", msg.get("MessageId"))
                except Exception:
                    logger.exception(
                        "[CommandSQS] failed message_id=%s — leaving for retry/DLQ",
                        msg.get("MessageId"),
                    )
        except Exception:
            logger.exception("[CommandSQS] receive loop error; retrying")


def start_command_consumer() -> Optional[threading.Thread]:
    """Start daemon thread if enabled and configured. Returns thread or None."""
    if not SQS_COMMAND_CONSUMER_ENABLED:
        logger.info("[CommandSQS] consumer disabled (SQS_COMMAND_CONSUMER_ENABLED=0)")
        return None
    if not AWS_SQS_COMMAND_QUEUE_URL:
        logger.warning("[CommandSQS] AWS_SQS_COMMAND_QUEUE_URL not set — consumer not started")
        return None
    if not (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY and AWS_REGION):
        logger.warning("[CommandSQS] AWS credentials incomplete — consumer not started")
        return None
    t = threading.Thread(target=_loop, name="sqs-command-consumer", daemon=True)
    t.start()
    return t
