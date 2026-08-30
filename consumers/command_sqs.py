"""SQS command consumer — long-polls Pace-Unit crawl commands into the orchestrator."""
from __future__ import annotations

import json
import logging
import re
import threading
import uuid
from typing import Any, Dict, Optional

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_REGION,
    AWS_SECRET_ACCESS_KEY,
    AWS_SQS_COMMAND_QUEUE_URL,
    DEFAULT_FETCH_LIMIT,
    SQS_COMMAND_CONSUMER_ENABLED,
    SQS_WAIT_TIME_SECONDS,
)
logger = logging.getLogger(__name__)


def resolve_adapters(source):
    from sources import resolve_adapters as resolve

    return resolve(source)


def normalize_time_delta(value):
    from orchestrator import normalize_time_delta as normalize

    return normalize(value)


def get_crawl_task(task_id):
    from persist import get_crawl_task as get_task

    return get_task(task_id)


def create_accepted_task(*args):
    from orchestrator import create_accepted_task as create

    return create(*args)


def submit_crawl(*args):
    from orchestrator import submit_crawl as submit

    return submit(*args)


def create_accepted_influencer_task(*args, **kwargs):
    from influencer_orchestrator import create_accepted_influencer_task as create

    return create(*args, **kwargs)


def submit_influencer_task(*args, **kwargs):
    from influencer_orchestrator import submit_influencer_task as submit

    return submit(*args, **kwargs)


def _influencer_request(cmd: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: cmd[key]
        for key in (
            "company_id",
            "company_name",
            "company_domain",
            "company_summary",
            "field",
            "related_terms",
            "platform",
            "limit",
            "resume",
        )
    }


def _sqs_client():
    import boto3

    kwargs = {"region_name": AWS_REGION}
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        kwargs["aws_access_key_id"] = AWS_ACCESS_KEY_ID
        kwargs["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
    return boto3.client("sqs", **kwargs)


def _parse_command(body: Dict[str, Any]) -> Dict[str, Any]:
    task_type = (body.get("task_type") or "content_crawl").strip().lower()
    if task_type not in ("content_crawl", "influencer_discovery"):
        raise ValueError(
            "task_type must be content_crawl or influencer_discovery"
        )
    params = body.get("input", body)
    if not isinstance(params, dict):
        raise ValueError("input must be an object")

    job_id = (body.get("job_id") or "").strip() or str(uuid.uuid4())
    if task_type == "influencer_discovery":
        required = (
            "company_id",
            "company_name",
            "company_domain",
            "company_summary",
            "field",
            "related_terms",
            "platform",
            "limit",
        )
        missing = [name for name in required if name not in params]
        if missing:
            raise ValueError(f"influencer request missing required fields: {', '.join(missing)}")
        company_id = str(params.get("company_id") or "").strip()
        company_name = " ".join(str(params.get("company_name") or "").split())
        company_domain = str(params.get("company_domain") or "").strip().lower()
        company_summary = " ".join(str(params.get("company_summary") or "").split())
        field = " ".join(str(params.get("field") or "").split())
        platform = str(params.get("platform") or "").strip().lower()
        related_terms = params.get("related_terms")
        if not company_id:
            raise ValueError("company_id is required")
        if not company_name:
            raise ValueError("company_name is required")
        if not re.fullmatch(r"[a-z0-9.-]+", company_domain):
            raise ValueError("company_domain must be a valid domain")
        if not company_summary:
            raise ValueError("company_summary is required")
        if not field:
            raise ValueError("field is required")
        if not isinstance(related_terms, list) or not related_terms:
            raise ValueError("related_terms must be a non-empty array")
        related_terms = [" ".join(str(term).split()) for term in related_terms if str(term).strip()]
        if not related_terms:
            raise ValueError("related_terms must contain at least one non-empty term")
        if platform != "x":
            raise ValueError("only platform x is currently implemented")
        try:
            limit = int(params["limit"])
        except (TypeError, ValueError) as exc:
            raise ValueError("limit must be an integer") from exc
        if limit < 1 or limit > 100:
            raise ValueError("limit must be between 1 and 100")
        return {
            "job_id": job_id,
            "task_type": task_type,
            "company_id": company_id,
            "company_name": company_name,
            "company_domain": company_domain,
            "company_summary": company_summary,
            "field": field,
            "related_terms": related_terms,
            "platform": platform,
            "limit": max(1, min(limit, 100)),
            "resume": bool(params.get("resume", False)),
        }

    query = (params.get("query") or "").strip()
    if not query:
        raise ValueError("query is required")
    source = params.get("source")
    time_delta = params.get("time_delta")
    try:
        limit = int(params.get("limit", DEFAULT_FETCH_LIMIT))
    except (TypeError, ValueError) as exc:
        raise ValueError("limit must be an integer") from exc
    limit = max(1, min(limit, 100))
    resolve_adapters(source)
    normalize_time_delta(time_delta)
    return {
        "job_id": job_id,
        "task_type": task_type,
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
        if (
            cmd["task_type"] == "influencer_discovery"
            and cmd.get("resume")
            and existing.get("status") in {
                "failed",
                "stopped_x_rate_limit",
                "stopped_x_access_errors",
            }
        ):
            submit_influencer_task(task_id, **_influencer_request(cmd))
            logger.info(
                "event=influencer.resume_submitted task_id=%s run_id=%s company_id=%s platform=%s "
                "step=resume status=submitted previous_status=%s",
                task_id,
                task_id,
                cmd["company_id"],
                cmd["platform"],
                existing.get("status"),
            )
            return task_id
        logger.info(
            "[CommandSQS] skip duplicate task_id=%s status=%s",
            task_id,
            existing.get("status"),
        )
        return task_id
    if cmd["task_type"] == "influencer_discovery":
        request = _influencer_request(cmd)
        create_accepted_influencer_task(task_id, **request)
        submit_influencer_task(task_id, **request)
        logger.info(
            "event=influencer.command_accepted task_id=%s run_id=%s company_id=%s platform=%s "
            "step=command_accept status=accepted field=%r limit=%s",
            task_id,
            task_id,
            cmd["company_id"],
            cmd["platform"],
            cmd["field"],
            cmd["limit"],
        )
        logger.debug(
            "influencer command terms task_id=%s company_id=%s terms=%s",
            task_id,
            cmd["company_id"],
            len(cmd["related_terms"]),
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
