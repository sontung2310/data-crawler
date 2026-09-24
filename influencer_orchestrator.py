"""Data-Crawler-Task owner for the durable influencer discovery workflow."""
from __future__ import annotations

import logging
import os
import re
import threading
from pathlib import Path
from typing import Any, Iterable

import config
from app.alerts import notify_session_expired
from exceptions import SessionExpiredError
from orchestrator import TaskState
from persist import (
    get_influencer_candidate_evidence,
    get_mongo_db,
    upsert_influencer_candidate_evidence,
)
from x_influencer_discovery.config import Settings
from x_influencer_discovery.models import CandidateEvidence, TopicBrief
from x_influencer_discovery.pipeline import (
    GracefulShutdownRequested,
    PendingQueueWorkError,
    run,
)
from x_influencer_discovery.platforms import account_ref
from x_influencer_discovery.queueing import DurableProfileQueue
from x_influencer_discovery.storage import CandidateStore
from x_influencer_discovery.worker_lock import XWorkerLockUnavailableError

logger = logging.getLogger(__name__)
SOURCE = "x_influencer_discovery"


def _event(
    level: int,
    event: str,
    task_id: str,
    *,
    run_id: str | None = None,
    company_id: str | None = None,
    platform: str = "x",
    step: str | None = None,
    **fields: Any,
) -> None:
    values = {
        "event": event,
        "task_id": task_id,
        "run_id": run_id or task_id,
        "company_id": company_id or "unknown",
        "platform": platform,
    }
    if step is not None:
        values["step"] = step
    values.update(fields)
    rendered = " ".join(f"{key}={_log_value(value)}" for key, value in values.items())
    logger.log(level, rendered)


def _log_value(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if value is None:
        return "none"
    if isinstance(value, (list, tuple, set, dict)):
        return repr(value)
    return str(value).replace(" ", "_")


def _settings() -> Settings:
    """Map DCT environment settings to the reference durable package."""
    if config.INFLUENCER_SQS_MAX_RECEIVE_COUNT != 1:
        raise ValueError(
            "INFLUENCER_SQS_MAX_RECEIVE_COUNT must be exactly 1 for the fast-fail delivery policy"
        )
    session = None
    if config.X_AUTH_TOKEN and config.X_CT0:
        session = f"auth_token={config.X_AUTH_TOKEN}; ct0={config.X_CT0}"
    elif config.X_BROWSER_SESSION:
        session = config.X_BROWSER_SESSION
    return Settings(
        mongodb_url=config.MONGODB_URI,
        database_host=getattr(config, "DASHBOARD_DATABASE_HOST", None),
        database_name=getattr(config, "DASHBOARD_DATABASE_NAME", None),
        database_username=getattr(config, "DASHBOARD_DATABASE_USERNAME", None),
        database_password=getattr(config, "DASHBOARD_DATABASE_PASSWORD", None),
        openai_api_key=config.OPENAI_API_KEY or None,
        openai_model=config.EVAL_MODEL,
        classification_backend=config.INFLUENCER_CLASSIFICATION_BACKEND,
        local_classifier_model=config.INFLUENCER_LOCAL_CLASSIFIER_MODEL,
        local_classifier_device=config.INFLUENCER_LOCAL_CLASSIFIER_DEVICE,
        x_session=session,
        embedding_enabled=True,
        embedding_model=config.INFLUENCER_EMBEDDING_MODEL,
        headless=config.INFLUENCER_HEADLESS,
        data_dir=config.INFLUENCER_DATA_DIR,
        x_authors_per_query=max(1, config.INFLUENCER_X_AUTHORS_PER_QUERY),
        x_max_scrolls_per_query=max(0, config.INFLUENCER_X_MAX_SCROLLS_PER_QUERY),
        public_candidates_per_article=max(1, config.INFLUENCER_PUBLIC_CANDIDATES_PER_ARTICLE),
        embedding_batch_size=max(1, int(os.getenv("EMBEDDING_BATCH_SIZE", "32"))),
        platform="x",
        company_summary=None,
        minimum_followers=max(1, config.INFLUENCER_MINIMUM_FOLLOWERS),
        minimum_relevance_score=config.INFLUENCER_MINIMUM_RELEVANCE_SCORE,
        good_hybrid_relevance_threshold=config.INFLUENCER_GOOD_HYBRID_RELEVANCE_THRESHOLD,
        profile_refresh_after_hours=max(1, config.INFLUENCER_PROFILE_REFRESH_AFTER_HOURS),
        leaderboard_max_age_days=max(1, config.INFLUENCER_LEADERBOARD_MAX_AGE_DAYS),
        # Match the reference package: disabled by default, but honor its
        # ENABLE_SNOWBALL switch when the operator explicitly enables it.
        enable_snowball=config.INFLUENCER_ENABLE_SNOWBALL,
        following_max_scrolls=max(0, config.INFLUENCER_FOLLOWING_MAX_SCROLLS),
        sqs_endpoint_url=config.INFLUENCER_SQS_ENDPOINT_URL or None,
        sqs_queue_name=config.INFLUENCER_SQS_QUEUE_NAME,
        sqs_visibility_timeout_seconds=max(60, config.INFLUENCER_SQS_VISIBILITY_TIMEOUT_SECONDS),
        sqs_receive_batch_size=min(10, max(1, config.INFLUENCER_SQS_RECEIVE_BATCH_SIZE)),
        sqs_receive_wait_time_seconds=min(20, max(0, config.INFLUENCER_SQS_RECEIVE_WAIT_TIME_SECONDS)),
        sqs_idle_poll_seconds=min(60, max(1, config.INFLUENCER_SQS_IDLE_POLL_SECONDS)),
        sqs_max_receive_count=config.INFLUENCER_SQS_MAX_RECEIVE_COUNT,
        account_attempt_timeout_seconds=max(1, config.INFLUENCER_ACCOUNT_ATTEMPT_TIMEOUT_SECONDS),
        playwright_concurrency=1,
        x_worker_lock_path=config.INFLUENCER_WORKER_LOCK_PATH,
        x_fetch_log_dir=config.INFLUENCER_FETCH_LOG_DIR,
        x_access_failure_streak_limit=max(1, config.INFLUENCER_X_ACCESS_FAILURE_STREAK_LIMIT),
    )


class EvidenceLedger:
    """Adapter between package discovery callbacks and DCT Mongo persistence."""

    def record(
        self,
        *,
        task_id: str,
        company_id: str,
        platform: str,
        profile_url: str,
        evidence: dict[str, Any],
    ) -> None:
        reference = account_ref(platform, profile_url=profile_url)
        upsert_influencer_candidate_evidence(
            task_id=task_id,
            company_id=company_id,
            platform=platform,
            account_id=reference.account_id,
            handle=reference.handle,
            evidence=[evidence],
        )

    def get(
        self,
        *,
        task_id: str,
        company_id: str,
        platform: str,
        account_id: str,
    ) -> list[dict[str, Any]]:
        return get_influencer_candidate_evidence(
            task_id=task_id,
            company_id=company_id,
            platform=platform,
            account_id=account_id,
        )


def _normalize_request(
    *,
    company_id: str,
    company_name: str,
    company_domain: str,
    company_summary: str,
    field: str,
    related_terms: Iterable[str],
    platform: str,
    limit: int,
    resume: bool = False,
) -> dict[str, Any]:
    company_id = str(company_id).strip()
    company_name = " ".join(str(company_name).split())
    company_domain = str(company_domain).strip().lower()
    company_summary = " ".join(str(company_summary).split())
    field = " ".join(str(field).split())
    platform = str(platform).strip().lower()
    if not isinstance(related_terms, (list, tuple)):
        raise ValueError("related_terms must be a non-empty array")
    terms = [" ".join(str(term).split()) for term in related_terms if str(term).strip()]
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
    if not terms:
        raise ValueError("related_terms must contain at least one term")
    if platform != "x":
        raise ValueError("only platform x is currently implemented")
    if not 1 <= int(limit) <= 100:
        raise ValueError("limit must be between 1 and 100")

    brief = TopicBrief.build(field, terms)
    return {
        "company_id": company_id,
        "company_name": company_name,
        "company_domain": company_domain,
        "company_summary": company_summary,
        "field": brief.field,
        "related_terms": brief.related_terms,
        "topic_terms": list(brief.related_terms),
        "language": brief.language,
        "platform": platform,
        "limit": int(limit),
        "resume": bool(resume),
    }


def create_accepted_influencer_task(task_id: str, **request: Any) -> None:
    normalized = _normalize_request(**request)
    TaskState(
        task_id,
        normalized["field"],
        [SOURCE],
        {"task_type": "influencer_discovery", **normalized},
        status="accepted",
        persist_now=True,
    )
    _event(
        logging.INFO,
        "influencer.task_accepted",
        task_id,
        company_id=normalized["company_id"],
        platform=normalized["platform"],
        step="request_validation",
        status="accepted",
        terms=len(normalized["topic_terms"]),
        limit=normalized["limit"],
    )


def _output_path(settings: Settings, task_id: str, field: str) -> Path:
    safe_field = re.sub(r"[^a-z0-9]+", "_", field.casefold()).strip("_") or "field"
    return settings.data_dir / f"{safe_field}_{task_id}_x_influencers.json"


def _update_task_metrics(task: TaskState, output: dict[str, Any]) -> None:
    metrics = output.get("metrics") or {}
    queue_metrics = metrics.get("queue") or {}
    processing = metrics.get("processing") or {}
    eligibility = metrics.get("eligibility") or {}
    leaderboard = metrics.get("leaderboard") or {}
    sources = metrics.get("sources") or {}
    task.set_progress(
        discovery_submitted=sum(int(value or 0) for value in sources.values()),
        profiles_fetched=int(processing.get("fetched") or 0),
        candidates_evaluated=int(processing.get("evaluated") or 0),
        eligible=int(eligibility.get("passed") or 0),
        rejected=int(eligibility.get("rejected") or 0),
        retried=int(queue_metrics.get("retried") or 0),
        persisted=int(leaderboard.get("inserted") or 0) + int(leaderboard.get("updated") or 0),
        dead_lettered=int(queue_metrics.get("dead_letter_depth") or 0),
    )


def run_influencer_task(task_id: str, **request: Any) -> None:
    normalized = _normalize_request(**request)
    task = TaskState(
        task_id,
        normalized["field"],
        [SOURCE],
        {"task_type": "influencer_discovery", **normalized},
        status="validating",
        persist_now=True,
    )
    company_id = normalized["company_id"]
    platform = normalized["platform"]
    _event(logging.INFO, "influencer.task_start", task_id, company_id=company_id, platform=platform, status="validating")

    try:
        settings = _settings()
        task.set_source_status(SOURCE, "validating")
        _event(logging.INFO, "influencer.step_start", task_id, company_id=company_id, platform=platform,
               step="topic_normalization", status="running")
        settings = Settings(
            **{
                **settings.__dict__,
                "company_summary": normalized["company_summary"],
                "platform": platform,
            }
        )
        _event(logging.INFO, "influencer.step_result", task_id, company_id=company_id, platform=platform,
               step="topic_normalization", status="completed", terms=len(normalized["topic_terms"]))

        task.set_status("discovering")
        task.set_source_status(SOURCE, "discovering")
        store = CandidateStore(get_mongo_db()[CandidateStore.collection_name])
        queue = DurableProfileQueue.from_settings(
            settings,
            normalized["company_name"],
            company_id=company_id,
            platform=platform,
        )
        evidence_ledger = EvidenceLedger()
        task.set_status("processing")

        def pipeline_event(event: str, **fields: Any) -> None:
            event_task_id = str(fields.pop("task_id", task_id))
            event_run_id = str(fields.pop("run_id", task_id))
            event_company_id = str(fields.pop("company_id", company_id))
            event_platform = str(fields.pop("platform", platform))
            step = fields.pop("step", None)
            status = fields.pop("status", "completed")
            level = logging.WARNING if status == "degraded" else logging.INFO
            _event(
                level,
                event,
                event_task_id,
                run_id=event_run_id,
                company_id=event_company_id,
                platform=event_platform,
                step=step,
                status=status,
                **fields,
            )

        output = run(
            normalized["field"],
            normalized["limit"],
            settings,
            _output_path(settings, task_id, normalized["field"]),
            company_id=company_id,
            company_name=normalized["company_name"],
            company_domain=normalized["company_domain"],
            platform=platform,
            related_terms=normalized["related_terms"],
            candidate_store=store,
            queue=queue,
            evidence_store=evidence_ledger,
            run_id=task_id,
            event_callback=pipeline_event,
            produce_discovery=not normalized["resume"],
        )
        _update_task_metrics(task, output)
        metrics = output.get("metrics") or {}
        sources = metrics.get("sources") or {}
        eligibility = metrics.get("eligibility") or {}
        results = output.get("results") or []
        task.set_source_status(SOURCE, "completed")
        final_status = output.get("status") or ("completed" if results else "completed_with_no_results")
        if final_status == "completed" and not results:
            final_status = "completed_with_no_results"
        task.finish(final_status)
        _event(logging.INFO, "influencer.task_result", task_id, company_id=company_id, platform=platform,
               status=task.status, results=len(results), output=output.get("x_fetch_report", "none"))
    except SessionExpiredError as exc:
        notify_session_expired(exc.source or SOURCE, exc.message)
        task.add_error(SOURCE, "session_expired", exc.message)
        task.finish("failed")
        _event(logging.ERROR, "influencer.task_result", task_id, company_id=company_id, platform=platform,
               status="failed", reason="session_expired")
    except XWorkerLockUnavailableError as exc:
        task.add_error(SOURCE, "worker_lock_unavailable", str(exc))
        task.finish("failed")
        _event(logging.ERROR, "influencer.task_result", task_id, company_id=company_id, platform=platform,
               status="failed", reason="worker_lock_unavailable")
    except PendingQueueWorkError as exc:
        task.add_error(SOURCE, "pending_queue_work", str(exc))
        task.finish("failed")
        _event(logging.ERROR, "influencer.task_result", task_id, company_id=company_id, platform=platform,
               status="failed", reason="pending_queue_work")
    except GracefulShutdownRequested as exc:
        task.add_warning(SOURCE, "graceful_shutdown", str(exc))
        task.finish("failed")
        _event(logging.WARNING, "influencer.task_result", task_id, company_id=company_id, platform=platform,
               status="failed", reason="graceful_shutdown")
    except Exception as exc:
        logger.exception("event=influencer.task_exception task_id=%s company_id=%s", task_id, company_id)
        task.add_error(SOURCE, "influencer_error", str(exc))
        task.finish("failed")
        _event(logging.ERROR, "influencer.task_result", task_id, company_id=company_id, platform=platform,
               status="failed", reason=type(exc).__name__)


def submit_influencer_task(task_id: str, **request: Any) -> None:
    threading.Thread(
        target=run_influencer_task,
        kwargs={"task_id": task_id, **request},
        daemon=True,
        name=f"influencer-{task_id[:8]}",
    ).start()
