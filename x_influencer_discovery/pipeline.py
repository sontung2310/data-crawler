from __future__ import annotations

import asyncio
import inspect
import logging
import random
import signal
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .browser import PlaywrightFetcher
from .config import Settings
from .discovery import discover_public_candidates, enqueue_public_candidates
from .embeddings import LocalEmbeddingMatcher
from .evaluation import evaluate_candidate
from .extractors import extract_x_profile_from_html
from .classification import ArticleLinkFilter, LocalProfileClassifier, ProfileClassifier
from .models import AccountRef, CandidateEvidence, XProfile
from .storage import open_company_candidate_store, save_json
from .queueing import DurableProfileQueue, QueueMessage, ReceivedProfileMessage
from .snowball import SnowballCheckpoint, progress_path_for_output
from .platforms import (
    account_ref,
    get_platform_adapter,
    normalize_profile_url,
    validate_platform,
)
from .worker_lock import ExclusiveLocalXWorkerLock
from .x_search import XPostSearcher, enqueue_x_search_results
from .x_profile_posts import (
    RecentPostMarkupError,
    XAccessShellRecoveryRequired,
    XFollowingRateLimitError,
    XRecentPostFetcher,
    recent_activity_from_posts,
)

logger = logging.getLogger(__name__)
SYDNEY_TIMEZONE = ZoneInfo("Australia/Sydney")


def _emit_step_event(
    callback: Any,
    event: str,
    *,
    task_id: str | None,
    company_id: str | None,
    platform: str,
    step: str,
    status: str,
    **fields: Any,
) -> None:
    """Emit a stable workflow event without coupling the package to DCT."""
    if not callable(callback):
        return
    callback(
        event,
        task_id=task_id or "unknown",
        run_id=task_id or "unknown",
        company_id=company_id or "unknown",
        platform=platform,
        step=step,
        status=status,
        **fields,
    )

# An authenticated X web session can begin returning a blank HTTP-200 shell
# after sustained profile/timeline requests. Ordinary hydration keeps its
# short retry; an identified access/error shell gets one batch-context reset
# and cooldown before the same handle is attempted again.
_TRANSIENT_X_FETCH_ATTEMPTS = 2
_TRANSIENT_X_RETRY_DELAYS_SECONDS = (5.0,)
_RETRYABLE_PROFILE_FETCH_STATUSES = {"profile_identity_mismatch", "fetch_timeout", "fetch_failed"}
_X_ACCESS_SHELL_COOLDOWN_SECONDS = 300.0


class GracefulShutdownRequested(RuntimeError):
    """Raised after an operator-requested stop has released transient resources."""

    def __init__(self, signal_name: str):
        self.signal_name = signal_name
        super().__init__(f"received {signal_name}")


class PendingQueueWorkError(RuntimeError):
    """Raised when a fresh discovery command would consume unfinished work."""


def _safe_filename(query: str) -> str:
    import re
    return re.sub(r"[^a-z0-9]+", "_", query.lower()).strip("_") or "query"


def _log_funnel(stages: list[tuple[str, int]], limit: int) -> None:
    chain = " → ".join(f"{count} {name}" for name, count in stages)
    logger.debug("funnel: %s → limit %d", chain, limit)


@dataclass(frozen=True)
class FetchedProfile:
    """One sequential browser outcome for transient queue work."""

    profile: XProfile | None
    recent_posts: list[dict[str, Any]]
    technical_error: str | None = None
    immediate_retry: bool = False


class XFetchOutcomeReport:
    """Keep a compact, per-run operational funnel outside MongoDB."""

    def __init__(
        self,
        company_name: str,
        log_dir: Path,
        *,
        company_id: str | None = None,
        platform: str = "x",
        refresh_after_hours: int = 24,
    ):
        self.company_name = company_name
        self.company_id = company_id
        self.platform = platform
        self.log_dir = log_dir
        self.refresh_after_hours = refresh_after_hours
        self.run_id = uuid.uuid4().hex[:12]
        self.started_at = datetime.now(SYDNEY_TIMEZONE)
        self._source_urls = {"x_post": set(), "public_search": set()}
        self._fresh_handles: set[str] = set()
        self._rejections: dict[str, set[str]] = {}
        self._low_scores: dict[str, float] = {}
        self._eligible: dict[str, tuple[int, float]] = {}
        self._retries: dict[tuple[str, str], None] = {}
        self.timeout_count = 0
        self.last_batch_tripped_x_access_circuit_breaker = False

    @staticmethod
    def _error_key(stage: str, reason: str) -> str:
        compact_reason = " ".join(str(reason).split()) or "unknown_technical_error"
        return compact_reason if stage == "fetch" else f"{stage}: {compact_reason}"

    @staticmethod
    def _handle(profile_url: str) -> str:
        return normalize_profile_url(profile_url).rsplit("/", 1)[-1].lower()

    @staticmethod
    def _handle_entry(handle: str, **fields: Any) -> dict[str, Any]:
        """Attach the canonical X URL to every handle-level report item."""
        return {"handle": handle, "handle_url": f"https://x.com/{handle}", **fields}

    def record_discovered(self, source: str, profile_url: str) -> None:
        if source not in self._source_urls:
            raise ValueError(f"unknown discovery source: {source}")
        self._source_urls[source].add(self._handle(profile_url))

    def record_fresh_skip(self, profile_url: str) -> None:
        self._fresh_handles.add(self._handle(profile_url))

    def record_rejection(self, profile_url: str, reason: str) -> None:
        self._rejections.setdefault(self._handle(profile_url), set()).add(reason or "unknown")

    def record_low_score(self, profile_url: str, hybrid_relevance: float) -> None:
        self._low_scores[self._handle(profile_url)] = hybrid_relevance

    def record_eligible(self, profile_url: str, *, final_score: int, hybrid_relevance: float) -> None:
        self._eligible[self._handle(profile_url)] = (final_score, hybrid_relevance)

    def record_failure(self, profile_url: str | None, *, stage: str, reason: str) -> None:
        if not profile_url:
            return
        error_key = self._error_key(stage, reason)
        self._retries[(self._handle(profile_url), error_key)] = None

    def record_deferred(self, profile_url: str) -> None:
        self._retries[(self._handle(profile_url), "deferred_for_manual_resume")] = None

    def discovery_counts(self) -> dict[str, int]:
        """Return source-unique discovery counts for the console summary."""
        x_handles = self._source_urls["x_post"]
        public_handles = self._source_urls["public_search"]
        return {
            "total_unique_handles": len(x_handles | public_handles),
            "unique_from_x_post": len(x_handles),
            "unique_from_public_search": len(public_handles),
            "found_by_both_sources": len(x_handles & public_handles),
        }

    @property
    def fresh_skip_count(self) -> int:
        return len(self._fresh_handles)

    def finish_receive_batch(self, *, circuit_breaker_tripped: bool) -> None:
        self.last_batch_tripped_x_access_circuit_breaker = circuit_breaker_tripped

    def write(self, *, status: str) -> Path:
        ended_at = datetime.now(SYDNEY_TIMEZONE)
        path = self.log_dir / (
            f"{_safe_filename(self.company_name)}-"
            f"{ended_at.strftime('%Y%m%dT%H%M%S%z')}-{self.run_id}-x-fetch-report.json"
        )
        discovery = self.discovery_counts()
        save_json(
            {
                "status": status,
                "company_id": self.company_id,
                "company": self.company_name,
                "platform": self.platform,
                "started_at": self.started_at.isoformat(),
                "ended_at": ended_at.isoformat(),
                "discovery": discovery,
                "skipped_existing_fresh": {
                    "freshness_window_hours": self.refresh_after_hours,
                    "count": len(self._fresh_handles),
                    "handles": [
                        self._handle_entry(handle)
                        for handle in sorted(self._fresh_handles)
                    ],
                },
                "rejected": {
                    "count": len(self._rejections),
                    "handles": [
                        self._handle_entry(handle, reason=reason)
                        for handle, reasons in sorted(self._rejections.items())
                        for reason in sorted(reasons)
                    ],
                },
                "evaluated": {
                    "low_score": [
                        self._handle_entry(handle, hybrid_relevance=round(score, 4))
                        for handle, score in sorted(self._low_scores.items())
                    ],
                    "eligible": [
                        self._handle_entry(
                            handle,
                            final_score=final_score,
                            hybrid_relevance=round(hybrid_relevance, 4),
                        )
                        for handle, (final_score, hybrid_relevance) in sorted(self._eligible.items())
                    ],
                },
                "retry": {
                    "count": len(self._retries),
                    "handles": [
                        self._handle_entry(handle, reason=reason)
                        for handle, reason in sorted(self._retries)
                    ],
                },
            },
            path,
        )
        return path


class RunMetrics:
    """Operational run counters kept outside the permanent candidate schema."""

    def __init__(self):
        self.sources: dict[str, int] = {}
        self.degraded_sources: set[str] = set()
        # ``submitted`` is producer-side truth: a successful SQS send.  It is
        # intentionally distinct from ``received``, which counts deliveries
        # and can grow above submissions when SQS redrives a technical error.
        self.queue = {"submitted": 0, "received": 0, "deleted": 0, "retried": 0}
        self.processing = {"fetched": 0, "evaluated": 0}
        self.eligibility = {"passed": 0, "rejected": 0}
        self.leaderboard = {"inserted": 0, "updated": 0}
        self.snowball = {"submitted": 0, "following_incomplete": 0}

    def record_discovery(self, source: str, *, sent: int, degraded: bool = False) -> None:
        self.sources[source] = self.sources.get(source, 0) + sent
        self.queue["submitted"] += sent
        if degraded:
            self.degraded_sources.add(source)

    def add_batch(self, stats: dict[str, int]) -> None:
        for key in self.queue:
            self.queue[key] += stats.get(key, 0)
        self.processing["fetched"] += stats.get("fetched", 0)
        self.processing["evaluated"] += stats.get("eligible", 0) + stats.get("ineligible", 0)
        self.eligibility["passed"] += stats.get("eligible", 0)
        self.eligibility["rejected"] += stats.get("ineligible", 0)
        for key in self.leaderboard:
            self.leaderboard[key] += stats.get(key, 0)
        self.snowball["submitted"] += stats.get("snowball_submitted", 0)
        self.snowball["following_incomplete"] += stats.get("following_incomplete", 0)
        if stats.get("snowball_failed", 0):
            self.degraded_sources.add("snowball")

    def record_snowball(self, *, sent: int, incomplete_following: bool = False) -> None:
        self.snowball["submitted"] += sent
        self.snowball["following_incomplete"] += int(incomplete_following)

    def report(self, *, queue_depth: int, dead_letter_depth: int = 0, timeouts: int = 0) -> dict[str, Any]:
        queue = {**self.queue, "remaining_depth": queue_depth, "dead_letter_depth": dead_letter_depth}
        if timeouts:
            queue["timeouts"] = timeouts
        return {
            "sources": dict(self.sources),
            "degraded_sources": sorted(self.degraded_sources),
            "queue": queue,
            "processing": dict(self.processing),
            "eligibility": dict(self.eligibility),
            "leaderboard": dict(self.leaderboard),
            "snowball": dict(self.snowball),
        }


class _FreshProfileSubmissionQueue:
    """Apply the eligible-candidate refresh policy before a new queue send.

    The wrapper deliberately delegates every consumer operation to the durable
    queue. It only intercepts source submissions, so a message already in SQS
    is always fetched and evaluated by the worker.
    """

    def __init__(
        self,
        queue: Any,
        candidate_store: Any,
        *,
        refresh_after_hours: int,
        company_id: str | None = None,
        platform: str = "x",
        outcome_report: XFetchOutcomeReport | None = None,
    ):
        self._queue = queue
        self._candidate_store = candidate_store
        self._refresh_after_hours = refresh_after_hours
        self._company_id = company_id
        self._platform = validate_platform(platform)
        self._checked_urls: set[str] = set()
        self._fresh_urls: set[str] = set()
        self._outcome_report = outcome_report

    def __getattr__(self, name: str) -> Any:
        return getattr(self._queue, name)

    def send(self, value: str, **context: Any) -> bool:
        """Submit an X or public-discovery URL unless its record is fresh."""
        return self._submit(value, snowball=False, **context)

    def send_snowball(self, value: str, **context: Any) -> bool:
        """Apply the same policy to relationship-discovery URLs."""
        return self._submit(value, snowball=True, **context)

    def send_account(self, account: AccountRef | dict[str, Any], **context: Any) -> bool:
        """Submit an account using the full platform-aware queue envelope."""

        values = account.to_dict() if isinstance(account, AccountRef) else dict(account)
        platform = validate_platform(context.get("platform", self._platform))
        company_id = context.get("company_id", self._company_id)
        if not company_id:
            raise ValueError("company_id is required for structured snowball queue messages")
        message = QueueMessage.from_account(
            company_id=str(company_id),
            platform=platform,
            account=values,
        )
        return self._submit_message(message, snowball=True)

    def _submit(self, value: str, *, snowball: bool, **context: Any) -> bool:
        platform = context.get("platform", self._platform)
        profile_url = normalize_profile_url(value, platform)
        message = QueueMessage.from_url(
            profile_url,
            company_id=context.get("company_id", self._company_id),
            platform=platform,
            account_id=context.get("account_id"),
            handle=context.get("handle"),
        )
        return self._submit_message(message, snowball=snowball, structured=False)

    def _submit_message(
        self,
        message: QueueMessage,
        *,
        snowball: bool,
        structured: bool = True,
    ) -> bool:
        profile_url = message.profile_url
        if profile_url in self._fresh_urls:
            return False
        if profile_url not in self._checked_urls:
            self._checked_urls.add(profile_url)
            is_fresh = getattr(self._candidate_store, "is_fresh", None)
            if callable(is_fresh):
                try:
                    try:
                        fresh_candidate = bool(is_fresh(
                            profile_url,
                            company_id=message.company_id,
                            platform=message.platform,
                            refresh_after_hours=self._refresh_after_hours,
                        ))
                    except TypeError:
                        # Existing fixture stores still expose the old URL-only
                        # freshness method during the migration window.
                        fresh_candidate = bool(is_fresh(
                            profile_url,
                            refresh_after_hours=self._refresh_after_hours,
                        ))
                except Exception as exc:
                    # A transient Mongo lookup failure must not discard newly
                    # discovered work. Send normally and surface the problem.
                    logger.warning(
                        "candidate freshness check failed before queue submission for %s: %s; submitting normally",
                        profile_url,
                        exc,
                    )
                else:
                    if fresh_candidate:
                        self._fresh_urls.add(profile_url)
                        if self._outcome_report is not None:
                            self._outcome_report.record_fresh_skip(profile_url)
                        return False
        send = self._queue.send_snowball if snowball else self._queue.send
        if structured:
            try:
                return bool(send(message))
            except TypeError:
                # Compatibility for legacy test/caller queue doubles. The
                # real DurableProfileQueue accepts QueueMessage and always
                # serializes the structured envelope.
                try:
                    return bool(send(
                        profile_url,
                        company_id=message.company_id,
                        platform=message.platform,
                        account_id=message.account_id,
                        handle=message.handle,
                    ))
                except TypeError:
                    return bool(send(profile_url))
        try:
            return bool(send(
                profile_url,
                company_id=message.company_id,
                platform=message.platform,
                account_id=message.account_id,
                handle=message.handle,
            ))
        except TypeError:
            return bool(send(profile_url))


def _submit_snowball_account(
    queue: Any,
    account: AccountRef,
    *,
    company_id: str,
    platform: str,
) -> bool:
    """Submit one Following account through the structured queue contract."""

    send_account = getattr(queue, "send_account", None)
    if callable(send_account):
        return bool(send_account(account, company_id=company_id, platform=platform))

    # Compatibility for injected legacy queue doubles. Production queues and
    # _FreshProfileSubmissionQueue always take the AccountRef path above.
    send = getattr(queue, "send_snowball", None) or getattr(queue, "send")
    try:
        return bool(send(
            account.profile_url,
            company_id=company_id,
            platform=platform,
            account_id=account.account_id,
            handle=account.handle,
        ))
    except TypeError:
        return bool(send(account.profile_url))


def _safe_x_following_diagnostics(diagnostics: dict[str, Any]) -> str:
    """Format only bounded Following-shell fields for operational logs."""

    markers = diagnostics.get("visible_markers") or []
    title = " ".join(str(diagnostics.get("page_title") or "").split())[:120]
    final_url = " ".join(str(diagnostics.get("final_url") or "").split())[:240]
    return (
        f"markers={','.join(str(marker) for marker in markers) or 'none'}, "
        f"user_cell_count={int(diagnostics.get('user_cell_count') or 0)}, "
        f"page_title={title!r}, final_url={final_url!r}"
    )


def _store_upsert(
    candidate_store: Any,
    profile: XProfile,
    recent_posts: list[dict[str, Any]],
    *,
    company_id: str,
    company_name: str,
    platform: str = "x",
    final_score: int,
    account_id: str | None = None,
    evaluation: dict[str, Any] | None = None,
    discovery_evidence: list[dict[str, Any]] | None = None,
) -> str:
    """Call the shared store while tolerating legacy fixture-store methods."""

    try:
        return candidate_store.upsert(
            profile,
            recent_posts,
            company_id=company_id,
            company_name=company_name,
            platform=platform,
            final_score=final_score,
            evaluation=evaluation,
            discovery_evidence=discovery_evidence,
            account={
                "account_id": account_id or profile.account_id,
                "handle": profile.handle,
                "profile_url": profile.profile_url,
            },
        )
    except TypeError as exc:
        if "unexpected keyword" not in str(exc) and "required keyword-only" not in str(exc):
            raise
        return candidate_store.upsert(profile, recent_posts, final_score=final_score)


def _read_discovery_evidence(
    evidence_store: Any,
    *,
    run_id: str | None,
    company_id: str | None,
    platform: str,
    account_id: str | None,
) -> list[dict[str, Any]] | None:
    """Read durable source evidence without coupling the package to DCT Mongo."""
    if evidence_store is None or not run_id or not company_id or not account_id:
        return None
    getter = getattr(evidence_store, "get", None)
    if not callable(getter):
        return None
    try:
        value = getter(
            task_id=run_id,
            company_id=company_id,
            platform=platform,
            account_id=account_id,
        )
    except TypeError:
        value = getter(run_id, company_id, platform, account_id)
    return list(value or [])


def _store_delete(
    candidate_store: Any,
    profile_url: str,
    *,
    company_id: str | None = None,
    platform: str,
) -> bool:
    try:
        return bool(candidate_store.delete(
            profile_url,
            company_id=company_id,
            platform=platform,
        ))
    except TypeError as exc:
        if "unexpected keyword" not in str(exc) and "required keyword-only" not in str(exc):
            raise
        return bool(candidate_store.delete(profile_url))


def _store_leaderboard(
    candidate_store: Any,
    *,
    company_id: str,
    platform: str,
    limit: int,
    max_age_days: int | None = 30,
) -> list[dict[str, Any]]:
    try:
        return candidate_store.leaderboard(
            company_id=company_id,
            platform=platform,
            limit=limit,
            max_age_days=max_age_days,
        )
    except TypeError as exc:
        if "unexpected keyword" not in str(exc) and "required keyword-only" not in str(exc):
            raise
        return candidate_store.leaderboard(limit=limit)


def _log_candidate_decision(profile: XProfile, result: Any) -> None:
    """Log one handle's final business decision without content or secrets."""
    relevance_calculated = result.reason in {None, "relevance_below_minimum"}
    relevance = result.relevance
    if result.score is None:
        logger.debug(
            "handle=@%s state=completed outcome=rejected reason=%s followers=%s "
            "lexical=%s semantic=%s hybrid=%s",
            profile.handle,
            result.reason,
            profile.followers.estimated,
            f"{relevance['lexical_overlap']:.4f}" if relevance_calculated else "n/a",
            f"{relevance['semantic_similarity']:.4f}" if relevance_calculated else "n/a",
            f"{relevance['hybrid_relevance']:.4f}" if relevance_calculated else "n/a",
        )
        return

    score = result.score.score_breakdown
    logger.debug(
        "handle=@%s state=scoring_completed followers=%s lexical=%.4f semantic=%.4f hybrid=%.4f "
        "final_score=%d score_components={topic_relevance:%d,followers:%d,recent_activity:%d,engagement:%d}",
        profile.handle,
        profile.followers.estimated,
        relevance["lexical_overlap"],
        relevance["semantic_similarity"],
        relevance["hybrid_relevance"],
        score.total,
        score.topic_relevance,
        score.followers,
        score.recent_activity,
        score.engagement,
    )


def _log_batch_summary(batch_number: int, stats: dict[str, int], rejection_reasons: dict[str, int]) -> None:
    evaluated = stats["eligible"] + stats["ineligible"]
    logger.debug(
        "==================== batch %d completed received=%d fetched=%d evaluated=%d eligible=%d rejected=%d retried=%d "
        "acknowledged=%d inserted=%d updated=%d rejection_reasons=%s ====================",
        batch_number,
        stats["received"],
        stats["fetched"],
        evaluated,
        stats["eligible"],
        stats["ineligible"],
        stats["retried"],
        stats["deleted"],
        stats["inserted"],
        stats["updated"],
        dict(sorted(rejection_reasons.items())),
    )


class SequentialProfileWorker:
    """Fetch one X profile and its recent posts at a time.

    Identity and timeline work share one authenticated Chromium. The queue
    consumer owns sequencing so an X session never has overlapping profile
    actions or a second cold login for the same handle.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self.profile_fetcher = PlaywrightFetcher(
            headless=settings.headless, x_session=settings.x_session, timeout_ms=12_000
        )
        self.post_fetcher = XRecentPostFetcher(
            settings.headless,
            settings.x_session,
            timeout_ms=12_000,
            scroll_delay_min_seconds=settings.x_scroll_delay_min_seconds,
            scroll_delay_max_seconds=settings.x_scroll_delay_max_seconds,
        )
        self._last_fetched_handle: str | None = None

    def _batch_context_is_open(self) -> bool:
        has_context = getattr(self.post_fetcher, "has_batch_context", None)
        return bool(has_context()) if callable(has_context) else False

    @asynccontextmanager
    async def batch_context(self):
        """Keep one authenticated X browser while the caller holds this context.

        Nested SQS batches reuse the already-open Chromium instead of logging
        in again. Handle pacing is preserved across those batches.
        """
        close_on_exit = not self._batch_context_is_open()
        if close_on_exit:
            await self.open_batch_context()
        try:
            yield
        finally:
            if close_on_exit:
                await self.close_batch_context()

    async def wait_before_handle(self, profile_url: str) -> None:
        """Pace different X handles outside the per-profile timeout."""
        handle = profile_url.rsplit("/", 1)[-1].casefold()
        previous_handle = self._last_fetched_handle
        self._last_fetched_handle = handle
        if previous_handle is None or previous_handle == handle:
            return
        minimum = max(0.0, float(getattr(self.settings, "x_handle_delay_min_seconds", 3.0)))
        maximum = max(
            minimum,
            float(getattr(self.settings, "x_handle_delay_max_seconds", 6.0)),
        )
        delay = random.uniform(minimum, maximum)
        logger.debug("X handle pacing: waiting %.2fs before @%s", delay, handle)
        await asyncio.sleep(delay)

    async def close_batch_context(self) -> None:
        close_context = getattr(self.post_fetcher, "close_batch_context", None)
        if callable(close_context):
            result = close_context()
            if inspect.isawaitable(result):
                await result
        self._last_fetched_handle = None

    async def open_batch_context(self) -> None:
        open_context = getattr(self.post_fetcher, "open_batch_context", None)
        if callable(open_context):
            result = open_context()
            if inspect.isawaitable(result):
                await result

    async def fetch(self, profile_url: str) -> FetchedProfile:
        requested_handle = profile_url.rsplit("/", 1)[-1]
        fetch_combined = getattr(self.post_fetcher, "fetch_profile_and_posts", None)
        if callable(fetch_combined):
            return await self._fetch_combined(profile_url, requested_handle)
        return await self._fetch_split(profile_url, requested_handle)

    async def fetch_with_recovery(self, profile_url: str, *, timeout_seconds: int | None = None) -> FetchedProfile:
        """Fetch one profile and recover a blank X access shell outside the timeout."""
        async def once() -> FetchedProfile:
            if timeout_seconds is not None:
                return await asyncio.wait_for(self.fetch(profile_url), timeout=timeout_seconds)
            return await self.fetch(profile_url)

        try:
            return await once()
        except XAccessShellRecoveryRequired as first_error:
            handle = profile_url.rsplit("/", 1)[-1]
            logger.warning(
                "====== Get rate limiting, cooling for 5 minutes then try again ====== handle=@%s",
                handle,
            )
            logger.warning(
                "handle=@%s state=access_shell_recovery_scheduled attempt=2/2 delay_seconds=%s reason=%s",
                handle,
                _X_ACCESS_SHELL_COOLDOWN_SECONDS,
                first_error,
            )
            await self.close_batch_context()
            await asyncio.sleep(_X_ACCESS_SHELL_COOLDOWN_SECONDS)
            await self.open_batch_context()
            logger.debug("handle=@%s state=access_shell_recovery_started attempt=2/2", handle)
            try:
                return await once()
            except XAccessShellRecoveryRequired as second_error:
                return FetchedProfile(
                    None,
                    [],
                    technical_error=f"recent_posts_failed: {second_error}",
                )

    async def _fetch_combined(self, profile_url: str, requested_handle: str) -> FetchedProfile:
        page = None
        last_error: Exception | None = None
        for attempt in range(1, _TRANSIENT_X_FETCH_ATTEMPTS + 1):
            logger.debug(
                "handle=@%s state=fetch_started attempt=%d/%d",
                requested_handle,
                attempt,
                _TRANSIENT_X_FETCH_ATTEMPTS,
            )
            try:
                page = await self.post_fetcher.fetch_profile_and_posts(requested_handle, posts_per_profile=5)
                break
            except XAccessShellRecoveryRequired:
                raise
            except (RecentPostMarkupError, TimeoutError) as exc:
                last_error = exc
                logger.warning(
                    "handle=@%s state=fetch_failed attempt=%d/%d reason=%s",
                    requested_handle,
                    attempt,
                    _TRANSIENT_X_FETCH_ATTEMPTS,
                    exc,
                )
                if attempt == _TRANSIENT_X_FETCH_ATTEMPTS:
                    break
                await self._retry_delay("timeline", profile_url, attempt, str(exc))
            except Exception as exc:
                return FetchedProfile(None, [], technical_error=f"recent_posts_failed: {exc}")
        if page is None:
            assert last_error is not None
            return FetchedProfile(None, [], technical_error=f"recent_posts_failed: {last_error}")
        if page.problem:
            return FetchedProfile(
                None,
                [],
                technical_error=f"Unexpected response: {page.problem}",
                immediate_retry=page.problem == "profile_not_found",
            )
        if not page.html:
            return FetchedProfile(None, [], technical_error="profile_missing")
        try:
            profile = extract_x_profile_from_html(
                requested_handle,
                page.html,
                rendered_surface=page.profile_surface,
            )
        except Exception as exc:
            return FetchedProfile(None, [], technical_error=f"profile_parse_failed: {exc}")
        if profile.source_status != "ok":
            return FetchedProfile(None, [], technical_error=profile.error or profile.source_status)
        logger.debug(
            "handle=@%s state=fetch_completed followers=%s recent_posts=%d",
            profile.handle,
            profile.followers.estimated,
            len(page.posts),
        )
        return FetchedProfile(profile, page.posts)

    async def _fetch_split(self, profile_url: str, requested_handle: str) -> FetchedProfile:
        page = None
        for attempt in range(1, _TRANSIENT_X_FETCH_ATTEMPTS + 1):
            logger.debug("handle=@%s state=fetch_started attempt=%d/%d", requested_handle, attempt, _TRANSIENT_X_FETCH_ATTEMPTS)
            page = await self.profile_fetcher.fetch_one(profile_url)
            if page.html and page.status == "ok":
                break
            logger.warning(
                "handle=@%s state=fetch_failed attempt=%d/%d reason=%s",
                requested_handle, attempt, _TRANSIENT_X_FETCH_ATTEMPTS, page.error or page.status,
            )
            if page.status not in _RETRYABLE_PROFILE_FETCH_STATUSES or attempt == _TRANSIENT_X_FETCH_ATTEMPTS:
                break
            await self._retry_delay("profile", profile_url, attempt, page.error or page.status)

        assert page is not None
        if not page.html or page.status != "ok":
            return FetchedProfile(
                None,
                [],
                technical_error=page.error or page.status,
                immediate_retry=page.status == "profile_not_found",
            )
        handle = profile_url.rsplit("/", 1)[-1]
        try:
            profile = extract_x_profile_from_html(handle, page.html, rendered_surface=page.profile_surface)
        except Exception as exc:
            return FetchedProfile(None, [], technical_error=f"profile_parse_failed: {exc}")
        if profile.source_status != "ok":
            return FetchedProfile(None, [], technical_error=profile.error or profile.source_status)
        posts = None
        timeline_profile_surface = None
        last_post_error: Exception | None = None
        for attempt in range(1, _TRANSIENT_X_FETCH_ATTEMPTS + 1):
            try:
                logger.debug("handle=@%s state=timeline_fetch_started attempt=%d/%d", profile.handle, attempt, _TRANSIENT_X_FETCH_ATTEMPTS)
                fetch_with_surface = getattr(self.post_fetcher, "fetch_one_with_profile_surface", None)
                if callable(fetch_with_surface):
                    posts, timeline_profile_surface = await fetch_with_surface(profile.handle, posts_per_profile=5)
                else:
                    posts = await self.post_fetcher.fetch_one(profile.handle, posts_per_profile=5)
                break
            except (RecentPostMarkupError, TimeoutError) as exc:
                last_post_error = exc
                logger.warning(
                    "handle=@%s state=timeline_fetch_failed attempt=%d/%d reason=%s",
                    profile.handle, attempt, _TRANSIENT_X_FETCH_ATTEMPTS, exc,
                )
                if isinstance(exc, XAccessShellRecoveryRequired):
                    raise
                if attempt == _TRANSIENT_X_FETCH_ATTEMPTS:
                    break
                await self._retry_delay("timeline", profile.profile_url, attempt, str(exc))
            except Exception as exc:
                return FetchedProfile(None, [], technical_error=f"recent_posts_failed: {exc}")
        if posts is None:
            assert last_post_error is not None
            return FetchedProfile(None, [], technical_error=f"recent_posts_failed: {last_post_error}")
        if timeline_profile_surface is not None:
            profile = replace(
                profile,
                bio=profile.bio or timeline_profile_surface.bio,
                profile_img_url=profile.profile_img_url or timeline_profile_surface.profile_img_url,
            )
        logger.debug(
            "handle=@%s state=fetch_completed followers=%s recent_posts=%d",
            profile.handle, profile.followers.estimated, len(posts),
        )
        return FetchedProfile(profile, posts)

    async def _retry_delay(self, stage: str, profile_url: str, attempt: int, reason: str) -> None:
        """Retry one known transient X surface with a new browser context.

        A local retry delay is configurable and defaults to zero. If the
        bounded local attempt also fails, the SQS delivery is released
        immediately for redrive; no queue-wide cooldown is introduced.
        """
        delay_index = min(attempt - 1, len(_TRANSIENT_X_RETRY_DELAYS_SECONDS) - 1)
        delay_seconds = float(getattr(
            self.settings,
            "x_local_retry_delay_seconds",
            _TRANSIENT_X_RETRY_DELAYS_SECONDS[delay_index],
        ))
        logger.debug(
            "handle=@%s state=%s_retry_scheduled attempt=%d/%d delay_seconds=%s reason=%s",
            profile_url.rsplit("/", 1)[-1],
            "fetch" if stage == "profile" else "timeline_fetch",
            attempt + 1,
            _TRANSIENT_X_FETCH_ATTEMPTS,
            delay_seconds,
            reason,
        )
        await asyncio.sleep(delay_seconds)

    async def fetch_relationships(
        self,
        handle: str,
        related_terms: list[str],
        following_max_scrolls: int,
    ) -> Any:
        return await self.post_fetcher.fetch_relationships(
            handle,
            related_terms,
            following_max_scrolls,
        )

    async def fetch_following(self, handle: str, following_max_scrolls: int) -> Any:
        return await self.post_fetcher.fetch_following(handle, following_max_scrolls)


class _BatchVisibilityHeartbeat:
    """Renew every unacknowledged delivery while a receive batch is processed."""

    def __init__(self, queue: Any, messages: list[ReceivedProfileMessage]):
        self._extend_visibility = getattr(queue, "extend_visibility", None)
        self._active = {
            message.receipt_handle: message
            for message in messages
            if message.profile_url and not message.parse_error
        }
        timeout = max(60, int(getattr(queue, "visibility_timeout_seconds", 900)))
        self._interval_seconds = max(
            0.01,
            float(getattr(queue, "visibility_heartbeat_seconds", max(30, min(300, timeout // 2)))),
        )
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._extend_visibility is None or not self._active:
            return
        await self._renew_all()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)
        self._task = None

    def release(self, message: ReceivedProfileMessage) -> None:
        self._active.pop(message.receipt_handle, None)

    async def _run(self) -> None:
        while self._active:
            await asyncio.sleep(self._interval_seconds)
            await self._renew_all()

    async def _renew_all(self) -> None:
        assert self._extend_visibility is not None
        for message in list(self._active.values()):
            try:
                result = self._extend_visibility(message)
                if inspect.isawaitable(result):
                    await result
            except Exception as exc:
                # The current work remains idempotent; retain the message and
                # make a missed lease renewal visible for operational review.
                logger.warning("could not extend queue visibility for %s: %s", message.profile_url, exc)


async def _fetch_profile(worker: Any, message: ReceivedProfileMessage) -> FetchedProfile:
    assert message.profile_url is not None
    return await worker.fetch(message.profile_url)


async def _fetch_profile_with_access_shell_recovery(
    worker: Any,
    message: ReceivedProfileMessage,
    settings: Settings,
) -> FetchedProfile:
    """Retry one access-shell failure after a context cooldown.

    The cooldown intentionally lives outside the per-profile ``wait_for`` so
    the normal 30-second account timeout still applies to each actual fetch.
    """
    timeout_seconds = max(1, int(getattr(settings, "account_attempt_timeout_seconds", 30)))
    fetch_with_recovery = getattr(worker, "fetch_with_recovery", None)
    if callable(fetch_with_recovery):
        result = fetch_with_recovery(message.profile_url, timeout_seconds=timeout_seconds)
        if inspect.isawaitable(result):
            return await result
        return result
    try:
        return await asyncio.wait_for(_fetch_profile(worker, message), timeout=timeout_seconds)
    except XAccessShellRecoveryRequired as first_error:
        close_context = getattr(worker, "close_batch_context", None)
        open_context = getattr(worker, "open_batch_context", None)
        if not callable(close_context) or not callable(open_context):
            raise

        handle = message.profile_url.rsplit("/", 1)[-1]
        logger.warning(
            "====== Get rate limiting, cooling for 5 minutes then try again ====== handle=@%s",
            handle,
        )
        logger.warning(
            "handle=@%s state=access_shell_recovery_scheduled attempt=2/2 delay_seconds=%s reason=%s",
            handle,
            _X_ACCESS_SHELL_COOLDOWN_SECONDS,
            first_error,
        )
        result = close_context()
        if inspect.isawaitable(result):
            await result
        await asyncio.sleep(_X_ACCESS_SHELL_COOLDOWN_SECONDS)
        result = open_context()
        if inspect.isawaitable(result):
            await result
        logger.debug("handle=@%s state=access_shell_recovery_started attempt=2/2", handle)
        try:
            return await asyncio.wait_for(_fetch_profile(worker, message), timeout=timeout_seconds)
        except XAccessShellRecoveryRequired as second_error:
            return FetchedProfile(
                None,
                [],
                technical_error=f"recent_posts_failed: {second_error}",
            )


async def consume_profile_batch(
    queue: Any,
    worker: Any,
    classifier: Any,
    embedding_matcher: Any,
    candidate_store: Any,
    *,
    related_terms: list[str],
    company_name: str,
    company_id: str | None = None,
    platform: str = "x",
    company_domain: str | None,
    settings: Settings,
    handled_profile_urls: set[str] | None = None,
    outcome_report: XFetchOutcomeReport | None = None,
    batch_number: int = 1,
    evidence_store: Any | None = None,
    run_id: str | None = None,
) -> dict[str, int]:
    """Consume one SQS receive batch with delayed acknowledgement semantics."""
    messages: list[ReceivedProfileMessage] = queue.receive()
    if messages:
        logger.debug("==================== batch %d started received=%d ====================", batch_number, len(messages))
    heartbeat = _BatchVisibilityHeartbeat(queue, messages)
    await heartbeat.start()
    try:
        batch_context = getattr(worker, "batch_context", None)
        if messages and callable(batch_context):
            async with batch_context():
                stats = await _consume_received_profile_batch(
                    queue,
                    worker,
                    classifier,
                    embedding_matcher,
                    candidate_store,
                    messages=messages,
                    heartbeat=heartbeat,
                    related_terms=related_terms,
                    company_name=company_name,
                    company_id=company_id,
                    platform=platform,
                    company_domain=company_domain,
                    settings=settings,
                    handled_profile_urls=handled_profile_urls if handled_profile_urls is not None else set(),
                    outcome_report=outcome_report,
                    evidence_store=evidence_store,
                    run_id=run_id,
                )
        else:
            stats = await _consume_received_profile_batch(
                queue,
                worker,
                classifier,
                embedding_matcher,
                candidate_store,
                messages=messages,
                heartbeat=heartbeat,
                related_terms=related_terms,
                company_name=company_name,
                company_id=company_id,
                platform=platform,
                company_domain=company_domain,
                settings=settings,
                handled_profile_urls=handled_profile_urls if handled_profile_urls is not None else set(),
                outcome_report=outcome_report,
                evidence_store=evidence_store,
                run_id=run_id,
            )
        if messages:
            _log_batch_summary(batch_number, stats, {})
        return stats
    finally:
        await heartbeat.stop()


async def _consume_received_profile_batch(
    queue: Any,
    worker: Any,
    classifier: Any,
    embedding_matcher: Any,
    candidate_store: Any,
    *,
    messages: list[ReceivedProfileMessage],
    heartbeat: _BatchVisibilityHeartbeat,
    related_terms: list[str],
    company_name: str,
    company_id: str | None,
    platform: str,
    company_domain: str | None,
    settings: Settings,
    handled_profile_urls: set[str],
    outcome_report: XFetchOutcomeReport | None,
    evidence_store: Any | None,
    run_id: str | None,
) -> dict[str, int]:
    stats = {
        "received": len(messages), "fetched": 0, "retried": 0,
        "eligible": 0, "ineligible": 0, "inserted": 0, "updated": 0, "deleted": 0,
    }
    timeout_count = 0
    rejection_reasons: dict[str, int] = {}
    all_messages_are_first_delivery_x_work = bool(messages) and all(
        message.profile_url
        and not message.parse_error
        and message.platform == "x"
        and message.receive_count == 1
        for message in messages
    )
    consecutive_x_fetch_failures = 0
    circuit_breaker_tripped = False
    circuit_breaker_deferral_failed = False
    deferred_job_ids: set[str] = set()

    async def release_for_fast_fail(
        deliveries: list[ReceivedProfileMessage], *, profile_url: str | None, stage: str, reason: str,
    ) -> bool:
        """Yield failed work to SQS redrive without another application retry."""
        if outcome_report is not None:
            outcome_report.record_failure(profile_url, stage=stage, reason=reason)
        release = getattr(queue, "release_for_retry", None)
        if not callable(release):
            logger.warning(
                "candidate fast-fail could not release profile_url=%s stage=%s reason=%s",
                profile_url, stage, reason,
            )
            return False
        try:
            for delivery in deliveries:
                result = release(delivery)
                if inspect.isawaitable(result):
                    await result
                heartbeat.release(delivery)
            logger.warning(
                "handle=@%s state=released_for_dlq stage=%s reason=%s",
                profile_url.rsplit("/", 1)[-1] if profile_url else "unknown", stage, reason,
            )
            return True
        except Exception as exc:
            logger.warning(
                "candidate fast-fail could not release profile_url=%s stage=%s: %s",
                profile_url, stage, exc,
            )
            return False

    async def defer_for_manual_resume(
        deliveries: list[ReceivedProfileMessage], *, profile_url: str,
    ) -> bool:
        """Replace untouched deliveries before the circuit breaker stops.

        The queue adapter sends the replacement before acknowledging the
        received copy.  If that handoff cannot complete, this batch keeps
        processing rather than stopping and risking an unprocessed URL being
        redriven directly to the DLQ under the one-delivery policy.
        """
        defer = getattr(queue, "defer_for_manual_resume", None)
        if not callable(defer):
            logger.warning(
                "X-access circuit breaker could not defer profile_url=%s; continuing this batch safely",
                profile_url,
            )
            return False
        try:
            for delivery in deliveries:
                result = defer(delivery)
                if inspect.isawaitable(result):
                    result = await result
                if result is False:
                    raise RuntimeError("queue did not confirm manual-resume deferral")
                if outcome_report is not None:
                    outcome_report.record_deferred(profile_url)
                heartbeat.release(delivery)
                stats["deleted"] += 1
            logger.warning(
                "handle=@%s state=deferred_for_manual_resume",
                profile_url.rsplit("/", 1)[-1],
            )
            return True
        except Exception as exc:
            logger.warning(
                "X-access circuit breaker could not defer profile_url=%s; continuing this batch safely: %s",
                profile_url,
                exc,
            )
            return False

    delivery_groups: dict[str, list[ReceivedProfileMessage]] = {}
    for message in messages:
        if not message.profile_url or message.parse_error:
            reason = message.parse_error or "missing_profile_url"
            logger.warning("candidate fast-fail stage=queue_message reason=%s", reason)
            await release_for_fast_fail([message], profile_url=message.profile_url, stage="queue_message", reason=reason)
            stats["retried"] += 1
            continue
        if message.receive_count > settings.sqs_max_receive_count:
            await release_for_fast_fail(
                [message], profile_url=message.profile_url, stage="redrive_pending", reason="receive_limit_exceeded",
            )
            stats["retried"] += 1
            continue
        job_id = message.job_identity or f"{message.company_id or company_id or ''}:{message.platform}:{message.profile_url}"
        delivery_groups.setdefault(job_id, []).append(message)

    delivery_items = list(delivery_groups.items())

    async def maybe_trip_x_access_circuit_breaker(index: int) -> bool:
        """Stop after three consecutive X failures once later URLs are safe."""
        nonlocal circuit_breaker_tripped, circuit_breaker_deferral_failed
        nonlocal consecutive_x_fetch_failures
        if (
            not all_messages_are_first_delivery_x_work
            or circuit_breaker_deferral_failed
        ):
            return False
        consecutive_x_fetch_failures += 1
        if consecutive_x_fetch_failures < settings.x_access_failure_streak_limit:
            return False
        for deferred_job_id, deferred_deliveries in delivery_items[index + 1:]:
            if deferred_job_id in deferred_job_ids:
                continue
            deferred_url = deferred_deliveries[0].profile_url
            if not deferred_url or not await defer_for_manual_resume(deferred_deliveries, profile_url=deferred_url):
                circuit_breaker_deferral_failed = True
                return False
            deferred_job_ids.add(deferred_job_id)
        circuit_breaker_tripped = True
        return True

    fetched: list[tuple[list[ReceivedProfileMessage], FetchedProfile]] = []
    for index, (job_id, deliveries) in enumerate(delivery_items):
        profile_url = deliveries[0].profile_url
        if job_id in deferred_job_ids:
            continue
        if job_id in handled_profile_urls or profile_url in handled_profile_urls:
            consecutive_x_fetch_failures = 0
            try:
                for delivery in deliveries:
                    queue.acknowledge(delivery)
                    heartbeat.release(delivery)
                    stats["deleted"] += 1
                logger.debug("acknowledged already-handled duplicate delivery for %s", profile_url)
            except Exception as exc:
                logger.warning("duplicate delivery for %s could not be acknowledged and will redrive: %s", profile_url, exc)
                await release_for_fast_fail(deliveries, profile_url=profile_url, stage="acknowledge_duplicate", reason=str(exc))
                stats["retried"] += len(deliveries)
            continue
        message = deliveries[0]
        try:
            wait_before_handle = getattr(worker, "wait_before_handle", None)
            if callable(wait_before_handle):
                result = wait_before_handle(profile_url)
                if inspect.isawaitable(result):
                    await result
            outcome = await _fetch_profile_with_access_shell_recovery(worker, message, settings)
        except Exception as exc:
            logger.warning("profile fetch fast-fail for %s: %s", profile_url, exc)
            if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
                timeout_count += 1
                if outcome_report is not None:
                    outcome_report.timeout_count += 1
            released = await release_for_fast_fail(deliveries, profile_url=profile_url, stage="fetch", reason=str(exc))
            stats["retried"] += len(deliveries)
            if released:
                if await maybe_trip_x_access_circuit_breaker(index):
                    break
            continue
        if outcome.technical_error or outcome.profile is None:
            logger.warning(
                "candidate fast-fail profile_url=%s stage=fetch reason=%s",
                profile_url,
                outcome.technical_error or "profile_missing",
            )
            released = await release_for_fast_fail(
                deliveries,
                profile_url=profile_url,
                stage="fetch",
                reason=outcome.technical_error or "profile_missing",
            )
            if "timeout" in (outcome.technical_error or "").casefold():
                timeout_count += 1
                if outcome_report is not None:
                    outcome_report.timeout_count += 1
            stats["retried"] += len(deliveries)
            if released:
                if await maybe_trip_x_access_circuit_breaker(index):
                    break
            continue
        consecutive_x_fetch_failures = 0
        stats["fetched"] += 1
        fetched.append((deliveries, outcome))
    if outcome_report is not None:
        outcome_report.finish_receive_batch(
            circuit_breaker_tripped=circuit_breaker_tripped,
        )
    if not fetched:
        return stats

    profiles = [outcome.profile for _, outcome in fetched]
    assert all(profile is not None for profile in profiles)
    profiles = [profile for profile in profiles if profile is not None]
    try:
        for profile in profiles:
            logger.debug("handle=@%s state=classification_started", profile.handle)
        labels = await asyncio.to_thread(classifier.classify_profiles, profiles) if classifier else {
            profile.handle.lower(): "person_name" for profile in profiles
        }
        if not isinstance(labels, dict):
            raise RuntimeError("name classification did not return a handle-to-label mapping")
        for profile in profiles:
            label = labels.get(profile.handle.lower())
            if label:
                logger.debug("handle=@%s state=classification_completed label=%s", profile.handle, label)
    except Exception as exc:
        logger.warning(
            "candidate batch fast-fail stage=classification handles=%s reason=%s",
            [profile.handle for profile in profiles],
            exc,
        )
        for deliveries, outcome in fetched:
            await release_for_fast_fail(
                deliveries,
                profile_url=outcome.profile.profile_url if outcome.profile else None,
                stage="classification",
                reason=str(exc),
            )
        stats["retried"] += sum(len(deliveries) for deliveries, _ in fetched)
        return stats

    unresolved = [
        (deliveries, outcome)
        for deliveries, outcome in fetched
        if outcome.profile is not None and not labels.get(outcome.profile.handle.lower())
    ]
    if unresolved:
        unresolved_handles = [outcome.profile.handle for _, outcome in unresolved if outcome.profile is not None]
        logger.warning(
            "candidate fast-fail stage=classification_missing handles=%s reason=name classification did not produce a result",
            unresolved_handles,
        )
        for deliveries, outcome in unresolved:
            await release_for_fast_fail(
                deliveries,
                profile_url=outcome.profile.profile_url if outcome.profile else None,
                stage="classification_missing",
                reason="name classification did not produce a result",
            )
        stats["retried"] += sum(len(deliveries) for deliveries, _ in unresolved)
        fetched = [item for item in fetched if item not in unresolved]
        profiles = [outcome.profile for _, outcome in fetched if outcome.profile is not None]
    if not fetched:
        return stats

    try:
        documents = {
            profile.handle.lower(): _ranking_document(profile, outcome.recent_posts)
            for _, outcome in fetched
            for profile in [outcome.profile]
            if profile is not None
        }
        semantic_method = getattr(embedding_matcher, "strict_document_similarities", None)
        if semantic_method is None:
            semantic_method = embedding_matcher.document_similarities
        semantic = await asyncio.to_thread(
            semantic_method,
            settings.company_summary,
            documents,
            batch_size=settings.embedding_batch_size,
        )
    except Exception as exc:
        logger.warning(
            "candidate batch fast-fail stage=classification_or_embedding handles=%s reason=%s",
            [profile.handle for profile in profiles],
            exc,
        )
        for deliveries, outcome in fetched:
            await release_for_fast_fail(
                deliveries,
                profile_url=outcome.profile.profile_url if outcome.profile else None,
                stage="embedding",
                reason=str(exc),
            )
        stats["retried"] += sum(len(deliveries) for deliveries, _ in fetched)
        return stats

    for deliveries, outcome in fetched:
        message = deliveries[0]
        profile = outcome.profile
        assert profile is not None and message.profile_url is not None
        try:
            logger.debug("handle=@%s state=evaluation_started", profile.handle)
            result = evaluate_candidate(
                profile,
                outcome.recent_posts,
                related_terms=related_terms,
                company_summary=settings.company_summary or "",
                minimum_followers=settings.minimum_followers,
                minimum_relevance_score=settings.minimum_relevance_score,
                company_name=company_name,
                company_domain=company_domain,
                account_label=labels[profile.handle.lower()],
                semantic_similarity=semantic.get(profile.handle.lower()),
            )
            _log_candidate_decision(profile, result)
            stop_after_current = False
            if result.eligible:
                assert result.score is not None
                profile = replace(
                    profile,
                    platform=message.platform or platform,
                    account_id=message.account_id or profile.account_id,
                )
                operation = _store_upsert(
                    candidate_store,
                    profile,
                    outcome.recent_posts,
                    company_id=message.company_id or company_id or "",
                    company_name=company_name,
                    platform=message.platform or platform,
                    account_id=message.account_id,
                    final_score=result.score.score_breakdown.total,
                    discovery_evidence=_read_discovery_evidence(
                        evidence_store,
                        run_id=run_id,
                        company_id=message.company_id or company_id,
                        platform=message.platform or platform,
                        account_id=message.account_id or profile.account_id,
                    ),
                    evaluation={
                        "status": "eligible",
                        "score_breakdown": result.score.score_breakdown.__dict__,
                    },
                )
                stats["eligible"] += 1
                stats[operation] += 1
                logger.debug("handle=@%s state=persisted operation=%s", profile.handle, operation)
                logger.debug("handle=@%s state=completed outcome=eligible", profile.handle)
                if outcome_report is not None:
                    outcome_report.record_eligible(
                        profile.profile_url,
                        final_score=result.score.score_breakdown.total,
                        hybrid_relevance=result.relevance["hybrid_relevance"],
                    )
                if (
                    settings.enable_snowball
                    and result.relevance["hybrid_relevance"] >= settings.good_hybrid_relevance_threshold
                ):
                    try:
                        relationships = await asyncio.wait_for(
                            worker.fetch_relationships(
                                profile.handle,
                                related_terms,
                                settings.following_max_scrolls,
                            ),
                            timeout=max(1, int(getattr(settings, "account_attempt_timeout_seconds", 30))),
                        )
                        source_url = normalize_profile_url(profile.profile_url)
                        sent = 0
                        for relationship_url in relationships.profile_urls:
                            normalized_url = normalize_profile_url(relationship_url)
                            if normalized_url != source_url:
                                relationship_account = account_ref(
                                    message.platform or platform,
                                    profile_url=normalized_url,
                                )
                                sent += int(_submit_snowball_account(
                                    queue,
                                    relationship_account,
                                    company_id=message.company_id or company_id or "",
                                    platform=message.platform or platform,
                                ))
                        stats["snowball_submitted"] = stats.get("snowball_submitted", 0) + sent
                        stats["following_incomplete"] = stats.get("following_incomplete", 0) + int(
                            relationships.following_incomplete
                        )
                    except XFollowingRateLimitError as exc:
                        sent = 0
                        for relationship_url in exc.partial_profile_urls:
                            relationship_account = account_ref(
                                message.platform or platform,
                                profile_url=relationship_url,
                            )
                            sent += int(_submit_snowball_account(
                                queue,
                                relationship_account,
                                company_id=message.company_id or company_id or "",
                                platform=message.platform or platform,
                            ))
                        stats["snowball_submitted"] = stats.get("snowball_submitted", 0) + sent
                        stats["snowball_rate_limited"] = 1
                        stop_after_current = True
                        logger.warning(
                            "snowball state=rate_limit_detected handle=@%s partial_submitted=%d diagnostics=%s",
                            profile.handle,
                            sent,
                            _safe_x_following_diagnostics(exc.diagnostics),
                        )
                    except Exception as exc:
                        # Expansion is opt-in best-effort work. Its failure must
                        # not redrive an already persisted parent candidate.
                        logger.warning("snowball expansion failed for %s: %s", message.profile_url, exc)
                        if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
                            timeout_count += 1
                            if outcome_report is not None:
                                outcome_report.timeout_count += 1
                        stats["snowball_failed"] = stats.get("snowball_failed", 0) + 1
            else:
                _store_delete(
                    candidate_store,
                    message.profile_url,
                    company_id=message.company_id or company_id or "",
                    platform=message.platform or platform,
                )
                stats["ineligible"] += 1
                reason = result.reason or "unknown"
                rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1
                if outcome_report is not None:
                    if reason == "relevance_below_minimum":
                        outcome_report.record_low_score(
                            profile.profile_url,
                            result.relevance["hybrid_relevance"],
                        )
                    else:
                        outcome_report.record_rejection(profile.profile_url, reason)
            handled_profile_urls.add(job_id)
            for delivery in deliveries:
                queue.acknowledge(delivery)
                heartbeat.release(delivery)
                stats["deleted"] += 1
            if stop_after_current:
                break
        except Exception as exc:
            logger.warning("candidate fast-fail profile_url=%s stage=evaluate_or_persist reason=%s", message.profile_url, exc)
            await release_for_fast_fail(
                deliveries,
                profile_url=message.profile_url,
                stage="evaluate_or_persist",
                reason=str(exc),
            )
            stats["retried"] += len(deliveries)
    return stats


def _validate_durable_inputs(
    query: str,
    n: int,
    settings: Settings,
    company_name: str | None,
    company_domain: str | None,
    related_terms: list[str] | None,
) -> tuple[str, list[str]]:
    query = " ".join(query.split())
    if not query:
        raise ValueError("query must not be empty")
    if n < 1:
        raise ValueError("n must be at least 1")
    if not company_name or not company_name.strip():
        raise ValueError("company name is required")
    if not company_domain or not company_domain.strip():
        raise ValueError("company domain is required")
    if not settings.company_summary or not settings.company_summary.strip():
        raise ValueError("company summary is required")
    if not settings.embedding_enabled:
        raise ValueError("EMBEDDING_ENABLED must be true for durable discovery")
    terms = [" ".join(term.split()) for term in related_terms or [] if str(term).strip()]
    if not terms:
        raise ValueError("at least one explicit related term is required")
    return query, list(dict.fromkeys(terms))


def _classifier_for(settings: Settings) -> Any:
    if settings.classification_backend == "local":
        return LocalProfileClassifier(settings.local_classifier_model, settings.local_classifier_device)
    if settings.classification_backend == "openai":
        return ProfileClassifier(settings.openai_api_key, settings.openai_model)
    raise ValueError("CLASSIFICATION_BACKEND must be local or openai for durable discovery")


def _persist_discovery_evidence(
    evidence_store: Any,
    *,
    profile_url: str,
    evidence: CandidateEvidence,
    run_id: str | None,
    company_id: str | None,
    platform: str,
) -> None:
    """Write one normalized evidence item before queue submission."""
    if not run_id or not company_id:
        return
    recorder = getattr(evidence_store, "record", None)
    if not callable(recorder):
        return
    try:
        recorder(
            task_id=run_id,
            company_id=company_id,
            platform=platform,
            profile_url=profile_url,
            evidence=evidence.to_dict(),
        )
    except TypeError:
        recorder(run_id, company_id, platform, profile_url, evidence.to_dict())


async def produce_x_discovery(
    queue: Any,
    searcher: Any,
    related_terms: list[str],
    *,
    company_id: str | None = None,
    platform: str = "x",
    language: str,
    authors_per_query: int,
    max_scrolls_per_query: int,
    outcome_report: XFetchOutcomeReport | None = None,
    evidence_store: Any | None = None,
    run_id: str | None = None,
) -> tuple[int, bool]:
    """Run every X Latest term sequentially and send normalized author URLs."""
    try:
        lanes = await searcher.search(
            related_terms,
            language=language,
            authors_per_query=authors_per_query,
            max_scrolls_per_query=max_scrolls_per_query,
        )
    except Exception as exc:
        logger.warning("X Latest discovery degraded: %s", exc)
        return 0, True
    try:
        return enqueue_x_search_results(
            queue,
            lanes,
            related_terms,
            company_id=company_id,
            platform=platform,
            on_discovered=(
                (lambda profile_url: outcome_report.record_discovered("x_post", profile_url))
                if outcome_report is not None else None
            ),
            on_evidence=(
                (lambda profile_url, evidence: _persist_discovery_evidence(
                    evidence_store,
                    profile_url=profile_url,
                    evidence=evidence,
                    run_id=run_id,
                    company_id=company_id,
                    platform=platform,
                ))
                if evidence_store is not None else None
            ),
        ), bool(getattr(searcher, "failed_terms", set()))
    except Exception as exc:
        logger.warning("Could not enqueue X-discovered profiles: %s", exc)
        return 0, True


async def produce_public_discovery(
    queue: Any,
    fetcher: Any,
    related_terms: list[str],
    *,
    company_id: str | None = None,
    platform: str = "x",
    candidates_per_article: int,
    article_filter: Any | None,
    outcome_report: XFetchOutcomeReport | None = None,
    evidence_store: Any | None = None,
    run_id: str | None = None,
) -> tuple[int, bool]:
    """Run public article discovery independently and submit to the common queue."""
    try:
        candidates = await discover_public_candidates(
            related_terms,
            fetcher,
            candidates_per_article=candidates_per_article,
            article_filter=article_filter,
        )
    except Exception as exc:
        logger.warning("Public discovery degraded while X discovery continues: %s", exc)
        return 0, True
    try:
        degraded = bool(
            getattr(candidates, "failed_queries", ())
            or getattr(candidates, "failed_article_urls", ())
        )
        return enqueue_public_candidates(
            queue,
            candidates,
            company_id=company_id,
            platform=platform,
            on_discovered=(
                (lambda profile_url: outcome_report.record_discovered("public_search", profile_url))
                if outcome_report is not None else None
            ),
            on_evidence=(
                (lambda profile_url, evidence: _persist_discovery_evidence(
                    evidence_store,
                    profile_url=profile_url,
                    evidence=evidence,
                    run_id=run_id,
                    company_id=company_id,
                    platform=platform,
                ))
                if evidence_store is not None else None
            ),
        ), degraded
    except Exception as exc:
        logger.warning("Could not enqueue public-discovered profiles: %s", exc)
        return 0, True


async def _run_durable_with_store(
    query: str,
    n: int,
    settings: Settings,
    *,
    company_name: str,
    company_id: str | None = None,
    platform: str = "x",
    company_domain: str | None = None,
    related_terms: list[str],
    candidate_store: Any,
    queue: Any,
    worker: Any | None = None,
    classifier: Any | None = None,
    embedding_matcher: Any | None = None,
    x_searcher: Any | None = None,
    public_fetcher: Any | None = None,
    article_filter: Any | None = None,
    produce_discovery: bool = True,
    outcome_report: XFetchOutcomeReport | None = None,
    evidence_store: Any | None = None,
    run_id: str | None = None,
    event_callback: Any | None = None,
) -> dict[str, Any]:
    company_id = company_id or DurableProfileQueue.company_slug(company_name)
    platform = validate_platform(platform)
    metrics = RunMetrics()
    if produce_discovery:
        pending_depth = queue.depth()
        if pending_depth > 0:
            raise PendingQueueWorkError(
                f"{platform} queue has {pending_depth} unfinished message(s); run the same command with --resume first"
            )
    begin_run = getattr(queue, "begin_run", None)
    if callable(begin_run):
        reset_result = begin_run()
        if inspect.isawaitable(reset_result):
            await reset_result
    submission_queue = _FreshProfileSubmissionQueue(
        queue,
        candidate_store,
        refresh_after_hours=settings.profile_refresh_after_hours,
        company_id=company_id,
        platform=platform,
        outcome_report=outcome_report,
    )
    if produce_discovery:
        _emit_step_event(
            event_callback,
            "influencer.step_start",
            task_id=run_id,
            company_id=company_id,
            platform=platform,
            step="x_discovery",
            status="running",
            terms=len(related_terms),
        )
        x_sent, x_degraded = await produce_x_discovery(
            submission_queue,
            x_searcher or XPostSearcher(settings.headless, settings.x_session),
            related_terms,
            company_id=company_id,
            platform=platform,
            language="en",
            authors_per_query=settings.x_authors_per_query,
            max_scrolls_per_query=settings.x_max_scrolls_per_query,
            outcome_report=outcome_report,
            evidence_store=evidence_store,
            run_id=run_id,
        )
        metrics.record_discovery("x_post", sent=x_sent, degraded=x_degraded)
        _emit_step_event(
            event_callback,
            "influencer.step_result",
            task_id=run_id,
            company_id=company_id,
            platform=platform,
            step="x_discovery",
            status="degraded" if x_degraded else "completed",
            terms=len(related_terms),
            submitted=x_sent,
            degraded=x_degraded,
        )
        _emit_step_event(
            event_callback,
            "influencer.step_start",
            task_id=run_id,
            company_id=company_id,
            platform=platform,
            step="public_discovery",
            status="running",
            terms=len(related_terms),
        )
        public_sent, public_degraded = await produce_public_discovery(
            submission_queue,
            public_fetcher or PlaywrightFetcher(settings.headless, timeout_ms=12_000),
            related_terms,
            company_id=company_id,
            platform=platform,
            candidates_per_article=settings.public_candidates_per_article,
            article_filter=article_filter if article_filter is not None else ArticleLinkFilter(settings.openai_api_key, settings.openai_model),
            outcome_report=outcome_report,
            evidence_store=evidence_store,
            run_id=run_id,
        )
        metrics.record_discovery("public", sent=public_sent, degraded=public_degraded)
        _emit_step_event(
            event_callback,
            "influencer.step_result",
            task_id=run_id,
            company_id=company_id,
            platform=platform,
            step="public_discovery",
            status="degraded" if public_degraded else "completed",
            terms=len(related_terms),
            submitted=public_sent,
            degraded=public_degraded,
        )
        if outcome_report is not None:
            discovery = outcome_report.discovery_counts()
            logger.debug(
                "discovery completed total_unique_handles=%d x_post=%d public_search=%d found_by_both_sources=%d",
                discovery["total_unique_handles"],
                discovery["unique_from_x_post"],
                discovery["unique_from_public_search"],
                discovery["found_by_both_sources"],
            )
            logger.debug(
                "queue submission completed queued=%d skipped_existing_fresh=%d freshness_window_hours=%d",
                metrics.queue["submitted"],
                outcome_report.fresh_skip_count,
                settings.profile_refresh_after_hours,
            )

    _emit_step_event(
        event_callback,
        "influencer.step_start",
        task_id=run_id,
        company_id=company_id,
        platform=platform,
        step="queue_processing",
        status="running",
    )
    worker = worker or SequentialProfileWorker(settings)
    classifier = classifier or _classifier_for(settings)
    embedding_matcher = embedding_matcher or LocalEmbeddingMatcher(settings.embedding_enabled, settings.embedding_model)
    handled_profile_urls: set[str] = set()
    run_status = "completed"
    batch_number = 0
    async with _worker_batch_context(worker):
        while True:
            batch_number += 1
            batch = await consume_profile_batch(
                submission_queue,
                worker,
                classifier,
                embedding_matcher,
                candidate_store,
                related_terms=related_terms,
                company_name=company_name,
                company_id=company_id,
                platform=platform,
                company_domain=company_domain,
                settings=settings,
                handled_profile_urls=handled_profile_urls,
                outcome_report=outcome_report,
                batch_number=batch_number,
                evidence_store=evidence_store,
                run_id=run_id,
            )
            metrics.add_batch(batch)
            if batch.get("snowball_rate_limited"):
                logger.error(
                    "snowball state=stopped_rate_limit resume_required=true; "
                    "the current parent was acknowledged and untouched queue work remains available"
                )
                run_status = "stopped_x_rate_limit"
                break
            if outcome_report is not None and outcome_report.last_batch_tripped_x_access_circuit_breaker:
                logger.error(
                    "X-access circuit breaker: %d consecutive first-delivery X-fetch failures; "
                    "later received URLs were deferred for manual --resume; stopping before another batch",
                    settings.x_access_failure_streak_limit,
                )
                run_status = "stopped_x_access_errors"
                break
            if not batch["received"]:
                remaining_depth = queue.depth()
                if remaining_depth <= 0:
                    break
                logger.debug(
                    "queue idle: remaining_depth=%d; waiting %ds for temporarily invisible retry work",
                    remaining_depth,
                    settings.sqs_idle_poll_seconds,
                )
                await asyncio.sleep(settings.sqs_idle_poll_seconds)

    report = metrics.report(
        queue_depth=queue.depth(),
        dead_letter_depth=queue.dead_letter_depth() if hasattr(queue, "dead_letter_depth") else 0,
        timeouts=outcome_report.timeout_count if outcome_report is not None else 0,
    )
    _emit_step_event(
        event_callback,
        "influencer.step_result",
        task_id=run_id,
        company_id=company_id,
        platform=platform,
        step="queue_processing",
        status=run_status,
        fetched=report["processing"]["fetched"],
        evaluated=report["processing"]["evaluated"],
        eligible=report["eligibility"]["passed"],
        rejected=report["eligibility"]["rejected"],
        retried=report["queue"]["retried"],
        persisted=report["leaderboard"]["inserted"] + report["leaderboard"]["updated"],
        dead_lettered=report["queue"]["dead_letter_depth"],
    )
    logger.debug("durable discovery metrics: %s", report)
    return {
        "status": run_status,
        "company_id": company_id,
        "query": query,
        "company": company_name,
        "platform": platform,
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "results": _store_leaderboard(
            candidate_store,
            company_id=company_id,
            platform=platform,
            limit=n,
            max_age_days=settings.leaderboard_max_age_days,
        ),
        "metrics": report,
    }


@asynccontextmanager
async def _worker_batch_context(worker: Any):
    batch_context = getattr(worker, "batch_context", None)
    if callable(batch_context):
        async with batch_context():
            yield
    else:
        yield


def _snowball_result(
    output: dict[str, Any],
    *,
    status: str,
    checkpoint: SnowballCheckpoint,
    source_stats: dict[str, int],
) -> dict[str, Any]:
    result = dict(output)
    result["status"] = status
    result["snowball_progress"] = {
        "path": str(checkpoint.path),
        "active_seed_index": checkpoint.active_seed_index,
        "completed_seed_count": len(checkpoint.completed_seed_indices),
        "seed_count": len(checkpoint.seed_accounts),
        "submitted_child_count": len(checkpoint.submitted_children),
        "status": checkpoint.status,
    }
    result["snowball_source"] = dict(source_stats)
    return result


async def _run_snowball_with_store(
    n: int,
    settings: Settings,
    *,
    company_name: str,
    company_id: str,
    platform: str,
    company_domain: str,
    related_terms: list[str],
    candidate_store: Any,
    queue: Any,
    output_path: Path,
    minimum_seed_score: int,
    seed_limit: int | None,
    resume: bool,
    progress_path: Path,
    worker: Any | None = None,
    classifier: Any | None = None,
    embedding_matcher: Any | None = None,
    outcome_report: XFetchOutcomeReport | None = None,
) -> dict[str, Any]:
    """Expand Mongo seeds one hop through Following, then use the common drain."""

    if resume:
        checkpoint = SnowballCheckpoint.load(
            progress_path,
            company_id=company_id,
            platform=platform,
            output_path=output_path,
            minimum_seed_score=minimum_seed_score,
        )
        logger.info(
            "snowball state=resumed path=%s active_seed_index=%d submitted_children=%d",
            progress_path,
            checkpoint.active_seed_index,
            len(checkpoint.submitted_children),
        )
    else:
        pending_depth = queue.depth()
        if pending_depth > 0:
            raise PendingQueueWorkError(
                f"{platform} queue has {pending_depth} unfinished message(s); run snowball with --resume first"
            )
        logger.info(
            "snowball state=seed_selection_started company_id=%s minimum_score=%d",
            company_id,
            minimum_seed_score,
        )
        seed_accounts = candidate_store.seed_accounts(
            company_id=company_id,
            platform=platform,
            minimum_score=minimum_seed_score,
            limit=seed_limit,
        )
        checkpoint = SnowballCheckpoint.new(
            progress_path,
            company_id=company_id,
            platform=platform,
            output_path=output_path,
            minimum_seed_score=minimum_seed_score,
            seed_accounts=seed_accounts,
        )
        checkpoint.save()
        logger.info(
            "snowball state=seed_selection_completed selected=%d seed_limit=%s",
            len(checkpoint.seed_accounts),
            seed_limit if seed_limit is not None else "none",
        )

    # Dedicated snowball is one-hop only. Its children use the same consumer
    # but must not recursively expand their own Followings.
    consumer_settings = replace(settings, enable_snowball=False)
    worker = worker or SequentialProfileWorker(consumer_settings)
    classifier = classifier or _classifier_for(consumer_settings)
    embedding_matcher = embedding_matcher or LocalEmbeddingMatcher(
        consumer_settings.embedding_enabled,
        consumer_settings.embedding_model,
    )

    initial_output = await _run_durable_with_store(
        "snowball",
        n,
        consumer_settings,
        company_name=company_name,
        company_id=company_id,
        platform=platform,
        company_domain=company_domain,
        related_terms=related_terms,
        candidate_store=candidate_store,
        queue=queue,
        worker=worker,
        classifier=classifier,
        embedding_matcher=embedding_matcher,
        produce_discovery=False,
        outcome_report=outcome_report,
    )
    source_stats = {
        "seeds_selected": len(checkpoint.seed_accounts),
        "seeds_processed": len(checkpoint.completed_seed_indices),
        "seeds_failed": 0,
        "children_found": 0,
        "children_submitted": 0,
        "children_skipped": 0,
    }
    if initial_output.get("status") != "completed":
        return _snowball_result(
            initial_output,
            status=str(initial_output["status"]),
            checkpoint=checkpoint,
            source_stats=source_stats,
        )
    if checkpoint.status == "completed" or checkpoint.first_unfinished_seed() >= len(checkpoint.seed_accounts):
        checkpoint.set_status("completed")
        checkpoint.save()
        return _snowball_result(
            initial_output,
            status="completed",
            checkpoint=checkpoint,
            source_stats=source_stats,
        )

    submission_queue = _FreshProfileSubmissionQueue(
        queue,
        candidate_store,
        refresh_after_hours=consumer_settings.profile_refresh_after_hours,
        company_id=company_id,
        platform=platform,
        outcome_report=outcome_report,
    )
    stop_output: dict[str, Any] | None = None

    async with _worker_batch_context(worker):
        seed_index = checkpoint.first_unfinished_seed()
        while seed_index < len(checkpoint.seed_accounts):
            seed = checkpoint.seed_accounts[seed_index]
            seed_handle = seed.handle or seed.profile_url.rsplit("/", 1)[-1]
            logger.info(
                "snowball seed_index=%d/%d handle=@%s state=following_fetch_started",
                seed_index + 1,
                len(checkpoint.seed_accounts),
                seed_handle,
            )
            wait_before_handle = getattr(worker, "wait_before_handle", None)
            if callable(wait_before_handle):
                result = wait_before_handle(seed.profile_url or seed_handle)
                if inspect.isawaitable(result):
                    await result
            try:
                relationships = await asyncio.wait_for(
                    worker.fetch_following(seed_handle, consumer_settings.following_max_scrolls),
                    timeout=max(1, int(consumer_settings.account_attempt_timeout_seconds)),
                )
                relationship_urls = tuple(relationships.profile_urls)
            except XFollowingRateLimitError as exc:
                # The fetcher carries partial URLs so work found before the
                # shell is preserved before the controlled stop.
                relationship_urls = exc.partial_profile_urls
                for relationship_url in relationship_urls:
                    child = account_ref(platform, profile_url=relationship_url)
                    if child.account_id == seed.account_id or checkpoint.account_was_submitted(child):
                        source_stats["children_skipped"] += 1
                        continue
                    sent = _submit_snowball_account(
                        submission_queue,
                        child,
                        company_id=company_id,
                        platform=platform,
                    )
                    checkpoint.record_submitted(child)
                    checkpoint.save()
                    source_stats["children_found"] += 1
                    source_stats["children_submitted"] += int(sent)
                checkpoint.set_status("stopped_rate_limit")
                checkpoint.save()
                source_stats["seeds_processed"] = len(checkpoint.completed_seed_indices)
                logger.warning(
                    "snowball state=rate_limit_detected seed_index=%d/%d handle=@%s diagnostics=%s",
                    seed_index + 1,
                    len(checkpoint.seed_accounts),
                    seed_handle,
                    _safe_x_following_diagnostics(exc.diagnostics),
                )
                logger.warning(
                    "snowball state=checkpoint_saved path=%s active_seed_index=%d",
                    checkpoint.path,
                    checkpoint.active_seed_index,
                )
                logger.warning(
                    "snowball state=stopped_rate_limit resume_required=true"
                )
                stop_output = _snowball_result(
                    initial_output,
                    status="stopped_x_rate_limit",
                    checkpoint=checkpoint,
                    source_stats=source_stats,
                )
                break
            except Exception as exc:
                source_stats["seeds_failed"] += 1
                logger.warning(
                    "snowball seed_index=%d/%d handle=@%s state=following_fetch_failed reason=%s",
                    seed_index + 1,
                    len(checkpoint.seed_accounts),
                    seed_handle,
                    exc,
                )
                checkpoint.mark_seed_completed(seed_index)
                checkpoint.save()
                source_stats["seeds_processed"] = len(checkpoint.completed_seed_indices)
                seed_index = checkpoint.first_unfinished_seed()
                continue

            source_stats["children_found"] += len(relationship_urls)
            for relationship_url in relationship_urls:
                child = account_ref(platform, profile_url=relationship_url)
                if child.account_id == seed.account_id or checkpoint.account_was_submitted(child):
                    source_stats["children_skipped"] += 1
                    continue
                sent = _submit_snowball_account(
                    submission_queue,
                    child,
                    company_id=company_id,
                    platform=platform,
                )
                checkpoint.record_submitted(child)
                checkpoint.save()
                source_stats["children_submitted"] += int(sent)
            checkpoint.mark_seed_completed(seed_index)
            checkpoint.set_status("running")
            checkpoint.save()
            source_stats["seeds_processed"] = len(checkpoint.completed_seed_indices)
            logger.info(
                "snowball seed_index=%d/%d handle=@%s state=following_fetch_completed followed=%d submitted=%d skipped=%d",
                seed_index + 1,
                len(checkpoint.seed_accounts),
                seed_handle,
                len(relationship_urls),
                source_stats["children_submitted"],
                source_stats["children_skipped"],
            )
            seed_index = checkpoint.first_unfinished_seed()

    if stop_output is not None:
        return stop_output

    checkpoint.set_status("completed")
    checkpoint.save()
    logger.info(
        "snowball state=seed_expansion_completed seeds_selected=%d seeds_processed=%d seeds_failed=%d children_found=%d submitted=%d",
        source_stats["seeds_selected"],
        source_stats["seeds_processed"],
        source_stats["seeds_failed"],
        source_stats["children_found"],
        source_stats["children_submitted"],
    )
    logger.info("snowball state=queue_drain_started")
    final_output = await _run_durable_with_store(
        "snowball",
        n,
        consumer_settings,
        company_name=company_name,
        company_id=company_id,
        platform=platform,
        company_domain=company_domain,
        related_terms=related_terms,
        candidate_store=candidate_store,
        queue=queue,
        worker=worker,
        classifier=classifier,
        embedding_matcher=embedding_matcher,
        produce_discovery=False,
        outcome_report=outcome_report,
    )
    if final_output.get("status") != "completed":
        checkpoint.set_status("running")
        checkpoint.save()
        return _snowball_result(
            final_output,
            status=str(final_output["status"]),
            checkpoint=checkpoint,
            source_stats=source_stats,
        )
    logger.info("snowball state=completed")
    return _snowball_result(
        final_output,
        status="completed",
        checkpoint=checkpoint,
        source_stats=source_stats,
    )


async def run_durable_pipeline(
    query: str,
    n: int,
    settings: Settings,
    output_path: Path | None = None,
    *,
    company_id: str | None = None,
    company_name: str | None = None,
    platform: str | None = None,
    company_domain: str | None = None,
    related_terms: list[str] | None = None,
    candidate_store: Any | None = None,
    queue: Any | None = None,
    **dependencies: Any,
) -> dict[str, Any]:
    query, terms = _validate_durable_inputs(query, n, settings, company_name, company_domain, related_terms)
    assert company_name is not None and company_domain is not None
    resolved_company_id = company_id or DurableProfileQueue.company_slug(company_name)
    resolved_platform = validate_platform(platform or settings.platform)
    if not get_platform_adapter(resolved_platform).implemented:
        raise ValueError(f"platform adapter is not implemented yet: {resolved_platform}")
    outcome_report = XFetchOutcomeReport(
        company_name,
        settings.x_fetch_log_dir,
        company_id=resolved_company_id,
        platform=resolved_platform,
        refresh_after_hours=settings.profile_refresh_after_hours,
    )
    output: dict[str, Any] | None = None
    report_status = "failed"
    worker_lock = dependencies.pop("worker_lock", None) or ExclusiveLocalXWorkerLock(settings.x_worker_lock_path)
    try:
        with worker_lock:
            queue = queue or DurableProfileQueue.from_settings(
                settings,
                company_name,
                company_id=resolved_company_id,
                platform=resolved_platform,
            )
            if candidate_store is not None:
                output = await _run_durable_with_store(
                    query, n, settings, company_id=resolved_company_id, platform=resolved_platform,
                    company_name=company_name, company_domain=company_domain,
                    related_terms=terms, candidate_store=candidate_store, queue=queue,
                    outcome_report=outcome_report, **dependencies,
                )
            else:
                if not settings.mongodb_url:
                    raise ValueError("MONGODB_URI is required for durable company leaderboard persistence")
                with open_company_candidate_store(
                    settings.mongodb_url,
                    company_name,
                    company_id=resolved_company_id,
                    platform=resolved_platform,
                ) as store:
                    output = await _run_durable_with_store(
                        query, n, settings, company_id=resolved_company_id, platform=resolved_platform,
                        company_name=company_name, company_domain=company_domain,
                        related_terms=terms, candidate_store=store, queue=queue,
                        outcome_report=outcome_report, **dependencies,
                    )
            assert output is not None
            report_status = output["status"]
    except asyncio.CancelledError:
        report_status = "interrupted"
        raise
    finally:
        try:
            report_path = outcome_report.write(status=report_status)
            logger.debug("X-fetch outcome report saved: %s", report_path)
        except Exception:
            logger.exception("could not save X-fetch outcome report")
            report_path = None
    assert output is not None
    if report_path is not None:
        output["x_fetch_report"] = str(report_path)
    path = output_path or settings.data_dir / f"{_safe_filename(query)}_x_influencers.json"
    save_json(output, path)
    return output


async def run_snowball_pipeline(
    query: str,
    n: int,
    settings: Settings,
    output_path: Path | None = None,
    *,
    company_id: str | None = None,
    company_name: str | None = None,
    platform: str | None = None,
    company_domain: str | None = None,
    related_terms: list[str] | None = None,
    minimum_seed_score: int = 60,
    seed_limit: int | None = None,
    resume: bool = False,
    progress_path: Path | None = None,
    candidate_store: Any | None = None,
    queue: Any | None = None,
    **dependencies: Any,
) -> dict[str, Any]:
    """Run one-hop Following expansion from eligible MongoDB candidates."""

    query, terms = _validate_durable_inputs(
        query,
        n,
        settings,
        company_name,
        company_domain,
        related_terms,
    )
    assert company_name is not None and company_domain is not None
    resolved_company_id = company_id or DurableProfileQueue.company_slug(company_name)
    resolved_platform = validate_platform(platform or settings.platform)
    if resolved_platform != "x":
        raise ValueError("the snowball command currently supports only the X platform")
    output = output_path or settings.data_dir / f"{_safe_filename(query)}_x_influencers.json"
    resolved_progress_path = Path(progress_path) if progress_path else progress_path_for_output(output)
    if minimum_seed_score < 0:
        raise ValueError("minimum_seed_score must not be negative")
    if seed_limit is not None and seed_limit < 1:
        raise ValueError("seed_limit must be at least 1")

    outcome_report = XFetchOutcomeReport(
        company_name,
        settings.x_fetch_log_dir,
        company_id=resolved_company_id,
        platform=resolved_platform,
        refresh_after_hours=settings.profile_refresh_after_hours,
    )
    result: dict[str, Any] | None = None
    report_status = "failed"
    worker_lock = dependencies.pop("worker_lock", None) or ExclusiveLocalXWorkerLock(settings.x_worker_lock_path)
    try:
        with worker_lock:
            queue = queue or DurableProfileQueue.from_settings(
                settings,
                company_name,
                company_id=resolved_company_id,
                platform=resolved_platform,
            )
            if candidate_store is not None:
                result = await _run_snowball_with_store(
                    n,
                    settings,
                    company_name=company_name,
                    company_id=resolved_company_id,
                    platform=resolved_platform,
                    company_domain=company_domain,
                    related_terms=terms,
                    candidate_store=candidate_store,
                    queue=queue,
                    output_path=Path(output),
                    minimum_seed_score=minimum_seed_score,
                    seed_limit=seed_limit,
                    resume=resume,
                    progress_path=resolved_progress_path,
                    outcome_report=outcome_report,
                    **dependencies,
                )
            else:
                if not settings.mongodb_url:
                    raise ValueError("MONGODB_URI is required for durable company leaderboard persistence")
                with open_company_candidate_store(
                    settings.mongodb_url,
                    company_name,
                    company_id=resolved_company_id,
                    platform=resolved_platform,
                ) as store:
                    result = await _run_snowball_with_store(
                        n,
                        settings,
                        company_name=company_name,
                        company_id=resolved_company_id,
                        platform=resolved_platform,
                        company_domain=company_domain,
                        related_terms=terms,
                        candidate_store=store,
                        queue=queue,
                        output_path=Path(output),
                        minimum_seed_score=minimum_seed_score,
                        seed_limit=seed_limit,
                        resume=resume,
                        progress_path=resolved_progress_path,
                        outcome_report=outcome_report,
                        **dependencies,
                    )
            assert result is not None
            report_status = str(result["status"])
    except asyncio.CancelledError:
        report_status = "interrupted"
        raise
    finally:
        try:
            report_path = outcome_report.write(status=report_status)
            logger.debug("X-fetch outcome report saved: %s", report_path)
        except Exception:
            logger.exception("could not save X-fetch outcome report")
            report_path = None
    assert result is not None
    if report_path is not None:
        result["x_fetch_report"] = str(report_path)
    result["snowball_progress_file"] = str(resolved_progress_path)
    save_json(result, Path(output))
    return result


def evaluate_and_persist_profiles(
    profiles: list[XProfile],
    recent_posts_by_handle: dict[str, list[dict[str, Any]]],
    classification_labels: dict[str, str],
    semantic_similarities: dict[str, float],
    *,
    related_terms: list[str],
    company_name: str,
    company_id: str | None = None,
    platform: str = "x",
    company_domain: str | None,
    settings: Settings,
    candidate_store: Any,
) -> dict[str, int]:
    """Evaluate a fixture or fetched batch and persist only eligible profiles.

    ``candidate_store`` is deliberately a tiny interface (``upsert`` and
    ``leaderboard``) so this boundary is testable with a fake and reusable by
    the durable queue consumer.
    """
    if not settings.company_summary:
        raise ValueError("company summary is required for candidate evaluation")
    company_id = company_id or DurableProfileQueue.company_slug(company_name)
    platform = validate_platform(platform)
    counts = {"eligible": 0, "ineligible": 0, "inserted": 0, "updated": 0}
    for profile in profiles:
        handle = profile.handle.lower()
        result = evaluate_candidate(
            profile,
            recent_posts_by_handle.get(handle, []),
            related_terms=related_terms,
            company_summary=settings.company_summary,
            minimum_followers=settings.minimum_followers,
            minimum_relevance_score=settings.minimum_relevance_score,
            company_name=company_name,
            company_domain=company_domain,
            account_label=classification_labels.get(handle),
            semantic_similarity=semantic_similarities.get(handle),
        )
        if not result.eligible:
            _store_delete(
                candidate_store,
                profile.profile_url,
                company_id=company_id,
                platform=platform,
            )
            counts["ineligible"] += 1
            continue
        counts["eligible"] += 1
        assert result.score is not None
        operation = _store_upsert(
            candidate_store,
            profile,
            recent_posts_by_handle.get(handle, []),
            company_id=company_id,
            company_name=company_name,
            platform=platform,
            final_score=result.score.score_breakdown.total,
        )
        counts[operation] += 1
    return counts


async def run_pipeline(
    query: str,
    n: int,
    settings: Settings,
    output_path: Path | None = None,
    **request: Any,
) -> dict[str, Any]:
    """Public runtime entry point for the durable queue-backed pipeline."""
    return await run_durable_pipeline(query, n, settings, output_path, **request)


async def _run_with_shutdown_signals(
    query: str,
    n: int,
    settings: Settings,
    output_path: Path | None,
    request: dict[str, Any],
    pipeline_runner: Any = run_pipeline,
) -> dict[str, Any]:
    """Cancel the active run on an operator stop so normal cleanup can execute.

    ``SIGTERM`` otherwise ends a Python process immediately. Cancelling the
    coroutine instead lets browser contexts, SQS visibility heartbeats, and
    worker-lock context managers unwind. A received-but-unacknowledged queue
    message is deliberately left for SQS redelivery.
    """
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    assert task is not None
    requested_signal: signal.Signals | None = None
    registered_signals: list[signal.Signals] = []

    def request_shutdown(received_signal: signal.Signals) -> None:
        nonlocal requested_signal
        if requested_signal is None:
            requested_signal = received_signal
            logger.info(
                "received %s; stopping safely after releasing browser and worker resources",
                received_signal.name,
            )
        task.cancel()

    for received_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(received_signal, request_shutdown, received_signal)
        except (NotImplementedError, RuntimeError):
            # The CLI's KeyboardInterrupt handling remains a safe fallback on
            # platforms whose event loop does not support signal callbacks.
            continue
        registered_signals.append(received_signal)

    try:
        return await pipeline_runner(query, n, settings, output_path, **request)
    except asyncio.CancelledError:
        if requested_signal is None:
            raise
        raise GracefulShutdownRequested(requested_signal.name) from None
    finally:
        for received_signal in registered_signals:
            loop.remove_signal_handler(received_signal)


def run(
    query: str,
    n: int,
    settings: Settings,
    output_path: Path | None = None,
    **request: Any,
) -> dict[str, Any]:
    return asyncio.run(_run_with_shutdown_signals(query, n, settings, output_path, request))


def run_snowball(
    query: str,
    n: int,
    settings: Settings,
    output_path: Path | None = None,
    **request: Any,
) -> dict[str, Any]:
    return asyncio.run(
        _run_with_shutdown_signals(
            query,
            n,
            settings,
            output_path,
            request,
            pipeline_runner=run_snowball_pipeline,
        )
    )


def _dedupe_posts(posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    seen = set()
    for post in posts:
        key = post.get("url") or (post.get("created_at"), str(post.get("text") or "")[:80])
        if key in seen:
            continue
        seen.add(key)
        output.append(post)
    return output


def _ranking_document(profile: XProfile, posts: list[dict[str, Any]]) -> str:
    sample = _latest_five_posts(posts)
    post_text = " ".join(str(post.get("text") or "") for post in sample)
    return f"{profile.bio or ''} {post_text}".strip()


def _latest_five_posts(posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    non_pinned = [post for post in _dedupe_posts(posts) if not post.get("is_pinned")]
    return sorted(
        non_pinned,
        key=lambda post: str(post.get("created_at") or post.get("posted_at") or ""),
        reverse=True,
    )[:5]
