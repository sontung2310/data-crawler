from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .platforms import validate_platform

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


@dataclass(frozen=True)
class Settings:
    mongodb_url: str | None = None
    database_host: str | None = None
    database_name: str | None = None
    database_username: str | None = None
    database_password: str | None = None
    openai_api_key: str | None = None
    openai_model: str = "gpt-5-nano"
    classification_backend: str = "local"
    local_classifier_model: Path = Path("models/name-classifier")
    local_classifier_device: str = "auto"
    x_session: str | None = None
    embedding_enabled: bool = True
    embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    headless: bool = True
    data_dir: Path = Path("data/influencers")
    public_web_num_queries: int = 6
    x_topic_query_count: int = 6
    x_authors_per_query: int = 20
    x_max_scrolls_per_query: int = 8
    public_candidates_per_article: int = 40
    embedding_batch_size: int = 32
    platform: str = "x"
    candidate_collection_name: str = "influencer_candidates"
    company_summary: str | None = None
    minimum_followers: int = 10_000
    minimum_relevance_score: float = 0.20
    good_hybrid_relevance_threshold: float = 0.60
    profile_refresh_after_hours: int = 24
    leaderboard_max_age_days: int = 30
    enable_snowball: bool = False
    following_max_scrolls: int = 8
    sqs_endpoint_url: str = "http://localhost:4566"
    sqs_queue_name: str | None = None
    sqs_visibility_timeout_seconds: int = 180
    sqs_receive_batch_size: int = 10
    sqs_receive_wait_time_seconds: int = 2
    sqs_idle_poll_seconds: int = 5
    sqs_max_receive_count: int = 1
    account_attempt_timeout_seconds: int = 30
    x_local_retry_delay_seconds: float = 0.0
    x_handle_delay_min_seconds: float = 3.0
    x_handle_delay_max_seconds: float = 6.0
    x_scroll_delay_min_seconds: float = 1.5
    x_scroll_delay_max_seconds: float = 3.0
    playwright_concurrency: int = 1
    x_worker_lock_path: Path = Path("/tmp/x-influencer-discovery.x-worker.lock")
    x_fetch_log_dir: Path = Path("logs")
    x_access_failure_streak_limit: int = 3


def _env_text(*names: str) -> str:
    for name in names:
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    return ""


def resolve_x_session() -> str | None:
    """Build the authenticated X session from ``X_AUTH_TOKEN`` and ``X_CT0``.

    Influencer discovery uses the same cookie pair as content crawl. It does
    not read ``X_SESSION`` / ``X_BROWSER_SESSION``.
    """
    token = _env_text("X_AUTH_TOKEN")
    ct0 = _env_text("X_CT0")
    if token and ct0:
        return f"auth_token={token}; ct0={ct0}"
    return None


def load_settings(env_file: str | None = None) -> Settings:
    if load_dotenv:
        load_dotenv(env_file or ".env")
    defaults = Settings()

    def _integer(name: str, default: int, *, minimum: int = 1) -> int:
        value = int(os.getenv(name, str(default)))
        if value < minimum:
            raise ValueError(f"{name} must be at least {minimum}")
        return value

    def _score(name: str, default: float) -> float:
        value = float(os.getenv(name, str(default)))
        if not 0 <= value <= 1:
            raise ValueError(f"{name} must be between 0 and 1")
        return value

    def _duration(name: str, default: float) -> float:
        value = float(os.getenv(name, str(default)))
        if value < 0:
            raise ValueError(f"{name} must not be negative")
        return value

    def _duration_range(
        min_name: str,
        max_name: str,
        default_min: float,
        default_max: float,
    ) -> tuple[float, float]:
        minimum = _duration(min_name, default_min)
        maximum = _duration(max_name, default_max)
        if maximum < minimum:
            raise ValueError(f"{max_name} must be greater than or equal to {min_name}")
        return minimum, maximum

    def _boolean(name: str, default: bool) -> bool:
        value = os.getenv(name)
        return default if value is None else value.lower() == "true"

    playwright_concurrency = _integer("PLAYWRIGHT_CONCURRENCY", defaults.playwright_concurrency)
    if playwright_concurrency != 1:
        raise ValueError("PLAYWRIGHT_CONCURRENCY must be exactly 1")

    sqs_receive_batch_size = _integer("SQS_RECEIVE_BATCH_SIZE", defaults.sqs_receive_batch_size, minimum=1)
    if sqs_receive_batch_size > 10:
        raise ValueError("SQS_RECEIVE_BATCH_SIZE must be at most 10")
    sqs_receive_wait_time_seconds = _integer(
        "SQS_RECEIVE_WAIT_TIME_SECONDS", defaults.sqs_receive_wait_time_seconds, minimum=0
    )
    if sqs_receive_wait_time_seconds > 20:
        raise ValueError("SQS_RECEIVE_WAIT_TIME_SECONDS must be at most 20")
    sqs_idle_poll_seconds = _integer("SQS_IDLE_POLL_SECONDS", defaults.sqs_idle_poll_seconds, minimum=1)
    if sqs_idle_poll_seconds > 60:
        raise ValueError("SQS_IDLE_POLL_SECONDS must be at most 60")
    x_access_failure_streak_limit = _integer(
        "X_ACCESS_FAILURE_STREAK_LIMIT", defaults.x_access_failure_streak_limit
    )
    if x_access_failure_streak_limit > sqs_receive_batch_size:
        raise ValueError("X_ACCESS_FAILURE_STREAK_LIMIT must not exceed SQS_RECEIVE_BATCH_SIZE")

    x_handle_delay_min_seconds, x_handle_delay_max_seconds = _duration_range(
        "X_HANDLE_DELAY_MIN_SECONDS",
        "X_HANDLE_DELAY_MAX_SECONDS",
        defaults.x_handle_delay_min_seconds,
        defaults.x_handle_delay_max_seconds,
    )
    x_scroll_delay_min_seconds, x_scroll_delay_max_seconds = _duration_range(
        "X_SCROLL_DELAY_MIN_SECONDS",
        "X_SCROLL_DELAY_MAX_SECONDS",
        defaults.x_scroll_delay_min_seconds,
        defaults.x_scroll_delay_max_seconds,
    )

    platform = validate_platform(os.getenv("PLATFORM", defaults.platform))

    return Settings(
        mongodb_url=os.getenv("MONGODB_URI") or defaults.mongodb_url,
        database_host=(os.getenv("DATABASE_HOST") or os.getenv("DASHBOARD_DATABASE_HOST") or defaults.database_host),
        database_name=(os.getenv("DATABASE_NAME") or os.getenv("DASHBOARD_DATABASE_NAME") or defaults.database_name),
        database_username=(os.getenv("DATABASE_USERNAME") or os.getenv("DASHBOARD_DATABASE_USERNAME") or defaults.database_username),
        database_password=(os.getenv("DATABASE_PASSWORD") or os.getenv("DASHBOARD_DATABASE_PASSWORD") or defaults.database_password),
        openai_api_key=os.getenv("OPENAI_API") or os.getenv("OPENAI_API_KEY") or defaults.openai_api_key,
        openai_model=os.getenv("OPENAI_MODEL", defaults.openai_model),
        classification_backend=os.getenv("CLASSIFICATION_BACKEND", defaults.classification_backend).lower(),
        local_classifier_model=Path(os.getenv("LOCAL_CLASSIFIER_MODEL", str(defaults.local_classifier_model))),
        local_classifier_device=os.getenv("LOCAL_CLASSIFIER_DEVICE", defaults.local_classifier_device),
        x_session=resolve_x_session() or defaults.x_session,
        embedding_enabled=_boolean("EMBEDDING_ENABLED", defaults.embedding_enabled),
        embedding_model=os.getenv("EMBEDDING_MODEL", defaults.embedding_model),
        headless=_boolean("HEADLESS", defaults.headless),
        data_dir=Path(os.getenv("DATA_DIR", str(defaults.data_dir))),
        platform=platform,
        candidate_collection_name=os.getenv("CANDIDATE_COLLECTION_NAME", defaults.candidate_collection_name),
        company_summary=os.getenv("COMPANY_SUMMARY") or defaults.company_summary,
        minimum_followers=_integer("MINIMUM_FOLLOWERS", defaults.minimum_followers),
        minimum_relevance_score=_score("MINIMUM_RELEVANCE_SCORE", defaults.minimum_relevance_score),
        good_hybrid_relevance_threshold=_score(
            "GOOD_HYBRID_RELEVANCE_THRESHOLD", defaults.good_hybrid_relevance_threshold
        ),
        profile_refresh_after_hours=_integer("PROFILE_REFRESH_AFTER_HOURS", defaults.profile_refresh_after_hours),
        leaderboard_max_age_days=_integer(
            "LEADERBOARD_MAX_AGE_DAYS", defaults.leaderboard_max_age_days
        ),
        enable_snowball=_boolean("ENABLE_SNOWBALL", defaults.enable_snowball),
        following_max_scrolls=_integer("FOLLOWING_MAX_SCROLLS", defaults.following_max_scrolls, minimum=0),
        sqs_endpoint_url=os.getenv("SQS_ENDPOINT_URL", defaults.sqs_endpoint_url),
        sqs_queue_name=os.getenv("SQS_QUEUE_NAME") or defaults.sqs_queue_name,
        sqs_visibility_timeout_seconds=_integer(
            "SQS_VISIBILITY_TIMEOUT_SECONDS", defaults.sqs_visibility_timeout_seconds, minimum=60
        ),
        sqs_receive_batch_size=sqs_receive_batch_size,
        sqs_receive_wait_time_seconds=sqs_receive_wait_time_seconds,
        sqs_idle_poll_seconds=sqs_idle_poll_seconds,
        sqs_max_receive_count=_fast_fail_receive_count(defaults.sqs_max_receive_count),
        account_attempt_timeout_seconds=_integer(
            "ACCOUNT_ATTEMPT_TIMEOUT_SECONDS", defaults.account_attempt_timeout_seconds
        ),
        x_local_retry_delay_seconds=_duration("X_LOCAL_RETRY_DELAY_SECONDS", defaults.x_local_retry_delay_seconds),
        x_handle_delay_min_seconds=x_handle_delay_min_seconds,
        x_handle_delay_max_seconds=x_handle_delay_max_seconds,
        x_scroll_delay_min_seconds=x_scroll_delay_min_seconds,
        x_scroll_delay_max_seconds=x_scroll_delay_max_seconds,
        playwright_concurrency=playwright_concurrency,
        x_worker_lock_path=Path(os.getenv("X_WORKER_LOCK_PATH", str(defaults.x_worker_lock_path))),
        x_fetch_log_dir=Path(os.getenv("X_FETCH_LOG_DIR", str(defaults.x_fetch_log_dir))),
        x_access_failure_streak_limit=x_access_failure_streak_limit,
    )


def _fast_fail_receive_count(default: int | None = None) -> int:
    default = Settings().sqs_max_receive_count if default is None else default
    value = int(os.getenv("SQS_MAX_RECEIVE_COUNT", str(default)))
    if value != 1:
        raise ValueError("SQS_MAX_RECEIVE_COUNT must be exactly 1 for the fast-fail delivery policy")
    return value
