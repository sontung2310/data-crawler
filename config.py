"""Local Data-Crawler-Task configuration (env / .env)."""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
# Local .env wins; optionally reuse Pace-Unit credentials if present.
_pace_backend_env = (
    BASE_DIR.parent / "Pace-Unit" / "code" / "Backend" / "credentials" / "backend.env"
)
if _pace_backend_env.exists():
    load_dotenv(_pace_backend_env, override=False)
load_dotenv(BASE_DIR / ".env", override=True)

# API
HOST = os.environ.get("HOST", "127.0.0.1")
PORT = int(os.environ.get("PORT", "8001"))
CRAWL_API_KEY = (os.environ.get("CRAWL_API_KEY") or "").strip() or None

# Crawl runtime
CRAWL_MAX_WORKERS = int(os.environ.get("CRAWL_MAX_WORKERS", "4"))
CRAWL_PW_REDDIT_SLOTS = int(os.environ.get("CRAWL_PW_REDDIT_SLOTS", "1"))
CRAWL_TASK_TIMEOUT_SEC = int(os.environ.get("CRAWL_TASK_TIMEOUT_SEC", "10800"))  # 3h
DEFAULT_FETCH_LIMIT = int(os.environ.get("DEFAULT_FETCH_LIMIT", "50"))
# Influencer discovery runtime. These settings deliberately use a separate
# namespace from the article response queue: influencer profile work stays
# inside Data-Crawler-Task and is never forwarded to Pace's response queue.
INFLUENCER_DATA_DIR = Path(
    os.environ.get("INFLUENCER_DATA_DIR", str(BASE_DIR / "data" / "influencers"))
)
INFLUENCER_SQS_ENDPOINT_URL = (
    os.environ.get("INFLUENCER_SQS_ENDPOINT_URL")
    or os.environ.get("SQS_ENDPOINT_URL")
    or ""
).strip()
INFLUENCER_SQS_QUEUE_NAME = (
    os.environ.get("INFLUENCER_SQS_QUEUE_NAME")
    or os.environ.get("SQS_QUEUE_NAME")
    or "x-profile-jobs.fifo"
).strip()
INFLUENCER_SQS_VISIBILITY_TIMEOUT_SECONDS = int(
    os.environ.get("INFLUENCER_SQS_VISIBILITY_TIMEOUT_SECONDS", "180")
)
INFLUENCER_SQS_RECEIVE_BATCH_SIZE = int(
    os.environ.get("INFLUENCER_SQS_RECEIVE_BATCH_SIZE", "10")
)
INFLUENCER_SQS_RECEIVE_WAIT_TIME_SECONDS = int(
    os.environ.get("INFLUENCER_SQS_RECEIVE_WAIT_TIME_SECONDS", "2")
)
INFLUENCER_SQS_IDLE_POLL_SECONDS = int(
    os.environ.get("INFLUENCER_SQS_IDLE_POLL_SECONDS", "5")
)
INFLUENCER_SQS_MAX_RECEIVE_COUNT = int(
    os.environ.get("INFLUENCER_SQS_MAX_RECEIVE_COUNT", "1")
)
INFLUENCER_ACCOUNT_ATTEMPT_TIMEOUT_SECONDS = int(
    os.environ.get("INFLUENCER_ACCOUNT_ATTEMPT_TIMEOUT_SECONDS", "30")
)
INFLUENCER_X_AUTHORS_PER_QUERY = int(
    os.environ.get("X_AUTHORS_PER_QUERY", "20")
)
INFLUENCER_X_MAX_SCROLLS_PER_QUERY = int(
    os.environ.get("X_MAX_SCROLLS_PER_QUERY", "8")
)
INFLUENCER_PUBLIC_CANDIDATES_PER_ARTICLE = int(
    os.environ.get("PUBLIC_CANDIDATES_PER_ARTICLE", "40")
)
INFLUENCER_EMBEDDING_MODEL = os.environ.get(
    "EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2"
)
INFLUENCER_CLASSIFICATION_BACKEND = os.environ.get(
    "CLASSIFICATION_BACKEND", "local"
).strip().lower()
INFLUENCER_LOCAL_CLASSIFIER_MODEL = Path(
    os.environ.get(
        "LOCAL_CLASSIFIER_MODEL",
        str(BASE_DIR / "models" / "name-classifier"),
    )
)
INFLUENCER_LOCAL_CLASSIFIER_DEVICE = os.environ.get(
    "LOCAL_CLASSIFIER_DEVICE", "auto"
)
INFLUENCER_HEADLESS = os.environ.get("INFLUENCER_HEADLESS", "true").lower() in (
    "1",
    "true",
    "yes",
)
INFLUENCER_PROFILE_REFRESH_AFTER_HOURS = int(
    os.environ.get("PROFILE_REFRESH_AFTER_HOURS", "24")
)
INFLUENCER_LEADERBOARD_MAX_AGE_DAYS = int(
    os.environ.get("LEADERBOARD_MAX_AGE_DAYS", "30")
)
INFLUENCER_ENABLE_SNOWBALL = os.environ.get("ENABLE_SNOWBALL", "false").lower() in (
    "1",
    "true",
    "yes",
)
INFLUENCER_MINIMUM_FOLLOWERS = int(
    os.environ.get("MINIMUM_FOLLOWERS", "10000")
)
INFLUENCER_MINIMUM_RELEVANCE_SCORE = float(
    os.environ.get("MINIMUM_RELEVANCE_SCORE", "0.20")
)
INFLUENCER_GOOD_HYBRID_RELEVANCE_THRESHOLD = float(
    os.environ.get("GOOD_HYBRID_RELEVANCE_THRESHOLD", "0.45")
)
INFLUENCER_FOLLOWING_MAX_SCROLLS = int(
    os.environ.get("FOLLOWING_MAX_SCROLLS", "8")
)
INFLUENCER_WORKER_LOCK_PATH = Path(
    os.environ.get(
        "X_WORKER_LOCK_PATH",
        "/tmp/x-influencer-discovery.x-worker.lock",
    )
)
INFLUENCER_FETCH_LOG_DIR = Path(
    os.environ.get("X_FETCH_LOG_DIR", str(BASE_DIR / "logs"))
)
INFLUENCER_X_ACCESS_FAILURE_STREAK_LIMIT = int(
    os.environ.get("X_ACCESS_FAILURE_STREAK_LIMIT", "5")
)

# Third-party API keys
GUARDIAN_API_KEY = os.environ.get("GUARDIAN_API_KEY")
NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY")
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")

# OpenAI (relevance eval)
OPENAI_API_KEY = (os.environ.get("OPENAI_API_KEY") or "").strip()
EVAL_MODEL = (os.environ.get("EVAL_MODEL") or "gpt-5.6-luna").strip()

# Reddit OAuth (official / utils)
REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.environ.get("REDDIT_USER_AGENT")

# Playwright sessions (from browser DevTools cookies)
X_AUTH_TOKEN = (os.environ.get("X_AUTH_TOKEN") or "").strip()
X_CT0 = (os.environ.get("X_CT0") or "").strip()
X_BROWSER_SESSION = (
    os.environ.get("X_BROWSER_SESSION")
    or os.environ.get("X_browser_session")
    or os.environ.get("X_SESSION")
    or os.environ.get("X_session")
    or ""
).strip()
REDDIT_SESSION = (os.environ.get("REDDIT_SESSION") or "").strip()
REDDIT_TOKEN_V2 = (os.environ.get("REDDIT_TOKEN_V2") or "").strip()

# MongoDB
MONGODB_URI = (
    os.environ.get("MONGODB_URI")
    or os.environ.get("MONGODB_URL")
    or "mongodb://localhost:27017"
)
MONGODB_DBNAME = os.environ.get("MONGODB_DBNAME", "pace_database")
MONGODB_TLS = os.environ.get("MONGODB_TLS", "").lower() in ("1", "true", "yes")

# Dashboard MongoDB is intentionally separate from DCT's candidate database.
# The generic names remain accepted by the standalone export command.
DASHBOARD_DATABASE_HOST = (
    os.environ.get("DASHBOARD_DATABASE_HOST") or os.environ.get("DATABASE_HOST") or ""
).strip()
DASHBOARD_DATABASE_NAME = (
    os.environ.get("DASHBOARD_DATABASE_NAME") or os.environ.get("DATABASE_NAME") or ""
).strip()
DASHBOARD_DATABASE_USERNAME = (
    os.environ.get("DASHBOARD_DATABASE_USERNAME") or os.environ.get("DATABASE_USERNAME") or ""
).strip()
DASHBOARD_DATABASE_PASSWORD = (
    os.environ.get("DASHBOARD_DATABASE_PASSWORD") or os.environ.get("DATABASE_PASSWORD") or ""
).strip()

# AWS / SQS
AWS_ACCESS_KEY_ID = (os.environ.get("AWS_ACCESS_KEY_ID") or "").strip()
AWS_SECRET_ACCESS_KEY = (os.environ.get("AWS_SECRET_ACCESS_KEY") or "").strip()
AWS_REGION = (os.environ.get("AWS_REGION") or "ap-southeast-2").strip()
# Publish raw_collected article events here. Influencer results stay in DCT.
AWS_SQS_QUEUE_URL = (
    os.environ.get("AWS_SQS_QUEUE_URL")
    or os.environ.get("SQS_AI_QUEUE_URL")
    or ""
).strip()
# Long-poll crawl commands from Pace-Unit
AWS_SQS_COMMAND_QUEUE_URL = (
    os.environ.get("AWS_SQS_COMMAND_QUEUE_URL")
    or os.environ.get("SQS_COMMAND_QUEUE_URL")
    or ""
).strip()
SQS_WAIT_TIME_SECONDS = int(os.environ.get("SQS_WAIT_TIME_SECONDS", "20"))
SQS_COMMAND_CONSUMER_ENABLED = os.environ.get("SQS_COMMAND_CONSUMER_ENABLED", "1").lower() in (
    "1",
    "true",
    "yes",
)
# Email alerts (optional)
ALERT_EMAIL_TO = (os.environ.get("ALERT_EMAIL_TO") or "").strip()
ALERT_EMAIL_FROM = (os.environ.get("ALERT_EMAIL_FROM") or "").strip()
SMTP_HOST = (os.environ.get("SMTP_HOST") or "").strip()
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = (os.environ.get("SMTP_USER") or "").strip()
SMTP_PASSWORD = (os.environ.get("SMTP_PASSWORD") or "").strip()
SMTP_USE_TLS = os.environ.get("SMTP_USE_TLS", "1").lower() in ("1", "true", "yes")
