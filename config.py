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
PORT = int(os.environ.get("PORT", "8000"))
CRAWL_API_KEY = (os.environ.get("CRAWL_API_KEY") or "").strip() or None

# Crawl runtime
CRAWL_MAX_WORKERS = int(os.environ.get("CRAWL_MAX_WORKERS", "4"))
CRAWL_PW_X_SLOTS = int(os.environ.get("CRAWL_PW_X_SLOTS", "1"))
CRAWL_PW_REDDIT_SLOTS = int(os.environ.get("CRAWL_PW_REDDIT_SLOTS", "1"))
CRAWL_TASK_TIMEOUT_SEC = int(os.environ.get("CRAWL_TASK_TIMEOUT_SEC", "10800"))  # 3h
DEFAULT_FETCH_LIMIT = int(os.environ.get("DEFAULT_FETCH_LIMIT", "50"))

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
REDDIT_SESSION = (os.environ.get("REDDIT_SESSION") or "").strip()
REDDIT_TOKEN_V2 = (os.environ.get("REDDIT_TOKEN_V2") or "").strip()

# MongoDB
MONGODB_URI = os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DBNAME = os.environ.get("MONGODB_DBNAME", "pace_database")
MONGODB_TLS = os.environ.get("MONGODB_TLS", "").lower() in ("1", "true", "yes")

# SQS (optional — NullPublisher until fully configured)
AWS_SQS_QUEUE_URL = (os.environ.get("AWS_SQS_QUEUE_URL") or "").strip()
AWS_REGION = (os.environ.get("AWS_REGION") or "us-east-1").strip()
AWS_ACCESS_KEY_ID = (os.environ.get("AWS_ACCESS_KEY_ID") or "").strip()
AWS_SECRET_ACCESS_KEY = (os.environ.get("AWS_SECRET_ACCESS_KEY") or "").strip()

# Email alerts (optional)
ALERT_EMAIL_TO = (os.environ.get("ALERT_EMAIL_TO") or "").strip()
ALERT_EMAIL_FROM = (os.environ.get("ALERT_EMAIL_FROM") or "").strip()
SMTP_HOST = (os.environ.get("SMTP_HOST") or "").strip()
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = (os.environ.get("SMTP_USER") or "").strip()
SMTP_PASSWORD = (os.environ.get("SMTP_PASSWORD") or "").strip()
SMTP_USE_TLS = os.environ.get("SMTP_USE_TLS", "1").lower() in ("1", "true", "yes")
