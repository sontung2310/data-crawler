# Data-Crawler-Task

Local-only crawl API isolated from Pace-Unit. Accepts a query (HTTP or SQS), crawls selected sources in parallel (one keyword per task), writes each post/comment to MongoDB as soon as it is scraped, optionally publishes `raw_collected` events to SQS, and exposes task status.

Pace-Unit is **not** modified; this project is a copy + API wrapper.

## Requirements

- Python 3.11+
- MongoDB (local or Atlas)
- Playwright Chromium (for X / Reddit)
- Optional: YouTube / NewsAPI / Guardian / Reddit OAuth keys

## Setup

```bash
cd Data-Crawler-Task
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
cp .env.example .env
# Fill API keys / Mongo. Or keep Pace-Unit credentials/backend.env —
# config.py loads it automatically when present (local .env overrides).
```

Run (always **one** uvicorn worker):

```bash
uvicorn app.main:app --host 127.0.0.1 --port 8000 --workers 1
```

Smoke check:

```bash
curl http://127.0.0.1:8000/health
```

## Docker

```bash
docker build -t data-crawler-task .
# Mongo on the host (Mac/Windows):
docker run --rm -p 8000:8000 --shm-size=1gb \
  -e MONGODB_URI=mongodb://host.docker.internal:27017 \
  --env-file .env data-crawler-task
```

`--shm-size=1gb` is required for Chromium. Do not use `mongodb://localhost` inside the container unless Mongo runs in the same container network.

## API

### `POST /crawl`

```json
{
  "query": "agentic AI marketing",
  "source": "all",
  "time_delta": "day",
  "limit": 2
}
```

| Field | Notes |
|-------|--------|
| `query` | Required **search keywords** (same string for every adapter). Prefer short phrases over chat questions — see [QUERY_GUIDANCE.md](QUERY_GUIDANCE.md) |
| `source` | `x` \| `reddit` \| `youtube` \| `duckduckgo` \| `news` \| `all` (omit = all) |
| `time_delta` | `1`/`7`/`30`/`365` or `hour`/`day`/`week`/`month`/`year` |
| `limit` | Max posts per adapter (default 50). Comments always included for X/Reddit/YouTube |

Response:

```json
{ "task_id": "uuid", "status": "accepted" }
```

### `GET /crawl/{task_id}`

Progress, per-source status, errors (including `session_expired`), warnings.

### `GET /sources`

Category → adapter map.

### `GET /health`

Liveness.

Optional header `X-API-Key` if `CRAWL_API_KEY` is set in `.env`.

## Source categories

| Category | Adapters |
|----------|----------|
| `x` | x_playwright |
| `reddit` | reddit_playwright |
| `youtube` | youtube_api |
| `duckduckgo` | duckduckgo_web |
| `news` | google_news, bbc, techcrunch, guardian, hackernews, newsapi, reddit_official, reddit_rss |
| `all` | everything above |

Threading: one query; adapters run in a thread pool. X and Reddit each use one browser in their own thread (can run in parallel with each other and with HTTP sources). No multi-thread inside a single Playwright session.

## Sessions (X / Reddit)

Set in `.env`:

- `X_AUTH_TOKEN`, `X_CT0` from x.com cookies after login
- `REDDIT_SESSION` (optional `REDDIT_TOKEN_V2`) from reddit.com cookies

On expiry / login wall: task error `session_expired`, log line, and optional email if SMTP env vars are set.

## MongoDB

Collections: `raw_posts`, `raw_comments`, `crawl_tasks`.

Unique key for content: `(source, external_id)`. Each row includes `query` (crawl keyword) and may include `task_id`.

X/Reddit via the API: X uses multi-mode search (`top`+`live`); Reddit query search is **relevance-only**. Both dedupe by post id before comment crawl. X skips low-discussion posts on `live` (`min_replies >= 10`). Default comment hard cap for X/Reddit is 200.

## Relevance eval

```bash
python -m tests.eval_relevent --query "AI trending in Marketing" --limit 10
python -m tests.eval_relevent --query "AI" --source x_playwright --json report.json
```

Requires `OPENAI_API_KEY` (model default `gpt-5.6-luna` via `EVAL_MODEL`). Crawls adapters directly — no Mongo writes.

## SQS

Two optional queues. Both need `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_REGION`.

| Queue | Env | Role |
|-------|-----|------|
| Command | `AWS_SQS_COMMAND_QUEUE_URL` | Long-poll crawl jobs → same path as `POST /crawl` |
| Response | `AWS_SQS_QUEUE_URL` | Publish `raw_collected` after each Mongo write |

Set `SQS_COMMAND_CONSUMER_ENABLED=0` to disable the consumer. Omit a queue URL to skip that side. Response publish failures become task warnings (`sqs_error`) and never fail the crawl.

### Command message

```json
{
  "query": "agentic AI marketing",
  "source": "all",
  "time_delta": "day",
  "limit": 2,
  "job_id": "optional-stable-id"
}
```

Same fields as `POST /crawl`. `job_id` becomes `task_id` (otherwise a new UUID). Message is deleted after the task is **accepted**, not after the crawl finishes. If the same `job_id` is already in Mongo, the consumer skips starting another crawl (safe redelivery). Failed parse/accept leaves the message for visibility timeout retry / DLQ (configure redrive in AWS).

### Response event

```json
{
  "event_id": "uuid",
  "schema_version": 1,
  "event_type": "raw_collected",
  "content_type": "post",
  "source": "x_playwright",
  "external_id": "...",
  "parent_content_id": null,
  "history_id": "<task_id>",
  "occurred_at": "2026-01-01T00:00:00Z",
  "payload": { }
}
```

`content_type` is `post` or `comment`. One SQS message per scraped item.

## Timeouts

Expected Playwright runtime at `limit=50` with comments: up to ~2 hours per source. Task wall-clock timeout: `CRAWL_TASK_TIMEOUT_SEC` (default 10800 = 3 hours).
