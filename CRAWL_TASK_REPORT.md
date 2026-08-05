# Crawl Task — Progress Report

**Project:** Data-Crawler-Task (isolated crawl service)  
**Note:** This is **not** the Pace-Unit Backend Kafka pipeline. This service writes to MongoDB (and optionally SQS). Kafka is used in Pace-Unit separately.

---

## Section 1: Sources Available

Sources are grouped by **API category** and **technique**:

| Category | Adapters | Technique |
|----------|----------|-----------|
| `x` | `x_playwright` | Playwright (browser) |
| `reddit` | `reddit_playwright` | Playwright (browser) |
| `youtube` | `youtube_api` | YouTube Data API v3 |
| `duckduckgo` | `duckduckgo_web` | Web search + page extract |
| `news` | `google_news`, `bbc`, `techcrunch`, `guardian`, `hackernews`, `newsapi`, `reddit_official`, `reddit_rss` | RSS / HTTP API / Reddit OAuth |

Use `source: "all"` (or omit) to run every adapter.

**Comments:** Always included for X, Reddit (Playwright), and YouTube in this service.

---

## Section 2: Crawling Process Design

### Multi-thread (one worker per source)

- Orchestrator uses a shared **`ThreadPoolExecutor`** (`CRAWL_MAX_WORKERS`).
- Each selected adapter runs in its **own thread**.
- X and Reddit Playwright can run **in parallel** with each other and with HTTP/RSS sources.
- Playwright concurrency is capped with semaphores (`CRAWL_PW_X_SLOTS`, `CRAWL_PW_REDDIT_SLOTS`, default 1 each).
- No multi-threading inside a single Playwright browser session.

### Write-as-you-crawl (not Kafka)

- As soon as a post/comment is scraped, an `on_item` callback:
  1. **Upserts MongoDB** (`raw_posts` / `raw_comments`) immediately
  2. Optionally **publishes to AWS SQS** (if configured)
- So data is saved **during** the crawl, not only when the whole task finishes.
- Especially important for slow X / Reddit crawls with comments.

### High-level flow

```text
POST /crawl
   → Task accepted (task_id)
   → Thread pool: one thread per source
        → scrape item → write Mongo (+ optional SQS)
   → GET /crawl/{task_id} for progress / errors
```

### Alerts (email)

- On **session expired** / login wall (X or Reddit cookies):
  - Task records error `session_expired`
  - Optional **email** via SMTP if `ALERT_EMAIL_*` / `SMTP_*` env vars are set
- SMS is **not** implemented (email only for now).

---

## Section 3: API Design

FastAPI service (typical: `uvicorn app.main:app --port 8000 --workers 1`).

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/health` | Liveness |
| `GET` | `/sources` | Category → adapter map |
| `POST` | `/crawl` | Start crawl task |
| `GET` | `/crawl/{task_id}` | Task status / progress |

Optional header: `X-API-Key` (if `CRAWL_API_KEY` is set).

### Input schema (`POST /crawl`)

| Field | Required | Description |
|-------|----------|-------------|
| `query` | Yes | Keyword (same string for every adapter) |
| `source` | No | `x` \| `reddit` \| `youtube` \| `duckduckgo` \| `news` \| `all` (omit = all) |
| `time_delta` | No | `1`/`7`/`30`/`365` or `hour`/`day`/`week`/`month`/`year` |
| `limit` | No | Max posts per adapter (default 50, max 100) |

**Example:**

```http
POST /crawl
Content-Type: application/json

{
  "query": "agentic AI marketing",
  "source": "all",
  "time_delta": "day",
  "limit": 20
}
```

**Accepted response:**

```json
{ "task_id": "uuid", "status": "accepted" }
```

### Output schema (`GET /crawl/{task_id}`)

Crawl results are **not** returned as a big JSON dump of posts. Status returns progress; content is in MongoDB.

```json
{
  "task_id": "uuid",
  "status": "running|completed|failed|…",
  "query": "…",
  "sources": ["x_playwright", "reddit_playwright", "…"],
  "progress": { "posts_written": 12, "comments_written": 40 },
  "per_source": {
    "x_playwright": { "status": "running", "posts": 5, "comments": 10 }
  },
  "errors": [],
  "warnings": [],
  "completed_at": null,
  "params": { "limit": 20, "time_filter": "day" }
}
```

### Stored content shapes (Mongo)

**Post (simplified):** `query`, `post_id`, `source`, `url`, `title`, `text`, `author`, `published_ts`, `engagement`, `task_id`, …  
**Comment (simplified):** `query`, `comment_id`, `source`, `parent_content_id`, `parent_content_url`, `author`, `text`, `like_count`, `reply_count`, `parent_comment_id`, `published_ts`, `task_id`, …

Unique content key: `(source, external_id)`. Top-level `query` is the crawl keyword that produced the row.

### X / Reddit search modes (API path)

`POST /crawl` for `x_playwright` / `reddit_playwright` runs **multi-mode discovery**, then dedupes before comment enrichment:

| Adapter | Modes | Fresh-mode filter |
|---------|--------|-------------------|
| X | `top` + `live` | `live`: `min_replies >= 10` |
| Reddit | `top` + `new` + `hot` | `new`: `engagement.comments >= 10` |

`limit` = max unique posts after merge. Default comments per post (when unset): **200** for X/Reddit (YouTube remains 50).

---

## Section 4: Limitations

- Playwright (X / Reddit) is **slow**, especially with comments (can take a long time at high `limit`).
- Browser **session cookies** must be refreshed manually when expired.
- Result count may be **below `limit`** (scroll caps, filters, sparse search results).
- Site UI changes can break Playwright selectors.
- SQS is **optional / off** until AWS env vars are fully set.
- SMS alerts are not implemented (email only on session expiry).
- Run with **one uvicorn worker** (shared in-process thread pool / task state).

---

## Section 5: How to Integrate / Enable AWS SQS

SQS support already exists (`publishers/sqs.py`). It is **disabled** until all required env vars are set; otherwise a no-op publisher is used. SQS failures do not fail the crawl.

### Required env variables

```env
AWS_SQS_QUEUE_URL=https://sqs.<region>.amazonaws.com/<account>/<queue-name>
AWS_REGION=ap-southeast-2
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
```

### Behaviour when enabled

- After each post/comment is written to Mongo, the same event can be **`SendMessage`**’d to SQS.
- Downstream (Pace-Unit or another worker) can consume the queue for AI / further processing.

### Setup steps (short)

1. Create an SQS queue (optional DLQ + redrive policy).
2. Fill the four env vars above in `.env`.
3. Restart the crawl API.
4. Confirm messages appear while a crawl runs (`POST /crawl`).

No Kafka is required for Data-Crawler-Task.
