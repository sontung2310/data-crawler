"""FastAPI application entrypoint."""
from __future__ import annotations

import logging
import sys
from pathlib import Path

# Ensure project root is on sys.path when started via uvicorn
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fastapi import FastAPI

from app.api.crawl import router as crawl_router
from persist import ensure_raw_indexes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

app = FastAPI(
    title="Data Crawler Task",
    description="Local isolated crawl API — writes raw_posts / raw_comments to MongoDB.",
    version="0.1.0",
)
app.include_router(crawl_router)


@app.on_event("startup")
def _startup() -> None:
    try:
        ensure_raw_indexes()
    except Exception as exc:
        logging.getLogger(__name__).warning("Mongo index init deferred: %s", exc)
