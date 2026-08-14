from __future__ import annotations

import json
import logging
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def save_json(output: dict[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    return path


def maybe_save_mongo(output: dict[str, Any], mongodb_url: str | None, collection_name: str = "x_influencer_runs") -> None:
    if not mongodb_url:
        return
    client = None
    try:
        from pymongo import MongoClient
        client = MongoClient(mongodb_url, serverSelectionTimeoutMS=5000)
        db = client.get_default_database() if "/" in mongodb_url.rsplit("/", 1)[-1] else client["influencer_discovery"]
        db[collection_name].insert_one(deepcopy(output))
    except Exception as exc:
        # Local JSON is source of truth; Mongo is optional persistence.
        logger.warning("MongoDB persistence failed; local JSON was saved: %s", exc)
        return
    finally:
        if client is not None:
            client.close()

