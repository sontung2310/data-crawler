#!/usr/bin/env python3
"""
Crawl from registry sources and judge relevance of each post vs the query (LLM-as-judge).

Does NOT write to Mongo. Requires OPENAI_API_KEY in .env.

Run from the project root:
    python -m tests.eval_relevent --query "AI trending in Marketing"
    python -m tests.eval_relevent --query "AI" --limit 10 --source x_playwright --source reddit_playwright
    python -m tests.eval_relevent --query "AI" --json out.json

Notes:
    - reddit_playwright uses sort=relevance by default (same as production search).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import config
from sources import REGISTRY

JUDGE_SYSTEM = """You are a relevance judge for a web crawl.
Given a search QUERY and one CRAWLED ITEM (title/text/url), score how well the item matches the query intent.

Reply with ONLY valid JSON (no markdown):
{"score": <int 1-5>, "relevant": <true|false>, "reason": "<one short sentence>"}

Scoring:
1 = unrelated
2 = weakly related
3 = somewhat related
4 = clearly related
5 = highly relevant match
relevant = true when score >= 3.
"""


def _extract_posts(payload: Any) -> List[dict]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return [p for p in payload if isinstance(p, dict)]
    if isinstance(payload, dict):
        rows = payload.get("posts") or payload.get("videos") or []
        return [p for p in rows if isinstance(p, dict)]
    return []


def _item_text(row: dict) -> str:
    title = (row.get("title") or "").strip()
    text = (row.get("text") or "").strip()
    url = (row.get("url") or "").strip()
    author = (row.get("author") or "").strip()
    parts = []
    if title:
        parts.append(f"title: {title}")
    if text:
        parts.append(f"text: {text[:800]}")
    if author:
        parts.append(f"author: {author}")
    if url:
        parts.append(f"url: {url}")
    return "\n".join(parts) or "(empty item)"


def _judge_item(client: Any, model: str, query: str, row: dict) -> Dict[str, Any]:
    user = f"QUERY:\n{query}\n\nCRAWLED ITEM:\n{_item_text(row)}"
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": JUDGE_SYSTEM},
            {"role": "user", "content": user},
        ],
        response_format={"type": "json_object"},
    )
    raw = (resp.choices[0].message.content or "").strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {"score": 0, "relevant": False, "reason": f"bad judge JSON: {raw[:120]}"}

    score = int(data.get("score") or 0)
    score = max(1, min(5, score)) if score else 0
    relevant = bool(data.get("relevant")) if "relevant" in data else score >= 3
    reason = str(data.get("reason") or "").strip() or "(no reason)"
    return {"score": score, "relevant": relevant, "reason": reason}


def _resolve_sources(names: Optional[List[str]]) -> List[str]:
    if not names:
        return list(REGISTRY.keys())
    out: List[str] = []
    for name in names:
        key = name.strip()
        if key not in REGISTRY:
            # allow category aliases like "x" / "reddit"
            matched = [
                k for k, meta in REGISTRY.items() if meta.get("category") == key or k == key
            ]
            if not matched:
                raise SystemExit(f"Unknown source {name!r}. Known: {', '.join(REGISTRY)}")
            out.extend(matched)
        else:
            out.append(key)
    # preserve order, unique
    seen = set()
    uniq = []
    for s in out:
        if s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq


def _crawl_source(name: str, query: str, limit: int) -> List[dict]:
    fetch = REGISTRY[name]["fetch"]
    # Reddit keyword search defaults to sort=relevance (explicit for clarity).
    kwargs: Dict[str, Any] = {"limit": limit}
    if name == "reddit_playwright":
        from sources import reddit_playwright

        kwargs["sort"] = reddit_playwright.DEFAULT_SEARCH_SORT
    try:
        payload = fetch(query, **kwargs)
    except TypeError:
        try:
            payload = fetch(query, limit=limit)
        except TypeError:
            payload = fetch(query, limit)
    return _extract_posts(payload)


def run_eval(
    query: str,
    *,
    limit: int = 10,
    sources: Optional[List[str]] = None,
) -> Dict[str, Any]:
    if not config.OPENAI_API_KEY:
        raise SystemExit("OPENAI_API_KEY not set in .env")

    from openai import OpenAI

    client = OpenAI(api_key=config.OPENAI_API_KEY)
    model = config.EVAL_MODEL or "gpt-5.6-luna"
    source_names = _resolve_sources(sources)

    report: Dict[str, Any] = {
        "query": query,
        "limit": limit,
        "model": model,
        "sources": {},
        "totals": {"items": 0, "relevant": 0, "mean_score": None},
    }
    all_scores: List[int] = []

    for name in source_names:
        print(f"\n=== {name} ===")
        t0 = time.time()
        try:
            posts = _crawl_source(name, query, limit)
        except Exception as exc:
            print(f"  CRAWL FAIL: {exc}")
            report["sources"][name] = {
                "status": "crawl_failed",
                "error": str(exc),
                "items": [],
                "pct_relevant": None,
                "mean_score": None,
            }
            continue

        duration_ms = int((time.time() - t0) * 1000)
        print(f"  crawled {len(posts)} posts in {duration_ms}ms — judging…")

        items: List[dict] = []
        for i, row in enumerate(posts, start=1):
            try:
                judgment = _judge_item(client, model, query, row)
            except Exception as exc:
                judgment = {"score": 0, "relevant": False, "reason": f"judge error: {exc}"}

            entry = {
                "title": (row.get("title") or "")[:120],
                "url": row.get("url"),
                "source": row.get("source") or name,
                **judgment,
            }
            items.append(entry)
            all_scores.append(judgment["score"])
            flag = "Y" if judgment["relevant"] else "N"
            print(
                f"  [{i}] score={judgment['score']} relevant={flag} "
                f"| {entry['title'][:60]!r} — {judgment['reason']}"
            )

        n = len(items)
        n_rel = sum(1 for it in items if it.get("relevant"))
        scores = [it["score"] for it in items if it.get("score")]
        mean = round(sum(scores) / len(scores), 2) if scores else None
        pct = round(100.0 * n_rel / n, 1) if n else None
        report["sources"][name] = {
            "status": "ok",
            "duration_ms": duration_ms,
            "count": n,
            "relevant_count": n_rel,
            "pct_relevant": pct,
            "mean_score": mean,
            "items": items,
        }
        print(f"  summary: {n_rel}/{n} relevant ({pct}%), mean_score={mean}")

    total_items = sum(
        s.get("count") or 0 for s in report["sources"].values() if s.get("status") == "ok"
    )
    total_rel = sum(
        s.get("relevant_count") or 0
        for s in report["sources"].values()
        if s.get("status") == "ok"
    )
    report["totals"] = {
        "items": total_items,
        "relevant": total_rel,
        "pct_relevant": round(100.0 * total_rel / total_items, 1) if total_items else None,
        "mean_score": round(sum(all_scores) / len(all_scores), 2) if all_scores else None,
    }
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="LLM relevance eval for crawl adapters")
    parser.add_argument("--query", required=True, help="Search query / keyword")
    parser.add_argument("--limit", type=int, default=10, help="Posts per source (default 10)")
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        help="Registry name or category (repeatable). Default: all sources",
    )
    parser.add_argument(
        "--json",
        dest="json_path",
        default=None,
        help="Write full JSON report to this path (also printed as summary)",
    )
    args = parser.parse_args()

    report = run_eval(args.query, limit=max(1, args.limit), sources=args.sources)

    print("\n======== TOTALS ========")
    t = report["totals"]
    print(
        f"items={t['items']} relevant={t['relevant']} "
        f"pct={t['pct_relevant']}% mean_score={t['mean_score']}"
    )

    out_path = args.json_path
    if out_path:
        path = Path(out_path)
        path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"Wrote JSON report → {path}")
    else:
        # compact JSON to stdout when no path given is noisy; skip unless --json -
        pass


if __name__ == "__main__":
    main()
