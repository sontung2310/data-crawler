"""Score saved discovery output using the proposed independent relevance rules.

This is a calibration utility, not the queue consumer. It deliberately reads
only the saved candidate state and writes a JSON report to stdout.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from x_influencer_discovery.embeddings import LocalEmbeddingMatcher
from x_influencer_discovery.models import RecentActivity
from x_influencer_discovery.relevance import lexical_overlap
from x_influencer_discovery.scoring import follower_score
from x_influencer_discovery.x_profile_posts import build_engagement_metrics, engagement_score, recent_activity_score


COMPANIES = {
    "marketing-eye": {
        "summary": "B2B outsourced marketing agency for SMBs, SAP partners, associations, and nonprofits. Services: digital marketing, SEO, PPC, content marketing, web development, lead generation, marketing consulting/audits. Focus: brand awareness, lead generation, PR, social media. Challenges: lead conversion, search ranking, resource constraints, marketing consistency.",
        "terms": ["Outsourced marketing", "B2B marketing", "Small business marketing", "Lead generation", "Digital marketing", "SEO", "Marketing consulting", "SAP ecosystem", "Content marketing", "Brand awareness"],
    },
    "robotic-marketer": {
        "summary": "AI-powered autonomous marketing strategy platform. Automates strategy, content generation, campaign execution, and analytics for agencies, SMBs, enterprises, and channel partners. Focus: AI marketing automation, marketing strategy, campaign optimization, ROI, thought leadership. Challenges: funding, staffing, market expansion, channel diversification.",
        "terms": ["AI marketing", "Marketing automation", "Marketing strategy", "MarTech", "AI marketing tools", "Digital transformation", "Marketing agency", "Channel partnerships", "ROI marketing", "Marketing technology"],
    },
    "agforce": {
        "summary": "Peak advocacy organisation representing Queensland's rural producers of cattle, grain, cane, sheep, wool and goats. A not-for-profit farmer-led body advancing sustainable agribusiness, it serves as the leading political and industry voice for broadacre agriculture, protecting landholder rights, biosecurity, water access and farming interests. It engages in advocacy, government lobbying, policy submissions, legal challenges, and member representation across regional Queensland, while supporting rural communities through leadership programs, industry events, podcasts and public campaigns championing farmers and food security.",
        "terms": ["Agriculture advocacy", "Queensland farmers", "Cattle producers", "Farming policy", "Rural advocacy", "Agribusiness", "Biosecurity", "Landholder rights", "Food security", "Grazier"],
    },
    "axecom": {
        "summary": "Australian telecommunications provider delivering business and home fibre internet, NBN, dark fibre, enterprise ethernet, and unified communications solutions including 3CX phone systems and Microsoft Teams integration. It serves businesses and residential customers with fast, reliable connectivity, backup services, managed networks, and enterprise-grade service level agreements. The company positions itself as a one-stop shop for internet, phone, and cloud communications, emphasizing fast installation, competitive pricing, and dependable customer support.",
        "terms": ["Fibre internet", "Business internet", "NBN", "ISP", "Telecommunications", "Unified communications", "VoIP", "Managed IT services", "Business connectivity", "Network reliability"],
    },
}

INPUTS = [
    ("marketing-eye", "data/influencers/marketing-eye.json"),
    ("robotic-marketer", "data/influencers/robotic-marketer.json"),
    ("agforce", "data/influencers/agforce.json"),
    ("axecom", "data/influencers/telecommunications_x_influencers.json"),
    ("axecom", "data/influencers/axecom.json"),
]


def normalized_semantic(raw_similarity: float | None) -> float:
    if raw_similarity is None:
        return 0.0
    return min(1.0, max(0.0, (raw_similarity - 0.20) / 0.60))


def activity_from_record(record: dict[str, Any]) -> RecentActivity:
    raw = record.get("recent_activity") or {}
    return RecentActivity(
        status=str(raw.get("status") or "unknown"),
        latest_visible_post_at=raw.get("latest_visible_post_at"),
        evidence=raw.get("evidence"),
    )


def frequency_proxy(record: dict[str, Any]) -> int:
    """Historical JSON lacks current-run appearance counts.

    This temporary proxy mirrors the planned 0-10 frequently-appeared component
    using saved public-source count only; a real queue run computes it directly.
    """
    return min(10, len(record.get("discovery_sources") or []) * 3)


def score_records(company_key: str, path: Path) -> list[dict[str, Any]]:
    company = COMPANIES[company_key]
    payload = json.loads(path.read_text())
    records = payload["results"]
    documents = {
        record["handle"].lower(): "\n".join(
            [str(record.get("bio") or ""), *[str(post.get("text") or "") for post in (record.get("recent_posts") or [])[:5]]]
        )
        for record in records
    }
    semantic_raw = LocalEmbeddingMatcher(True, "sentence-transformers/all-MiniLM-L6-v2").document_similarities(
        company["summary"], documents, batch_size=32
    )
    scored = []
    for record in records:
        posts = (record.get("recent_posts") or [])[:5]
        lexical = lexical_overlap(str(record.get("bio") or ""), posts, company["terms"])
        raw = semantic_raw.get(record["handle"].lower())
        semantic = normalized_semantic(raw)
        hybrid = 0.60 * lexical["lexical_overlap"] + 0.40 * semantic
        followers = (record.get("followers") or {}).get("estimated")
        activity = activity_from_record(record)
        metrics = build_engagement_metrics(posts, follower_estimate=followers)
        # Provisional 100-point allocation proposed for threshold calibration.
        components = {
            "topic_relevance": round(hybrid * 50),
            "frequently_appeared_proxy": frequency_proxy(record),
            "followers": follower_score(followers, max_score=15),
            "recent_activity": recent_activity_score(activity, max_score=15),
            "engagement": engagement_score(metrics, max_score=10),
        }
        scored.append({
            "source_file": str(path),
            "company": company_key,
            "name": record.get("name"),
            "handle": record["handle"],
            "profile_url": record.get("profile_url"),
            "bio": record.get("bio"),
            "recent_posts": posts,
            "followers": followers,
            "lexical": lexical,
            "semantic_raw": round(raw, 4) if raw is not None else None,
            "semantic_normalized": round(semantic, 4),
            "hybrid_relevance": round(hybrid, 4),
            "components": components,
            "provisional_final_score": sum(components.values()),
        })
    return sorted(scored, key=lambda item: (-item["provisional_final_score"], item["handle"].lower()))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    results = []
    for company_key, filename in INPUTS:
        results.extend(score_records(company_key, Path(filename)))
    print(json.dumps({"scoring_allocation": {"topic_relevance": 50, "frequently_appeared_proxy": 10, "followers": 15, "recent_activity": 15, "engagement": 10}, "results": results}, indent=2 if args.pretty else None, ensure_ascii=False))


if __name__ == "__main__":
    main()
