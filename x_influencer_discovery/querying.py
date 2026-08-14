from __future__ import annotations

import re


STOPWORDS = {
    "and", "for", "from", "into", "of", "on", "or", "the", "to", "with",
}

# Small, bidirectional groups for common abbreviations. These are supporting
# terms; the local embedding model handles broader semantic relationships.
TOPIC_ALIASES = {
    "artificial intelligence": ("ai", "machine learning", "ml"),
    "marketing technology": ("martech",),
    "social media marketing": ("social media",),
}


def query_expansions(query: str) -> list[str]:
    """Return safe, deterministic variants of the supplied query."""
    q = query.strip()
    variants = [q]
    if "&" in q:
        # Do not turn abbreviations such as F&B into the low-signal phrase
        # "f b". The local embedding matcher handles semantic similarity.
        sides = [part.strip() for part in q.split("&")]
        if any(len(part) > 1 for part in sides):
            variants.append(q.replace("&", "and"))
            variants.append(q.replace("&", " "))
    if "/" in q:
        variants.append(q.replace("/", " "))
    out = []
    for v in variants:
        v = re.sub(r"\s+", " ", v).strip()
        if v and v.lower() not in {x.lower() for x in out}:
            out.append(v)
    return _add_topic_aliases(out)


def _add_topic_aliases(variants: list[str]) -> list[str]:
    expanded = list(variants)
    for variant in variants:
        lowered = variant.lower()
        for canonical, aliases in TOPIC_ALIASES.items():
            group = (canonical, *aliases)
            matched = next((term for term in group if term_matches_text(term, lowered)), None)
            if not matched:
                continue
            for replacement in group:
                candidate = re.sub(
                    rf"(?<![a-z0-9]){re.escape(matched)}(?![a-z0-9])",
                    replacement,
                    variant,
                    flags=re.I,
                )
                if candidate.lower() not in {item.lower() for item in expanded}:
                    expanded.append(candidate)
    return expanded


def query_terms(query: str) -> set[str]:
    terms: set[str] = set()
    for variant in query_expansions(query):
        low = variant.lower()
        terms.add(low)
        for part in re.split(r"[^a-z0-9]+", low):
            if len(part) >= 3 and part not in STOPWORDS:
                terms.add(part)
    return terms


def text_matches_query(text: str, query: str) -> bool:
    low = text.lower()
    terms = query_terms(query)
    return any(term_matches_text(term, low) for term in terms)


def term_matches_text(term: str, text: str) -> bool:
    """Match whole terms/phrases, so short queries such as AI do not match paid."""
    pattern = rf"(?<![a-z0-9]){re.escape(term.lower())}(?![a-z0-9])"
    return re.search(pattern, text.lower()) is not None

