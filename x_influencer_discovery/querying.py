from __future__ import annotations

import re
import unicodedata


STOPWORDS = {
    "and", "for", "from", "in", "into", "of", "on", "or", "the", "to", "with",
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


def normalize_lexical_text(value: str) -> str:
    """Create a stable token representation for the new lexical scorer.

    Hashtags and camel-case hashtag words become ordinary words; punctuation,
    hyphens, and slashes become separators. This deliberately does not infer
    synonyms or extract terms.
    """
    value = unicodedata.normalize("NFKD", value or "")
    value = "".join(char for char in value if not unicodedata.combining(char))
    value = re.sub(r"#([A-Za-z0-9]+)", r"\1", value)
    value = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", value)
    value = re.sub(r"[^A-Za-z0-9]+", " ", value).lower()
    return " ".join(value.split())


def lexical_match_details(term: str, text: str) -> tuple[float, set[str]]:
    """Return match strength and the normalized term words that matched.

    Comparisons use normalized whole tokens and accept basic English plural
    forms (``farmer``/``farmers`` and ``policy``/``policies``). A one-word term
    remains a full 1.0 match when it is present.
    """
    term_tokens = normalize_lexical_text(term).split()
    text_tokens = normalize_lexical_text(text).split()
    if not term_tokens or not text_tokens:
        return 0.0, set()

    def same_word(left: str, right: str) -> bool:
        if left == right:
            return True
        if left.endswith("y") and right == f"{left[:-1]}ies":
            return True
        if right.endswith("y") and left == f"{right[:-1]}ies":
            return True
        return right in {f"{left}s", f"{left}es"} or left in {f"{right}s", f"{right}es"}

    phrase_size = len(term_tokens)
    for index in range(len(text_tokens) - phrase_size + 1):
        if all(same_word(term_token, text_token) for term_token, text_token in zip(term_tokens, text_tokens[index:index + phrase_size])):
            return 1.0, set(term_tokens)

    if phrase_size == 1:
        return 0.0, set()
    matched_tokens = {
        term_token
        for term_token in term_tokens
        for text_token in text_tokens
        if same_word(term_token, text_token)
    }
    return (0.5, matched_tokens) if matched_tokens else (0.0, set())


def lexical_match_strength(term: str, text: str) -> float:
    """Return 1.0 for a full phrase, 0.5 for one word of a phrase."""
    return lexical_match_details(term, text)[0]
