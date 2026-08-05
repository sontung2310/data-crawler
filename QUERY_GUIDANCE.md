# Crawl Query Guidance

How to write queries for this crawler when you want **social / web insights for business decisions**.

The crawler is a **search system**, not a chat assistant. It normalizes your text into phrases and must-match tokens, searches each source, then soft-filters results. Good queries look like what you’d type into Google, X, or Reddit — not like a question to ChatGPT.

---

## How your query is interpreted

For any free-text query the pipeline builds a domain-agnostic **intent**:

| Piece | Role | Example for `Retail media networks Australia` |
|-------|------|-----------------------------------------------|
| **Phrases** | Consecutive topic words (usually bigrams), quoted in search | `"retail media"` |
| **Standalone must tokens** | Other required topic words | `networks`, `australia` |
| **Optional / weak words** | Soft intent only — **not** required in results | `trending`, `latest`, `popular`, `now` |

Stopwords (`in`, `the`, `is`, …) and weak modifiers are dropped from hard matching.

**Implications**

- `AI marketing trends in Australia` → search roughly `ai marketing australia` (`trends` is optional)
- `AI marketing trends` → `ai marketing` (`trends` is optional)
- `ChatGPT enterprise adoption` → `"chatgpt enterprise" adoption`
- Chatty questions are cleaned, but leftover filler can still hurt — prefer concise forms

---

## Queries you *should* use

Use short, concrete **topic + optional constraint** strings.

### 1. Category / trend sensing (high volume)

Good when you want “what’s moving in this space?”

| Prefer | Avoid |
|--------|--------|
| `AI marketing trends` | `What are the latest AI marketing trends right now?` |
| `agentic AI advertising` | `Tell me insights about agentic AI in ads` |
| `retail media networks` | `Why is everyone talking about retail media networks?` |

### 2. Product / brand / competitor monitoring

| Prefer | Avoid |
|--------|--------|
| `ChatGPT enterprise adoption` | `How are companies adopting ChatGPT in the enterprise?` |
| `"Notion AI" competitors` | `Who are the best Notion AI competitors and why?` |
| `Shopify checkout conversion` | `Is Shopify checkout bad for conversion lately?` |

### 3. Market / geo focus

Keep **topic phrase + place** explicit when location matters.

| Prefer | Avoid |
|--------|--------|
| `"performance marketing" Australia` | `Is performance marketing trending in Australia right now?` |
| `B2B SaaS marketing UK` | `How are UK B2B SaaS companies doing marketing these days?` |
| `demand gen Australia` | `What’s the vibe around demand gen in Australia?` |

### 4. Issue / risk / reputation

| Prefer | Avoid |
|--------|--------|
| `AI advertising regulation Australia` | `Did AI ads get into trouble in Australia?` |
| `"data breach" fintech Australia` | `Any scary fintech data breaches I should know about?` |

### Rule of thumb

> If you wouldn’t paste it into Google Search or X search and expect useful hits, don’t use it as a crawl query.

**Target shape:** 2–6 content words, optional quoted multi-word product or category names, optional place or brand.

---

## Queries you should *not* use

| Anti-pattern | Why it fails | Rewrite to |
|--------------|--------------|------------|
| Full questions | Engines OR/AND keywords loosely; judge expects full intent | Keywords / phrases |
| Stacked soft constraints | `trending` + niche + geo → sparse or empty on news wires | Drop `trending`; keep topic + geo |
| Single vague token | `trends`, `AI`, `marketing` | Add a second concrete term |
| Metaphor / opinion prompts | `why is everyone obsessed with…` | Named topic + channel/geo |
| Ultra-narrow time + niche | `Hobart demand gen this week` | Broader place or drop time word |
| Long chatty sentences | Extra words become false must-tokens | 3–5 tokens max |

### Especially fragile: “X trending in Y”

This pattern asks for **three** things at once: topic, place, and trend evidence.

- Retrieval often returns partial matches (topic only, or place only).
- The relevance judge then marks them irrelevant.
- Prefer `"topic" Place` or `topic Place` and treat “trending” as analysis you do on the results (volume, engagement, recency), not as a search keyword.

---

## Recommended query recipes for business insight

Imagine you run social listening to support decisions. Useful query families:

1. **Market trend** — `AI marketing`, `short form video commerce`, `retail media networks`
2. **Category × geo** — `"performance marketing" Australia`, `B2B SaaS marketing UK`
3. **Brand health** — `HubSpot marketing`, `"Salesforce" advertising`
4. **Product adoption** — `ChatGPT enterprise`, `Microsoft Copilot workplace`
5. **Competitor move** — `"OpenAI" enterprise pricing`, `Claude vs ChatGPT marketing`
6. **Channel behavior** — `TikTok shop Australia`, `UGC ads Meta`
7. **Risk / regulation** — `AI advertising regulation Australia`, `influencer disclosure FTC`

Run **several focused queries** rather than one mega-question.

---

## What to expect by source

Not every source has content for every query. Empty results often mean **corpus gap**, not a broken query.

| Source type | Best for | Often empty for |
|-------------|----------|-----------------|
| Google News / DuckDuckGo / NewsAPI | Industry & market news | Very narrow local niches |
| YouTube | Demos, explainers, practitioner content | Niche B2B unless high volume |
| Reddit (RSS / Playwright) | Practitioner debate, complaints, sentiment | Topics with little community discussion |
| X (Playwright) | Real-time industry chatter | Needs valid session + browser |
| BBC / TechCrunch / Guardian / HN | Broader tech / business agendas | Hyper-local or highly niche campaigns |
| Guardian / HN | Tech, society, business | Purely local consumer chatter |

**Example from internal evals (limit 5, non-Playwright sources):**

| Query | Overall relevant % | Notes |
|-------|--------------------|--------|
| `AI marketing trends` | ~97% | High-volume B2B topic; most sources filled |
| `ChatGPT enterprise adoption` | ~75% | Good on YouTube / DDG / NewsAPI; sparse on some news |
| `performance marketing Australia` | Lower / more variable | Stronger when the topic is widely covered; expect emptier wire sources for narrow geo niches |

Playwright sources (`x_playwright`, `reddit_playwright`) need a working Chromium install and valid session cookies; they were not scored in the automated agent environment when browsers were missing.

---

## Practical checklist before you crawl

1. Write the query as **keywords**, not a sentence.
2. Quote multi-word categories or products when useful: `"performance marketing"`, `"retail media"`.
3. Add **one** geo or brand constraint if needed — not three.
4. Drop filler: `trending`, `latest`, `insights`, `right now` (the pipeline already treats many as optional).
5. If results are empty, **relax** (remove geo, broaden the category) instead of lengthening the question.
6. For decisions, run **2–4 related queries** and compare sources — don’t rely on a single chatty prompt.

---

## Quick examples

**Good**

```text
AI marketing trends
"performance marketing" Australia
ChatGPT enterprise adoption
retail media networks
UGC ads Meta
Shopify competitor pricing
```

**Bad → better**

```text
Is performance marketing trending in Australia right now?
  →  "performance marketing" Australia

What are people saying about ChatGPT at work?
  →  ChatGPT workplace  OR  ChatGPT enterprise adoption

Give me insights on AI in marketing for my strategy deck
  →  AI marketing trends
```

---

## Related

- Relevance eval: `python -m tests.eval_relevent --query "AI marketing trends" --limit 10`
- Shared intent helpers: `sources/utils.py` (`parse_query_intent`, `keyword_search_query`, `web_search_query`, `text_matches_intent`)
