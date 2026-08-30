"""Optional classifiers for article filtering and X account classification."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from .models import XProfile

logger = logging.getLogger(__name__)


class ArticleFilterError(RuntimeError):
    """The optional public-link filter could not produce a trustworthy answer."""


class ArticleLinkFilter:
    """Use a small LLM classification pass to reject unrelated public articles."""

    def __init__(self, api_key: str | None, model: str = "gpt-5-nano"):
        self.api_key = api_key
        self.model = model

    def enabled(self) -> bool:
        return bool(self.api_key)

    def filter_articles(
        self,
        field: str,
        keywords: list[str],
        articles: list[dict[str, str]],
    ) -> list[dict[str, str]]:
        if not articles or not self.api_key:
            return articles

        qualified_ids: set[str] = set()
        try:
            from openai import OpenAI

            client = OpenAI(api_key=self.api_key)
            for offset in range(0, len(articles), 50):
                batch = articles[offset:offset + 50]
                prompt = (
                    "Filter public-search article links for an X influencer discovery task. "
                    "The field is the primary and mandatory subject. Keywords only refine the field; they are "
                    "not independent subjects. Keep an article when its title and URL show that it is directly "
                    "about the field, or that a keyword is explicitly discussed in the context of the field. "
                    "Prefer field-specific influencer or expert lists. For example, when the field is Marketing "
                    "and a keyword is AI in Marketing, keep Marketing influencer lists and AI-in-Marketing lists, "
                    "but reject generic AI influencer lists. The page should reasonably identify relevant experts, "
                    "influencers, thought leaders, or X accounts. Reject broad keyword-only pages, sports, "
                    "entertainment, celebrities, unrelated news, unrelated directories, and ambiguous results. "
                    "Judge relevance from the title and URL; search_term only records which query found the page "
                    "and is not evidence that the page qualifies. "
                    "Use only the supplied IDs; do not create links or IDs.\n"
                    f"Field: {field}\n"
                    f"Keywords: {json.dumps(keywords, ensure_ascii=False)}\n"
                    f"Articles: {json.dumps(batch, ensure_ascii=False)}"
                )
                response = client.responses.create(
                    model=self.model,
                    input=prompt,
                    text={
                        "format": {
                            "type": "json_schema",
                            "name": "qualified_article_links",
                            "strict": True,
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "qualified_ids": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                    }
                                },
                                "required": ["qualified_ids"],
                                "additionalProperties": False,
                            },
                        }
                    },
                )
                parsed: dict[str, Any] = json.loads(getattr(response, "output_text", "") or "{}")
                valid_ids = {article["id"] for article in batch}
                qualified_ids.update(
                    str(article_id)
                    for article_id in parsed.get("qualified_ids", [])
                    if str(article_id) in valid_ids
                )
        except Exception as exc:
            raise ArticleFilterError("public article-link filtering failed") from exc

        return [article for article in articles if article["id"] in qualified_ids]


class ProfileClassifier:
    """Optional OpenAI classifier used only for person/company classification."""

    def __init__(self, api_key: str | None, model: str = "gpt-5-nano"):
        self.api_key = api_key
        self.model = model
        self._client = None

    def enabled(self) -> bool:
        return bool(self.api_key)

    def classify_profiles(self, profiles: Iterable[XProfile]) -> dict[str, str]:
        """Return only `person_name` or `company_name`, in bounded batches."""
        if not self.api_key:
            return {}
        try:
            from openai import OpenAI

            if self._client is None:
                self._client = OpenAI(api_key=self.api_key)
            payloads = [
                {
                    "handle": profile.handle,
                    "name": profile.name,
                    "bio": profile.bio,
                    "followers": profile.followers.estimated,
                }
                for profile in profiles
            ]
            labels: dict[str, str] = {}
            for offset in range(0, len(payloads), 25):
                payload = payloads[offset:offset + 25]
                prompt = (
                    "Classify each X profile below as exactly one label: person_name or company_name. "
                    "person_name means an individual human. company_name means a company, publication, "
                    "community, media brand, product, or organization. Return only a JSON object keyed by handle, "
                    "with each value exactly person_name or company_name.\n%s"
                ) % json.dumps(payload, ensure_ascii=False)
                response = self._client.responses.create(model=self.model, input=prompt)
                try:
                    batch = json.loads(getattr(response, "output_text", "") or "{}")
                except (TypeError, ValueError):
                    continue
                if not isinstance(batch, dict):
                    continue
                for handle, value in batch.items():
                    if value in {"person_name", "company_name"}:
                        labels[str(handle).lower()] = value
            return labels
        except Exception as exc:
            # Cost/stability: never fail the pipeline because optional classification failed.
            logger.warning("Profile classification failed: %s", exc)
            return {}


class LocalProfileClassifier:
    """Use the bundled offline name classifier for person/company filtering."""

    def __init__(self, model_path: Path, device: str = "auto"):
        self.model_path = model_path
        self.device = device
        self._tokenizer = None
        self._model = None
        self._torch = None
        self._resolved_device: str | None = None

    def classify_profiles(self, profiles: Iterable[XProfile]) -> dict[str, str]:
        profiles = [profile for profile in profiles if profile.name or profile.handle]
        if not profiles:
            return {}
        try:
            self._load()
            assert self._tokenizer is not None and self._model is not None and self._torch is not None
            names = [profile.name or profile.handle for profile in profiles]
            encoded = self._tokenizer(names, return_tensors="pt", padding=True, truncation=True)
            inputs = {key: value.to(self._resolved_device) for key, value in encoded.items()}
            with self._torch.no_grad():
                label_ids = self._model(**inputs).logits.argmax(dim=-1).tolist()
            labels: dict[str, str] = {}
            for profile, label_id in zip(profiles, label_ids):
                label = self._model.config.id2label.get(int(label_id), "")
                mapped = self._pipeline_label(label)
                if mapped:
                    labels[profile.handle.lower()] = mapped
            return labels
        except Exception as exc:
            logger.warning("Local profile classification failed: %s", exc)
            return {}

    @staticmethod
    def _pipeline_label(model_label: str) -> str:
        """Translate every model result into one of the pipeline's two labels."""
        if model_label.lower() == "residential":
            return "person_name"
        if model_label.lower() in {"non_residential", "rental"}:
            return "company_name"
        return "person_name"

    def _load(self) -> None:
        if self._model is not None:
            return
        if not self.model_path.is_dir():
            raise FileNotFoundError(f"Local name-classifier model not found: {self.model_path}")
        import torch
        from transformers import AutoModelForSequenceClassification, AutoTokenizer

        if self.device == "auto":
            device = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"
        else:
            device = self.device
        self._tokenizer = AutoTokenizer.from_pretrained(self.model_path, local_files_only=True)
        self._model = AutoModelForSequenceClassification.from_pretrained(self.model_path, local_files_only=True)
        self._model.to(device)
        self._model.eval()
        self._torch = torch
        self._resolved_device = device
