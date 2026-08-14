from __future__ import annotations

import logging
import json
from collections.abc import Iterable

from .models import XProfile

logger = logging.getLogger(__name__)


class ProfileClassifier:
    """Optional OpenAI classifier used only for person/company classification."""

    def __init__(self, api_key: str | None, model: str = "gpt-5-nano"):
        self.api_key = api_key
        self.model = model

    def enabled(self) -> bool:
        return bool(self.api_key)

    def classify_profiles(self, profiles: Iterable[XProfile]) -> dict[str, str]:
        """Return only `person_name` or `company_name`, in bounded batches."""
        if not self.api_key:
            return {}
        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.api_key)
            payloads = [
                {"handle": p.handle, "name": p.name, "bio": p.bio, "followers": p.followers.estimated}
                for p in profiles
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
                response = client.responses.create(model=self.model, input=prompt)
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

