from __future__ import annotations

import logging
from collections.abc import Iterable

from .models import XProfile

logger = logging.getLogger(__name__)


class LocalEmbeddingMatcher:
    """Optional local semantic matcher backed by all-MiniLM-L6-v2."""

    def __init__(self, enabled: bool, model_name: str):
        self.enabled = enabled
        self.model_name = model_name

    def profile_similarities(self, query: str, profiles: Iterable[XProfile]) -> dict[str, float]:
        profiles = list(profiles)
        if not self.enabled or not profiles:
            return {}
        try:
            from sentence_transformers import SentenceTransformer

            try:
                # Normal runs stay local after the first model download.
                model = SentenceTransformer(self.model_name, local_files_only=True)
            except OSError:
                model = SentenceTransformer(self.model_name)
            query_embedding = model.encode(query, normalize_embeddings=True)
            profile_embeddings = model.encode(
                [self._profile_text(profile) for profile in profiles],
                normalize_embeddings=True,
            )
            return {
                profile.handle.lower(): float(query_embedding @ embedding)
                for profile, embedding in zip(profiles, profile_embeddings)
            }
        except Exception as exc:
            logger.warning("Local embedding relevance is unavailable; using keyword matching only: %s", exc)
            return {}

    @staticmethod
    def _profile_text(profile: XProfile) -> str:
        # Keep semantic matching consistent with lexical relevance: a display
        # name/page title is not grounded topical evidence.
        return profile.bio or ""

