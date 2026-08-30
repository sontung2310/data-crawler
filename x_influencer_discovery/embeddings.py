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
        self._model = None

    def profile_similarities(self, query: str, profiles: Iterable[XProfile]) -> dict[str, float]:
        profiles = list(profiles)
        if not self.enabled or not profiles:
            return {}
        return self.document_similarities(
            query,
            {profile.handle.lower(): self._profile_text(profile) for profile in profiles},
        )

    def document_similarities(
        self,
        query: str,
        documents: dict[str, str],
        *,
        batch_size: int = 32,
    ) -> dict[str, float]:
        if not self.enabled or not documents:
            return {}
        try:
            return self.strict_document_similarities(query, documents, batch_size=batch_size)
        except Exception as exc:
            logger.warning("Local embedding relevance is unavailable; using lexical relevance only: %s", exc)
            return {}

    def strict_document_similarities(
        self,
        query: str,
        documents: dict[str, str],
        *,
        batch_size: int = 32,
    ) -> dict[str, float]:
        """Calculate similarities, surfacing technical model failures to queue work."""
        if not self.enabled or not documents:
            return {}
        model = self._load_model()
        query_embedding = model.encode(query, normalize_embeddings=True)
        keys = list(documents)
        embeddings = model.encode(
            [documents[key] for key in keys],
            normalize_embeddings=True,
            batch_size=batch_size,
        )
        return {key: float(query_embedding @ embedding) for key, embedding in zip(keys, embeddings)}

    def _load_model(self):
        if self._model is not None:
            return self._model
        from sentence_transformers import SentenceTransformer

        try:
            self._model = SentenceTransformer(self.model_name, local_files_only=True)
        except OSError:
            self._model = SentenceTransformer(self.model_name)
        return self._model

    @staticmethod
    def _profile_text(profile: XProfile) -> str:
        # Keep semantic matching consistent with lexical relevance: a display
        # name/page title is not grounded topical evidence.
        return profile.bio or ""
