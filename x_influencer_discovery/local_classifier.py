from __future__ import annotations

import logging
from collections.abc import Iterable
from pathlib import Path

from .models import XProfile

logger = logging.getLogger(__name__)


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
    def _pipeline_label(model_label: str) -> str | None:
        if model_label.lower() == "residential":
            return "person_name"
        if model_label.lower() in {"non_residential", "rental"}:
            return "company_name"
        return None

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

