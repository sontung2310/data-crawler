from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


@dataclass(frozen=True)
class Settings:
    mongodb_url: str | None = None
    openai_api_key: str | None = None
    openai_model: str = "gpt-5-nano"
    classification_backend: str = "local"
    local_classifier_model: Path = Path("models/name-classifier")
    local_classifier_device: str = "auto"
    x_session: str | None = None
    embedding_enabled: bool = True
    embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    headless: bool = True
    data_dir: Path = Path("data/influencers")
    public_web_num_queries: int = 3


def load_settings(env_file: str | None = None) -> Settings:
    if load_dotenv:
        load_dotenv(env_file or ".env")
    data_dir = Path(os.getenv("DATA_DIR", "data/influencers"))
    return Settings(
        mongodb_url=os.getenv("MongoDB_URL") or os.getenv("MONGODB_URL") or None,
        openai_api_key=os.getenv("OPENAI_API") or os.getenv("OPENAI_API_KEY") or None,
        openai_model=os.getenv("OPENAI_MODEL", "gpt-5-nano"),
        classification_backend=os.getenv("CLASSIFICATION_BACKEND", "local").lower(),
        local_classifier_model=Path(os.getenv("LOCAL_CLASSIFIER_MODEL", "models/name-classifier")),
        local_classifier_device=os.getenv("LOCAL_CLASSIFIER_DEVICE", "auto"),
        x_session=os.getenv("X_browser_session") or os.getenv("X_BROWSER_SESSION") or os.getenv("X_session") or os.getenv("X_SESSION") or None,
        embedding_enabled=os.getenv("EMBEDDING_ENABLED", "true").lower() != "false",
        embedding_model=os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2"),
        headless=os.getenv("HEADLESS", "true").lower() != "false",
        data_dir=data_dir,
        public_web_num_queries=3,
    )

