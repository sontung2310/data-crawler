"""Durable progress state for the one-hop X Following snowball run."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .models import AccountRef
from .platforms import account_ref, validate_platform


class SnowballCheckpointError(ValueError):
    """The snowball progress file is missing, invalid, or for another run."""


def progress_path_for_output(output_path: Path) -> Path:
    """Return the stable sidecar path used by fresh and resumed snowball runs."""

    output_path = Path(output_path)
    return output_path.with_name(f"{output_path.stem}.snowball-progress.json")


def _account_from_dict(value: Any) -> AccountRef:
    if not isinstance(value, dict):
        raise SnowballCheckpointError("checkpoint account must be an object")
    try:
        return account_ref(
            "x",
            account_id=str(value["account_id"]) if value.get("account_id") is not None else None,
            handle=str(value["handle"]) if value.get("handle") is not None else None,
            profile_url=str(value["profile_url"]) if value.get("profile_url") is not None else None,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise SnowballCheckpointError(f"invalid checkpoint account: {exc}") from exc


@dataclass
class SnowballCheckpoint:
    """Atomic, local progress state for one ordered snowball seed snapshot."""

    path: Path
    company_id: str
    platform: str
    output_path: str
    minimum_seed_score: int
    seed_accounts: list[AccountRef]
    active_seed_index: int = 0
    completed_seed_indices: set[int] = field(default_factory=set)
    submitted_children: dict[str, AccountRef] = field(default_factory=dict)
    status: str = "running"
    updated_at: str | None = None

    VALID_STATUSES = {"running", "stopped_rate_limit", "completed"}

    @classmethod
    def new(
        cls,
        path: Path,
        *,
        company_id: str,
        platform: str,
        output_path: Path,
        minimum_seed_score: int,
        seed_accounts: Iterable[AccountRef],
    ) -> "SnowballCheckpoint":
        return cls(
            path=Path(path),
            company_id=str(company_id),
            platform=validate_platform(platform),
            output_path=str(Path(output_path).resolve()),
            minimum_seed_score=int(minimum_seed_score),
            seed_accounts=list(seed_accounts),
        )

    @classmethod
    def load(
        cls,
        path: Path,
        *,
        company_id: str,
        platform: str,
        output_path: Path,
        minimum_seed_score: int,
    ) -> "SnowballCheckpoint":
        path = Path(path)
        try:
            payload = json.loads(path.read_text())
        except FileNotFoundError as exc:
            raise SnowballCheckpointError(f"snowball checkpoint does not exist: {path}") from exc
        except (OSError, json.JSONDecodeError) as exc:
            raise SnowballCheckpointError(f"could not read snowball checkpoint {path}: {exc}") from exc
        if not isinstance(payload, dict):
            raise SnowballCheckpointError("snowball checkpoint must contain an object")

        expected_platform = validate_platform(platform)
        expected_output = str(Path(output_path).resolve())
        if payload.get("schema_version") != 1:
            raise SnowballCheckpointError("unsupported snowball checkpoint schema version")
        if str(payload.get("company_id")) != str(company_id):
            raise SnowballCheckpointError("snowball checkpoint company_id does not match this command")
        if payload.get("platform") != expected_platform:
            raise SnowballCheckpointError("snowball checkpoint platform does not match this command")
        if payload.get("output_path") != expected_output:
            raise SnowballCheckpointError("snowball checkpoint output path does not match this command")
        try:
            checkpoint_minimum_score = int(payload.get("minimum_seed_score", -1))
        except (TypeError, ValueError) as exc:
            raise SnowballCheckpointError("snowball checkpoint seed score threshold must be an integer") from exc
        if checkpoint_minimum_score != int(minimum_seed_score):
            raise SnowballCheckpointError("snowball checkpoint seed score threshold does not match this command")
        status = payload.get("status")
        if status not in cls.VALID_STATUSES:
            raise SnowballCheckpointError("snowball checkpoint has an invalid status")

        raw_seeds = payload.get("seed_accounts")
        if not isinstance(raw_seeds, list):
            raise SnowballCheckpointError("snowball checkpoint seed_accounts must be a list")
        seed_accounts = [_account_from_dict(value) for value in raw_seeds]
        raw_submitted = payload.get("submitted_children", [])
        if not isinstance(raw_submitted, list):
            raise SnowballCheckpointError("snowball checkpoint submitted_children must be a list")
        submitted_children: dict[str, AccountRef] = {}
        for value in raw_submitted:
            account = _account_from_dict(value)
            submitted_children[account.account_id] = account

        try:
            active_seed_index = int(payload.get("active_seed_index", 0))
            completed = {int(index) for index in payload.get("completed_seed_indices", [])}
        except (TypeError, ValueError) as exc:
            raise SnowballCheckpointError("snowball checkpoint seed indexes must be integers") from exc
        if active_seed_index < 0 or active_seed_index > len(seed_accounts):
            raise SnowballCheckpointError("snowball checkpoint active seed index is out of range")
        if any(index < 0 or index >= len(seed_accounts) for index in completed):
            raise SnowballCheckpointError("snowball checkpoint contains an invalid completed seed index")

        return cls(
            path=path,
            company_id=str(company_id),
            platform=expected_platform,
            output_path=expected_output,
            minimum_seed_score=int(minimum_seed_score),
            seed_accounts=seed_accounts,
            active_seed_index=active_seed_index,
            completed_seed_indices=completed,
            submitted_children=submitted_children,
            status=status,
            updated_at=str(payload.get("updated_at")) if payload.get("updated_at") else None,
        )

    def account_was_submitted(self, account: AccountRef) -> bool:
        return account.account_id in self.submitted_children

    def record_submitted(self, account: AccountRef) -> None:
        self.submitted_children[account.account_id] = account

    def mark_seed_completed(self, index: int) -> None:
        if index < 0 or index >= len(self.seed_accounts):
            raise IndexError("snowball seed index is out of range")
        self.completed_seed_indices.add(index)
        self.active_seed_index = max(self.active_seed_index, index + 1)

    def first_unfinished_seed(self) -> int:
        index = self.active_seed_index
        while index < len(self.seed_accounts) and index in self.completed_seed_indices:
            index += 1
        self.active_seed_index = index
        return index

    def set_status(self, status: str) -> None:
        if status not in self.VALID_STATUSES:
            raise ValueError(f"invalid snowball checkpoint status: {status}")
        self.status = status

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "company_id": self.company_id,
            "platform": self.platform,
            "output_path": self.output_path,
            "minimum_seed_score": self.minimum_seed_score,
            "seed_accounts": [account.to_dict() for account in self.seed_accounts],
            "active_seed_index": self.active_seed_index,
            "completed_seed_indices": sorted(self.completed_seed_indices),
            "submitted_children": [
                self.submitted_children[key].to_dict()
                for key in sorted(self.submitted_children)
            ],
            "status": self.status,
            "updated_at": self.updated_at,
        }

    def save(self) -> None:
        """Atomically persist the checkpoint so a controlled stop is resumable."""

        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.updated_at = datetime.now(timezone.utc).isoformat()
        temporary = self.path.with_name(f".{self.path.name}.{os.getpid()}.tmp")
        temporary.write_text(json.dumps(self.to_dict(), indent=2, ensure_ascii=False))
        os.replace(temporary, self.path)
