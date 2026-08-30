from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable
from urllib.parse import quote_plus, urlparse

try:
    from pymongo import MongoClient
except Exception:  # pragma: no cover - dependency/environment specific
    MongoClient = None


DESTINATION_COLLECTION_NAME = "dashboard_companydashboard"
_VALID_STATUSES = {"contacted", "uncontacted"}
_VALID_RELEVANCIES = {"", "relevant", "irrelevant"}


class DashboardExportError(ValueError):
    """Raised when dashboard export cannot safely be completed."""


@dataclass(frozen=True)
class DashboardPublishReport:
    company_domain: str
    rows_written: int
    matched_count: int
    modified_count: int
    dry_run: bool = False
    rows: list[dict[str, Any]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "company_domain": self.company_domain,
            "rows_written": self.rows_written,
            "matched_count": self.matched_count,
            "modified_count": self.modified_count,
            "dry_run": self.dry_run,
        }


def build_dashboard_mongodb_url(
    *,
    host: str | None,
    database: str | None,
    username: str | None = None,
    password: str | None = None,
) -> str:
    """Build the Robotic Marketer dashboard MongoDB URI from environment values."""

    host = str(host or "").strip()
    database = str(database or "").strip()
    if not host:
        raise DashboardExportError("DATABASE_HOST is required when publishing the dashboard leaderboard")
    if not database:
        raise DashboardExportError("DATABASE_NAME is required when publishing the dashboard leaderboard")

    encoded_database = quote_plus(database)
    if host in {"localhost", "127.0.0.1"}:
        return f"mongodb://{host}/{encoded_database}"

    if not username:
        raise DashboardExportError(
            "DATABASE_USERNAME is required for a non-local dashboard MongoDB host"
        )
    if not password:
        raise DashboardExportError(
            "DATABASE_PASSWORD is required for a non-local dashboard MongoDB host"
        )
    return (
        f"mongodb+srv://{quote_plus(str(username))}:{quote_plus(str(password))}"
        f"@{host}/{encoded_database}?retryWrites=true&w=majority"
    )


@contextmanager
def open_dashboard_collection(mongodb_url: str):
    """Open the deployed dashboard collection and always close its client."""

    if MongoClient is None:  # pragma: no cover - dependency/environment specific
        raise RuntimeError("pymongo is required to publish the dashboard leaderboard")
    client = MongoClient(mongodb_url, serverSelectionTimeoutMS=5_000)
    try:
        try:
            database = client.get_default_database()
        except Exception:
            parsed = urlparse(mongodb_url)
            database_name = parsed.path.lstrip("/").split("?", 1)[0]
            if not database_name:
                raise DashboardExportError("destination MongoDB URI must include a database name")
            database = client[database_name]
        yield database[DESTINATION_COLLECTION_NAME]
    finally:
        client.close()


def map_candidates_to_dashboard_rows(
    candidates: Iterable[dict[str, Any]],
    *,
    existing_rows: Iterable[dict[str, Any]] = (),
) -> list[dict[str, Any]]:
    """Map durable candidate documents into the dashboard's row contract."""

    existing_flags = _existing_flags(existing_rows)
    rows: list[dict[str, Any]] = []
    seen_handles: set[str] = set()
    for index, candidate in enumerate(candidates, start=1):
        row = _map_candidate(candidate, index=index, existing_flags=existing_flags)
        if row["name"] in seen_handles:
            raise DashboardExportError(
                f"source leaderboard contains duplicate handle {row['name']!r}"
            )
        seen_handles.add(row["name"])
        rows.append(row)
    return rows


def publish_dashboard_influencers(
    mongodb_url: str,
    company_domain: str,
    candidates: Iterable[dict[str, Any]],
    *,
    dry_run: bool = False,
    now: datetime | None = None,
) -> DashboardPublishReport:
    """Replace one company's dashboard influencer array with mapped candidates."""

    company_domain = str(company_domain or "").strip()
    if not company_domain:
        raise DashboardExportError("company domain must not be empty")

    candidate_list = list(candidates)
    if not candidate_list:
        raise DashboardExportError(
            "no fresh leaderboard candidates remain; refusing to change the dashboard"
        )

    selector = {"company_domain_id": company_domain}
    with open_dashboard_collection(mongodb_url) as collection:
        current = collection.find_one(
            selector,
            {"influencers.0.data.influencers": 1},
        )
        if current is None:
            raise DashboardExportError(
                f"no dashboard company document found for company_domain_id={company_domain!r}"
            )

        existing_rows = _nested_influencer_rows(current)
        rows = map_candidates_to_dashboard_rows(candidate_list, existing_rows=existing_rows)
        if dry_run:
            return DashboardPublishReport(
                company_domain=company_domain,
                rows_written=len(rows),
                matched_count=1,
                modified_count=0,
                dry_run=True,
                rows=rows,
            )

        result = collection.update_one(
            selector,
            {
                "$set": {
                    "influencers.0.data.influencers": rows,
                    "influencers.0.date": now or datetime.now(timezone.utc),
                }
            },
        )
        matched_count = int(getattr(result, "matched_count", 0))
        if matched_count != 1:
            raise DashboardExportError(
                f"dashboard update matched {matched_count} documents for "
                f"company_domain_id={company_domain!r}; expected exactly 1"
            )
        return DashboardPublishReport(
            company_domain=company_domain,
            rows_written=len(rows),
            matched_count=matched_count,
            modified_count=int(getattr(result, "modified_count", 0)),
            rows=rows,
        )


def _map_candidate(
    candidate: dict[str, Any],
    *,
    index: int,
    existing_flags: dict[str, dict[str, str]],
) -> dict[str, Any]:
    if not isinstance(candidate, dict):
        raise DashboardExportError(f"source leaderboard row {index} must be an object")
    account = candidate.get("account") or {}
    profile = candidate.get("profile") or {}
    if not isinstance(account, dict) or not isinstance(profile, dict):
        raise DashboardExportError(f"source leaderboard row {index} has invalid account/profile data")

    handle = _normalize_handle(account.get("handle") or candidate.get("handle"))
    if not handle:
        raise DashboardExportError(f"source leaderboard row {index} is missing an X handle")

    image = profile.get("avatar_url") or profile.get("profile_img_url")
    if not isinstance(image, str) or not _is_http_url(image.strip()):
        raise DashboardExportError(
            f"source leaderboard row {index} ({handle}) is missing a valid profile image URL"
        )

    followers = _follower_count(profile.get("followers"))
    if followers is None:
        raise DashboardExportError(
            f"source leaderboard row {index} ({handle}) is missing a numeric follower count"
        )

    flags = existing_flags.get(handle, {})
    return {
        "name": handle,
        "img": image.strip(),
        "followers": followers,
        "status": flags.get("status", "uncontacted"),
        "relevancy": flags.get("relevancy", ""),
    }


def _existing_flags(rows: Iterable[dict[str, Any]]) -> dict[str, dict[str, str]]:
    flags: dict[str, dict[str, str]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        handle = _normalize_handle(row.get("name"))
        if not handle:
            continue
        status = row.get("status")
        relevancy = row.get("relevancy")
        flags[handle] = {
            "status": status if isinstance(status, str) and status in _VALID_STATUSES else "uncontacted",
            "relevancy": relevancy if isinstance(relevancy, str) and relevancy in _VALID_RELEVANCIES else "",
        }
    return flags


def _nested_influencer_rows(document: dict[str, Any]) -> list[dict[str, Any]]:
    influencers = document.get("influencers")
    if not isinstance(influencers, list) or not influencers:
        return []
    first = influencers[0]
    if not isinstance(first, dict):
        return []
    data = first.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("influencers"), list):
        return []
    return data["influencers"]


def _normalize_handle(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lstrip("@").casefold()
    return normalized or None


def _follower_count(value: Any) -> int | None:
    if isinstance(value, dict):
        value = value.get("estimated")
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float) and value.is_integer() and value >= 0:
        return int(value)
    return None


def _is_http_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)
