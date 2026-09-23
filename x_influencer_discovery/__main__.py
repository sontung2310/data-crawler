from __future__ import annotations

import argparse
import json
import logging
import re
from dataclasses import replace
from datetime import datetime
from pathlib import Path

from .config import load_settings
from .dashboard_export import (
    DashboardExportError,
    build_dashboard_mongodb_url,
    publish_dashboard_influencers,
)
from .evaluation import compare_outputs
from .pipeline import GracefulShutdownRequested, PendingQueueWorkError, run, run_snowball
from .platforms import SUPPORTED_PLATFORMS
from .migration import migrate_candidate_documents
from .snowball import SnowballCheckpointError, progress_path_for_output
from .storage import open_company_candidate_store, save_json
from .worker_lock import XWorkerLockUnavailableError


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def _nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must not be negative")
    return parsed


def _related_terms(value: str) -> list[str]:
    return [term.strip() for term in value.split(",") if term.strip()]


def _output_stem(field: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", field.casefold()).strip("_") or "query"


def main() -> None:
    """Run the package directly with ``python -m x_influencer_discovery``."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Find the best N influencers in a field on X without the X API.")
    subcommands = parser.add_subparsers(dest="command", required=True)

    run_parser = subcommands.add_parser("run", help="Run influencer discovery")
    run_parser.add_argument("--query", help="Field/topic (backward-compatible alias for --field)")
    run_parser.add_argument("--field")
    run_parser.add_argument("--n", type=_positive_int, help="Result limit (backward-compatible alias for --limit)")
    run_parser.add_argument("--limit", type=_positive_int)
    run_parser.add_argument("--company-name", required=True)
    run_parser.add_argument("--company-id", help="Stable company identity (defaults to a slug of --company-name)")
    run_parser.add_argument("--company-domain", required=True)
    run_parser.add_argument("--platform", choices=SUPPORTED_PLATFORMS, default="x")
    run_parser.add_argument("--company-summary", help="Required summary used for local semantic relevance; may also come from COMPANY_SUMMARY")
    run_parser.add_argument("--related-terms", type=_related_terms, required=True)
    run_parser.add_argument("--minimum-followers", type=_positive_int)
    run_parser.add_argument("--minimum-relevance-score", type=float)
    run_parser.add_argument("--good-hybrid-relevance-threshold", type=float)
    run_parser.add_argument("--account-attempt-timeout-seconds", type=_positive_int)
    run_parser.add_argument("--enable-snowball", action=argparse.BooleanOptionalAction, default=None)
    run_parser.add_argument(
        "--resume",
        action="store_true",
        help="Drain the existing company queue without running discovery again (use after an interrupted run).",
    )
    run_parser.add_argument("--output", type=Path)
    run_parser.add_argument("--env-file")

    snowball_parser = subcommands.add_parser(
        "snowball",
        help="Expand eligible X candidates through the accounts they follow",
    )
    snowball_parser.add_argument("--limit", type=_positive_int, default=10)
    snowball_parser.add_argument("--company-name", required=True)
    snowball_parser.add_argument("--company-id")
    snowball_parser.add_argument("--company-domain", required=True)
    snowball_parser.add_argument("--platform", choices=SUPPORTED_PLATFORMS, default="x")
    snowball_parser.add_argument(
        "--company-summary",
        help="Required summary used for local semantic relevance; may also come from COMPANY_SUMMARY",
    )
    snowball_parser.add_argument("--related-terms", type=_related_terms, required=True)
    snowball_parser.add_argument("--minimum-seed-score", type=_nonnegative_int, default=60)
    snowball_parser.add_argument("--seed-limit", type=_positive_int)
    snowball_parser.add_argument("--account-attempt-timeout-seconds", type=_positive_int)
    snowball_parser.add_argument("--resume", action="store_true")
    snowball_parser.add_argument("--output", type=Path)
    snowball_parser.add_argument("--progress-file", type=Path)
    snowball_parser.add_argument("--env-file")

    export_parser = subcommands.add_parser(
        "export",
        help="Publish the fresh durable X leaderboard to the dashboard without discovery",
    )
    export_parser.add_argument("--company-id", required=True, help="Stable company identity used in the source leaderboard")
    export_parser.add_argument("--company-domain", required=True)
    export_parser.add_argument("--platform", choices=SUPPORTED_PLATFORMS, default="x")
    export_parser.add_argument("--limit", type=_positive_int, default=10)
    export_parser.add_argument("--output", type=Path, required=True)
    export_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Map and validate the dashboard payload without updating the destination database",
    )
    export_parser.add_argument("--env-file")

    migrate_parser = subcommands.add_parser("migrate", help="Copy one legacy company collection into the shared candidate collection")
    migrate_parser.add_argument("--company-id", required=True)
    migrate_parser.add_argument("--company-name", required=True)
    migrate_parser.add_argument("--source-collection")
    migrate_parser.add_argument("--platform", choices=SUPPORTED_PLATFORMS, default="x")
    migrate_parser.add_argument("--dry-run", action="store_true")
    migrate_parser.add_argument("--env-file")

    eval_parser = subcommands.add_parser("eval", help="Compare output with a reference JSON file")
    eval_parser.add_argument("--actual", type=Path, required=True)
    eval_parser.add_argument("--expected", type=Path, required=True)
    eval_parser.add_argument("--top-n", type=int, default=10)

    args = parser.parse_args()
    if args.command == "run":
        field = args.field or args.query
        limit = args.limit or args.n
        if not field:
            run_parser.error("one of --field or --query is required")
        if not limit:
            run_parser.error("one of --limit or --n is required")
        for option in ("minimum_relevance_score", "good_hybrid_relevance_threshold"):
            if getattr(args, option) is not None and not 0 <= getattr(args, option) <= 1:
                run_parser.error(f"--{option.replace('_', '-')} must be between 0 and 1")
        settings = load_settings(args.env_file)
        if not (args.company_summary or settings.company_summary):
            run_parser.error("--company-summary or COMPANY_SUMMARY is required")
        if not args.related_terms:
            run_parser.error("--related-terms must contain at least one term")
        settings = replace(
            settings,
            company_summary=args.company_summary or settings.company_summary,
            minimum_followers=args.minimum_followers or settings.minimum_followers,
            minimum_relevance_score=args.minimum_relevance_score if args.minimum_relevance_score is not None else settings.minimum_relevance_score,
            good_hybrid_relevance_threshold=args.good_hybrid_relevance_threshold if args.good_hybrid_relevance_threshold is not None else settings.good_hybrid_relevance_threshold,
            enable_snowball=settings.enable_snowball if args.enable_snowball is None else args.enable_snowball,
            platform=args.platform,
            account_attempt_timeout_seconds=args.account_attempt_timeout_seconds or settings.account_attempt_timeout_seconds,
        )
        output_path = args.output or settings.data_dir / f"{_output_stem(field)}_x_influencers.json"
        try:
            output = run(
                field,
                int(limit),
                settings,
                output_path,
                company_id=args.company_id,
                company_name=args.company_name,
                company_domain=args.company_domain,
                platform=args.platform,
                related_terms=args.related_terms,
                produce_discovery=not args.resume,
            )
        except (XWorkerLockUnavailableError, PendingQueueWorkError) as exc:
            run_parser.error(str(exc))
        except (GracefulShutdownRequested, KeyboardInterrupt) as exc:
            signal_name = exc.signal_name if isinstance(exc, GracefulShutdownRequested) else "SIGINT"
            logging.getLogger(__name__).info(
                "discovery stopped safely (%s); unfinished queue work remains available. Resume with the same command plus --resume.",
                signal_name,
            )
            raise SystemExit(130) from None
        print(json.dumps({
            "status": output.get("status", "completed"),
            "output": str(output_path),
            "x_fetch_report": output.get("x_fetch_report"),
            "results_count": len(output.get("results", [])),
            "handles": [item.get("account", {}).get("handle") for item in output.get("results", [])],
        }, indent=2, ensure_ascii=False))
    elif args.command == "snowball":
        settings = load_settings(args.env_file)
        if not (args.company_summary or settings.company_summary):
            snowball_parser.error("--company-summary or COMPANY_SUMMARY is required")
        if not args.related_terms:
            snowball_parser.error("--related-terms must contain at least one term")
        settings = replace(
            settings,
            company_summary=args.company_summary or settings.company_summary,
            platform=args.platform,
            account_attempt_timeout_seconds=(
                args.account_attempt_timeout_seconds
                or settings.account_attempt_timeout_seconds
            ),
        )
        output_path = args.output or settings.data_dir / f"{_output_stem(args.company_name)}_snowball_x_influencers.json"
        progress_path = args.progress_file or progress_path_for_output(output_path)
        try:
            output = run_snowball(
                "snowball",
                args.limit,
                settings,
                output_path,
                company_id=args.company_id,
                company_name=args.company_name,
                company_domain=args.company_domain,
                platform=args.platform,
                related_terms=args.related_terms,
                minimum_seed_score=args.minimum_seed_score,
                seed_limit=args.seed_limit,
                resume=args.resume,
                progress_path=progress_path,
            )
        except (XWorkerLockUnavailableError, PendingQueueWorkError, SnowballCheckpointError, ValueError) as exc:
            snowball_parser.error(str(exc))
        except (GracefulShutdownRequested, KeyboardInterrupt) as exc:
            signal_name = exc.signal_name if isinstance(exc, GracefulShutdownRequested) else "SIGINT"
            logging.getLogger(__name__).info(
                "snowball stopped safely (%s); unfinished queue and checkpoint work remain available. Resume with --resume.",
                signal_name,
            )
            raise SystemExit(130) from None
        print(json.dumps({
            "status": output.get("status", "completed"),
            "output": str(output_path),
            "progress_file": output.get("snowball_progress_file", str(progress_path)),
            "x_fetch_report": output.get("x_fetch_report"),
            "results_count": len(output.get("results", [])),
            "handles": [item.get("account", {}).get("handle") for item in output.get("results", [])],
        }, indent=2, ensure_ascii=False))
    elif args.command == "export":
        settings = load_settings(args.env_file)
        if not settings.mongodb_url:
            export_parser.error("MONGODB_URI is required to export the durable company leaderboard")
        if args.platform != "x":
            export_parser.error("dashboard export currently supports only the X platform")
        store_context = open_company_candidate_store(
            settings.mongodb_url,
            company_id=args.company_id,
            platform=args.platform,
        )
        with store_context as store:
            try:
                results = store.leaderboard(
                    company_id=args.company_id,
                    platform=args.platform,
                    limit=args.limit,
                    max_age_days=settings.leaderboard_max_age_days,
                )
            except TypeError:
                results = store.leaderboard(limit=args.limit)
            output = {
                "company_id": args.company_id,
                "company_domain": args.company_domain,
                "platform": args.platform,
                "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
                "results": results,
            }

        try:
            dashboard_url = build_dashboard_mongodb_url(
                host=settings.database_host,
                database=settings.database_name,
                username=settings.database_username,
                password=settings.database_password,
            )
            publish_report = publish_dashboard_influencers(
                dashboard_url,
                args.company_domain,
                output["results"],
                dry_run=args.dry_run,
            )
        except (DashboardExportError, RuntimeError, OSError) as exc:
            export_parser.error(str(exc))
        output["dashboard_rows"] = publish_report.rows or []
        output["dashboard_export"] = publish_report.to_dict()
        save_json(output, args.output)
        print(json.dumps({
            "status": "dry_run" if args.dry_run else "completed",
            "output": str(args.output),
            "results_count": len(output["results"]),
            "handles": [item.get("account", {}).get("handle") for item in output["results"]],
            "dashboard_export": output["dashboard_export"],
        }, indent=2, ensure_ascii=False))
    elif args.command == "migrate":
        settings = load_settings(args.env_file)
        if not settings.mongodb_url:
            migrate_parser.error("MONGODB_URI is required for migration")
        try:
            from pymongo import MongoClient
        except Exception as exc:  # pragma: no cover - environment-specific
            migrate_parser.error(f"pymongo is required for migration: {exc}")
        client = MongoClient(settings.mongodb_url, serverSelectionTimeoutMS=5_000)
        try:
            database = client.get_default_database()
            source_name = args.source_collection or re.sub(
                r"[^a-z0-9]+", "_", args.company_name.casefold()
            ).strip("_") + "_candidates"
            report = migrate_candidate_documents(
                database[source_name].find({}),
                database["influencer_candidates"],
                company_id=args.company_id,
                company_name=args.company_name,
                platform=args.platform,
                dry_run=args.dry_run,
            )
        finally:
            client.close()
        print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))
    else:
        actual = json.loads(args.actual.read_text())
        expected = json.loads(args.expected.read_text())
        print(json.dumps(compare_outputs(actual, expected, args.top_n), indent=2))

if __name__ == "__main__":
    main()
