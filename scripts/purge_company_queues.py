"""Purge one shared platform SQS work queue and matching DLQ.

Queues are platform-scoped, not company-scoped. A purge therefore removes work
for every company on the selected platform. It requires ``--yes`` because SQS
purge is destructive. The script resolves existing queues only; it never
creates queues or changes their attributes.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from x_influencer_discovery.config import load_settings
from x_influencer_discovery.platforms import SUPPORTED_PLATFORMS, dlq_name, queue_name


def _counts(client, queue_url: str) -> tuple[int, int]:
    attributes = client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    return (
        int(attributes.get("ApproximateNumberOfMessages", 0)),
        int(attributes.get("ApproximateNumberOfMessagesNotVisible", 0)),
    )


def _print_counts(client, *, label: str, queue_url: str) -> None:
    visible, in_flight = _counts(client, queue_url)
    print(f"{label}: visible={visible} in_flight={in_flight} total={visible + in_flight}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Destructively purge one shared platform SQS main queue and DLQ."
    )
    parser.add_argument(
        "--platform",
        choices=SUPPORTED_PLATFORMS,
        default="x",
        help="Platform queue to purge (default: x). This affects every company on the platform.",
    )
    parser.add_argument("--env-file", help="Optional path to a dotenv file.")
    parser.add_argument("--yes", action="store_true", help="Confirm permanent deletion of messages in both queues.")
    args = parser.parse_args()

    if not args.yes:
        parser.error("refusing to purge queues without --yes")

    settings = load_settings(args.env_file)
    expected_main_name = queue_name(args.platform)
    if settings.sqs_queue_name and settings.sqs_queue_name != expected_main_name:
        parser.error(f"SQS_QUEUE_NAME must be {expected_main_name} for platform {args.platform!r}")
    expected_dlq_name = dlq_name(args.platform)

    try:
        import boto3

        client = boto3.client(
            "sqs",
            endpoint_url=settings.sqs_endpoint_url,
            region_name=os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION", "us-east-1"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        )
        main_url = client.get_queue_url(QueueName=expected_main_name)["QueueUrl"]
        dlq_url = client.get_queue_url(QueueName=expected_dlq_name)["QueueUrl"]
    except Exception as exc:
        raise SystemExit(f"Could not resolve existing queues for platform {args.platform!r}: {exc}") from exc

    print(f"Platform: {args.platform}")
    print("Warning: this removes messages for every company on this platform.")
    _print_counts(client, label="Main queue before purge", queue_url=main_url)
    _print_counts(client, label="DLQ before purge", queue_url=dlq_url)

    try:
        # Purge the main queue first, then the DLQ to clear any message already
        # redriven before this command began.
        client.purge_queue(QueueUrl=main_url)
        client.purge_queue(QueueUrl=dlq_url)
    except Exception as exc:
        raise SystemExit(f"Purge request failed: {exc}") from exc

    print("Purge requested for both queues. SQS may take up to 60 seconds to reflect zero counts.")
    _print_counts(client, label="Main queue after request", queue_url=main_url)
    _print_counts(client, label="DLQ after request", queue_url=dlq_url)


if __name__ == "__main__":
    main()
