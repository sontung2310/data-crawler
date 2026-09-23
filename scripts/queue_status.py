"""Print current SQS work and DLQ counts for one shared platform queue.

Queues are platform-scoped, not company-scoped. Every company on the selected
platform shares the same main queue and DLQ. This script resolves existing
queues only; it never creates them.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Allow ``python scripts/queue_status.py`` from the repository root without
# requiring the package to be installed into the virtual environment.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from x_influencer_discovery.config import load_settings
from x_influencer_discovery.platforms import SUPPORTED_PLATFORMS, dlq_name, queue_name


def _counts(client, queue_url: str) -> tuple[int, int]:
    attributes = client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
        ],
    )["Attributes"]
    visible = int(attributes.get("ApproximateNumberOfMessages", 0))
    in_flight = int(attributes.get("ApproximateNumberOfMessagesNotVisible", 0))
    return visible, in_flight


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Show SQS queue counts for one shared platform work queue and DLQ."
    )
    parser.add_argument(
        "--platform",
        choices=SUPPORTED_PLATFORMS,
        default="x",
        help="Platform queue to inspect (default: x). Counts include every company on the platform.",
    )
    parser.add_argument("--env-file", help="Optional path to a dotenv file.")
    args = parser.parse_args()

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

    main_visible, main_in_flight = _counts(client, main_url)
    dlq_visible, dlq_in_flight = _counts(client, dlq_url)

    print(f"Platform: {args.platform}")
    print(f"Main queue ({expected_main_name}): visible={main_visible} in_flight={main_in_flight} total={main_visible + main_in_flight}")
    print(f"DLQ ({expected_dlq_name}):        visible={dlq_visible} in_flight={dlq_in_flight} total={dlq_visible + dlq_in_flight}")


if __name__ == "__main__":
    main()
