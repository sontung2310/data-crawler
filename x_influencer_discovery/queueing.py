"""Platform-aware durable FIFO message and SQS primitives."""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from dataclasses import dataclass
from typing import Any

from .models import AccountRef
from .platforms import (
    account_group_id,
    account_ref,
    normalize_profile_url,
    queue_name as platform_queue_name,
    validate_platform,
)


@dataclass(frozen=True)
class QueueMessage:
    """One platform-account evaluation job."""

    profile_url: str
    company_id: str | None = None
    platform: str = "x"
    account_id: str | None = None
    handle: str | None = None

    @classmethod
    def from_url(
        cls,
        value: str,
        *,
        company_id: str | None = None,
        platform: str = "x",
        account_id: str | None = None,
        handle: str | None = None,
    ) -> "QueueMessage":
        platform = validate_platform(platform)
        canonical = normalize_profile_url(value, platform)
        reference = account_ref(
            platform,
            account_id=account_id,
            handle=handle,
            profile_url=canonical,
        )
        return cls(
            profile_url=canonical,
            company_id=str(company_id) if company_id is not None else None,
            platform=platform,
            account_id=reference.account_id,
            handle=reference.handle,
        )

    @classmethod
    def from_account(
        cls,
        *,
        company_id: str,
        platform: str,
        account: AccountRef | dict[str, Any],
    ) -> "QueueMessage":
        values = account.to_dict() if isinstance(account, AccountRef) else dict(account)
        profile_url = values.get("profile_url")
        if not profile_url:
            raise ValueError("queue account.profile_url is required")
        return cls.from_url(
            str(profile_url),
            company_id=company_id,
            platform=platform,
            account_id=values.get("account_id"),
            handle=values.get("handle"),
        )

    @property
    def job_identity(self) -> str:
        if self.account_id is None:
            return f"{self.company_id or ''}:{self.platform}:{self.profile_url}"
        return f"{self.company_id or ''}:{self.platform}:{self.account_id}"

    @property
    def account(self) -> AccountRef:
        return AccountRef(
            account_id=self.account_id or f"url:{self.profile_url}",
            handle=self.handle,
            profile_url=self.profile_url,
        )

    def body(self) -> str:
        if self.company_id is None:
            return json.dumps({"profile_url": self.profile_url}, separators=(",", ":"))
        return json.dumps(
            {
                "company_id": self.company_id,
                "platform": self.platform,
                "account": self.account.to_dict(),
            },
            separators=(",", ":"),
        )

    @classmethod
    def from_body(cls, value: str) -> "QueueMessage":
        try:
            parsed = json.loads(value)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError("queue message body is not valid JSON") from exc
        if not isinstance(parsed, dict):
            raise ValueError("queue message body must be an object")
        if set(parsed) == {"profile_url"}:
            return cls.from_url(str(parsed["profile_url"]))
        if set(parsed) != {"company_id", "platform", "account"}:
            raise ValueError("queue message body must contain company_id, platform, and account")
        if not isinstance(parsed["account"], dict):
            raise ValueError("queue message account must be an object")
        account = parsed["account"]
        if set(account) - {"account_id", "handle", "profile_url"}:
            raise ValueError("queue account contains unsupported fields")
        if not account.get("profile_url"):
            raise ValueError("queue account.profile_url is required")
        if not parsed.get("company_id"):
            raise ValueError("queue company_id must not be empty")
        return cls.from_url(
            str(account["profile_url"]),
            company_id=str(parsed["company_id"]),
            platform=str(parsed["platform"]),
            account_id=str(account["account_id"]) if account.get("account_id") is not None else None,
            handle=str(account["handle"]) if account.get("handle") is not None else None,
        )


@dataclass(frozen=True)
class ReceivedProfileMessage:
    """Transient delivery metadata; it is never stored in MongoDB."""

    profile_url: str | None
    receipt_handle: str
    message_id: str
    receive_count: int
    parse_error: str | None = None
    company_id: str | None = None
    platform: str = "x"
    account_id: str | None = None
    handle: str | None = None

    @property
    def job_identity(self) -> str | None:
        if not self.profile_url:
            return None
        account_id = self.account_id or f"url:{self.profile_url}"
        return f"{self.company_id or ''}:{self.platform}:{account_id}"

    @property
    def account(self) -> AccountRef | None:
        if not self.profile_url:
            return None
        return AccountRef(
            account_id=self.account_id or f"url:{self.profile_url}",
            handle=self.handle,
            profile_url=self.profile_url,
        )


class DurableProfileQueue:
    """SQS FIFO adapter with one shared queue per platform."""

    def __init__(
        self,
        client: Any,
        *,
        queue_url: str,
        company_slug: str | None = None,
        company_id: str | None = None,
        platform: str = "x",
        dlq_url: str | None = None,
        receive_batch_size: int = 10,
        receive_wait_time_seconds: int = 2,
        visibility_timeout_seconds: int = 180,
    ):
        self.client = client
        self.queue_url = queue_url
        self.company_slug = company_slug
        self.company_id = company_id
        self.platform = validate_platform(platform)
        self.dlq_url = dlq_url
        self.receive_batch_size = min(10, max(1, receive_batch_size))
        self.receive_wait_time_seconds = min(20, max(0, receive_wait_time_seconds))
        self.visibility_timeout_seconds = visibility_timeout_seconds
        self._seen_jobs: set[str] = set()
        self._seen_urls: set[str] = set()
        self._run_id = uuid.uuid4().hex

    @staticmethod
    def company_slug(company_name: str) -> str:
        slug = re.sub(r"[^a-z0-9]+", "-", company_name.casefold()).strip("-")
        if not slug:
            raise ValueError("company name must contain letters or digits")
        return slug

    @staticmethod
    def profile_group_id(value: str) -> str:
        profile_url = normalize_profile_url(value)
        return f"profile-{hashlib.sha256(profile_url.encode('utf-8')).hexdigest()}"

    @staticmethod
    def account_group_id(platform: str, account_id: str) -> str:
        return account_group_id(platform, account_id)

    @classmethod
    def from_settings(
        cls,
        settings: Any,
        company_name: str | None = None,
        *,
        company_id: str | None = None,
        platform: str = "x",
        client: Any | None = None,
    ) -> "DurableProfileQueue":
        platform = validate_platform(platform)
        resolved_company_id = company_id or (cls.company_slug(company_name) if company_name else None)
        if client is None:
            try:
                import boto3
                client = boto3.client(
                    "sqs",
                    endpoint_url=settings.sqs_endpoint_url,
                    region_name=os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION", "us-east-1"),
                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
                    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
                )
            except Exception as exc:  # pragma: no cover - environment-specific
                raise RuntimeError("Unable to initialize the SQS client; install a compatible boto3/SSL stack") from exc
        expected_name = platform_queue_name(platform)
        name = settings.sqs_queue_name or expected_name
        if name != expected_name:
            raise ValueError(f"SQS_QUEUE_NAME must be the platform queue {expected_name}")
        return cls.provision(
            client,
            queue_name=name,
            company_id=resolved_company_id,
            platform=platform,
            receive_batch_size=settings.sqs_receive_batch_size,
            receive_wait_time_seconds=settings.sqs_receive_wait_time_seconds,
            visibility_timeout_seconds=settings.sqs_visibility_timeout_seconds,
            max_receive_count=getattr(settings, "sqs_max_receive_count", 1),
        )

    @classmethod
    def provision(
        cls,
        client: Any,
        *,
        queue_name: str,
        company_slug: str | None = None,
        company_id: str | None = None,
        platform: str = "x",
        receive_batch_size: int = 10,
        receive_wait_time_seconds: int = 2,
        visibility_timeout_seconds: int = 180,
        max_receive_count: int = 1,
    ) -> "DurableProfileQueue":
        if not queue_name.endswith(".fifo"):
            raise ValueError("SQS profile queue names must end in .fifo")
        dlq_name = f"{queue_name[:-5]}-dlq.fifo"
        dlq_url = cls._queue_url(client, dlq_name, {"FifoQueue": "true"})
        attrs = client.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"]
        queue_url = cls._queue_url(
            client,
            queue_name,
            {
                "FifoQueue": "true",
                "VisibilityTimeout": str(visibility_timeout_seconds),
                "RedrivePolicy": json.dumps({
                    "deadLetterTargetArn": attrs["QueueArn"],
                    "maxReceiveCount": str(max_receive_count),
                }),
            },
        )
        return cls(
            client,
            queue_url=queue_url,
            company_slug=company_slug,
            company_id=company_id,
            platform=platform,
            dlq_url=dlq_url,
            receive_batch_size=receive_batch_size,
            receive_wait_time_seconds=receive_wait_time_seconds,
            visibility_timeout_seconds=visibility_timeout_seconds,
        )

    @staticmethod
    def _queue_url(client: Any, name: str, attributes: dict[str, str]) -> str:
        try:
            queue_url = client.get_queue_url(QueueName=name)["QueueUrl"]
        except Exception as exc:
            code = getattr(exc, "response", {}).get("Error", {}).get("Code")
            if code and code != "AWS.SimpleQueueService.NonExistentQueue":
                raise
            return client.create_queue(QueueName=name, Attributes=attributes)["QueueUrl"]
        mutable = {key: value for key, value in attributes.items() if key != "FifoQueue"}
        if mutable and hasattr(client, "set_queue_attributes"):
            client.set_queue_attributes(QueueUrl=queue_url, Attributes=mutable)
        return queue_url

    def send(
        self,
        value: str | QueueMessage,
        *,
        company_id: str | None = None,
        platform: str | None = None,
        account_id: str | None = None,
        handle: str | None = None,
    ) -> bool:
        return self._send(value, company_id, platform, account_id, handle)

    def send_snowball(
        self,
        value: str | QueueMessage,
        *,
        company_id: str | None = None,
        platform: str | None = None,
        account_id: str | None = None,
        handle: str | None = None,
    ) -> bool:
        return self._send(value, company_id, platform, account_id, handle)

    def begin_run(self) -> None:
        self._seen_jobs.clear()
        self._seen_urls.clear()
        self._run_id = uuid.uuid4().hex

    def _message_for(
        self,
        value: str | QueueMessage,
        company_id: str | None,
        platform: str | None,
        account_id: str | None,
        handle: str | None,
    ) -> QueueMessage:
        if isinstance(value, QueueMessage):
            if company_id is None and self.company_id is not None:
                company_id = self.company_id
            if platform is None:
                platform = self.platform
            if value.company_id == company_id and value.platform == platform:
                return value
            return QueueMessage.from_url(
                value.profile_url,
                company_id=company_id,
                platform=platform or value.platform,
                account_id=account_id or value.account_id,
                handle=handle or value.handle,
            )
        return QueueMessage.from_url(
            value,
            company_id=company_id if company_id is not None else self.company_id,
            platform=platform or self.platform,
            account_id=account_id,
            handle=handle,
        )

    def _send(
        self,
        value: str | QueueMessage,
        company_id: str | None,
        platform: str | None,
        account_id: str | None,
        handle: str | None,
    ) -> bool:
        message = self._message_for(value, company_id, platform, account_id, handle)
        identity = message.job_identity
        if identity in self._seen_jobs:
            return False
        self._seen_jobs.add(identity)
        self._seen_urls.add(message.profile_url)
        group_id = (
            self.profile_group_id(message.profile_url)
            if message.company_id is None
            else self.account_group_id(message.platform, message.account_id or f"url:{message.profile_url}")
        )
        try:
            self.client.send_message(
                QueueUrl=self.queue_url,
                MessageBody=message.body(),
                MessageGroupId=group_id,
                MessageDeduplicationId=hashlib.sha256(
                    f"{self._run_id}:{identity}".encode("utf-8")
                ).hexdigest(),
            )
        except Exception:
            self._seen_jobs.discard(identity)
            self._seen_urls.discard(message.profile_url)
            raise
        return True

    def receive(self) -> list[ReceivedProfileMessage]:
        response = self.client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=self.receive_batch_size,
            WaitTimeSeconds=self.receive_wait_time_seconds,
            VisibilityTimeout=self.visibility_timeout_seconds,
            AttributeNames=["ApproximateReceiveCount", "MessageGroupId"],
        )
        output = []
        for raw in response.get("Messages", []):
            try:
                parsed = QueueMessage.from_body(raw.get("Body", ""))
                if parsed.company_id is None and self.company_id is not None:
                    parsed = QueueMessage.from_url(
                        parsed.profile_url,
                        company_id=self.company_id,
                        platform=self.platform,
                        account_id=parsed.account_id,
                        handle=parsed.handle,
                    )
                if parsed.platform != self.platform:
                    raise ValueError("queue message platform does not match the queue")
                message_group_id = raw.get("Attributes", {}).get("MessageGroupId")
                allowed_groups = {
                    None,
                    self.company_slug,
                    self.profile_group_id(parsed.profile_url),
                    self.account_group_id(parsed.platform, parsed.account_id or f"url:{parsed.profile_url}"),
                }
                if message_group_id not in allowed_groups:
                    error = (
                        "queue message group does not match the company"
                        if self.company_id is None and self.company_slug
                        else "queue message group does not match the platform account"
                    )
                    raise ValueError(error)
                parse_error = None
            except ValueError as exc:
                parsed = None
                parse_error = str(exc)
            attributes = raw.get("Attributes", {})
            output.append(ReceivedProfileMessage(
                parsed.profile_url if parsed else None,
                raw["ReceiptHandle"],
                raw.get("MessageId", ""),
                int(attributes.get("ApproximateReceiveCount", 1)),
                parse_error,
                parsed.company_id if parsed else None,
                parsed.platform if parsed else self.platform,
                parsed.account_id if parsed and (parsed.company_id is not None or self.company_id is not None) else None,
                parsed.handle if parsed and (parsed.company_id is not None or self.company_id is not None) else None,
            ))
        return output

    def acknowledge(self, message: ReceivedProfileMessage) -> None:
        self.client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=message.receipt_handle)

    def extend_visibility(self, message: ReceivedProfileMessage) -> None:
        self.client.change_message_visibility(
            QueueUrl=self.queue_url,
            ReceiptHandle=message.receipt_handle,
            VisibilityTimeout=self.visibility_timeout_seconds,
        )

    def release_for_retry(self, message: ReceivedProfileMessage) -> None:
        self.client.change_message_visibility(
            QueueUrl=self.queue_url,
            ReceiptHandle=message.receipt_handle,
            VisibilityTimeout=0,
        )

    def defer_for_manual_resume(self, message: ReceivedProfileMessage) -> bool:
        if not message.profile_url:
            raise ValueError("cannot defer a queue message without a profile URL")
        replacement = QueueMessage.from_url(
            message.profile_url,
            company_id=message.company_id or self.company_id,
            platform=message.platform or self.platform,
            account_id=message.account_id,
            handle=message.handle,
        )
        group_id = (
            self.profile_group_id(replacement.profile_url)
            if replacement.company_id is None
            else self.account_group_id(replacement.platform, replacement.account_id or f"url:{replacement.profile_url}")
        )
        self.client.send_message(
            QueueUrl=self.queue_url,
            MessageBody=replacement.body(),
            MessageGroupId=group_id,
            MessageDeduplicationId=hashlib.sha256(
                f"{self._run_id}:deferred:{message.message_id}:{uuid.uuid4().hex}:{replacement.job_identity}".encode("utf-8")
            ).hexdigest(),
        )
        self.acknowledge(message)
        return True

    def depth(self) -> int:
        response = self.client.get_queue_attributes(
            QueueUrl=self.queue_url,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
        )
        attrs = response.get("Attributes", {})
        return int(attrs.get("ApproximateNumberOfMessages", 0)) + int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))

    def dead_letter_depth(self) -> int:
        if not self.dlq_url:
            return 0
        response = self.client.get_queue_attributes(
            QueueUrl=self.dlq_url,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
        )
        attrs = response.get("Attributes", {})
        return int(attrs.get("ApproximateNumberOfMessages", 0)) + int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
