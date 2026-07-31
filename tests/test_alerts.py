#!/usr/bin/env python3
"""
Unit + optional live tests for app.alerts email on session expiry.

Run from the project root (mocked SMTP only — safe for CI):
    python -m tests.test_alerts

Send a real email using SMTP_* / ALERT_EMAIL_* from .env:
    python -m tests.test_alerts --live
    python -m tests.test_alerts --live-only
"""

from __future__ import annotations

import argparse
import logging
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import config  # noqa: E402  — loads .env
from app import alerts  # noqa: E402


def _smtp_configured() -> bool:
    return bool(config.ALERT_EMAIL_TO and config.SMTP_HOST and config.ALERT_EMAIL_FROM)


class TestNotifySessionExpired(unittest.TestCase):
    def test_logs_and_delegates_to_send_email(self) -> None:
        with (
            patch.object(alerts, "_send_email") as send,
            self.assertLogs(alerts.logger, level="ERROR") as cm,
        ):
            alerts.notify_session_expired("reddit", "login wall detected")

        send.assert_called_once()
        kwargs = send.call_args.kwargs
        self.assertIn("reddit", kwargs["subject"])
        self.assertIn("login wall detected", kwargs["body"])
        self.assertTrue(any("session_expired" in line for line in cm.output))


class TestSendEmailMocked(unittest.TestCase):
    def test_skips_when_not_configured(self) -> None:
        with (
            patch.object(alerts, "ALERT_EMAIL_TO", ""),
            patch.object(alerts, "SMTP_HOST", "smtp.gmail.com"),
            patch.object(alerts, "ALERT_EMAIL_FROM", "from@example.com"),
            patch.object(alerts, "smtplib") as mock_smtplib,
        ):
            alerts._send_email(subject="s", body="b")
        mock_smtplib.SMTP.assert_not_called()

    def test_sends_via_smtp_when_configured(self) -> None:
        smtp = MagicMock()
        with (
            patch.object(alerts, "ALERT_EMAIL_TO", "to@example.com"),
            patch.object(alerts, "ALERT_EMAIL_FROM", "from@example.com"),
            patch.object(alerts, "SMTP_HOST", "smtp.example.com"),
            patch.object(alerts, "SMTP_PORT", 587),
            patch.object(alerts, "SMTP_USE_TLS", True),
            patch.object(alerts, "SMTP_USER", "user"),
            patch.object(alerts, "SMTP_PASSWORD", "pass"),
            patch.object(alerts.smtplib, "SMTP", return_value=smtp) as smtp_cls,
        ):
            smtp.__enter__.return_value = smtp
            alerts._send_email(subject="hello", body="world")

        smtp_cls.assert_called_once_with("smtp.example.com", 587, timeout=10)
        smtp.starttls.assert_called_once()
        smtp.login.assert_called_once_with("user", "pass")
        smtp.send_message.assert_called_once()
        msg = smtp.send_message.call_args.args[0]
        self.assertEqual(msg["Subject"], "hello")
        self.assertEqual(msg["From"], "from@example.com")
        self.assertEqual(msg["To"], "to@example.com")
        self.assertEqual(msg.get_content().strip(), "world")

    def test_smtp_failure_is_swallowed(self) -> None:
        with (
            patch.object(alerts, "ALERT_EMAIL_TO", "to@example.com"),
            patch.object(alerts, "ALERT_EMAIL_FROM", "from@example.com"),
            patch.object(alerts, "SMTP_HOST", "smtp.example.com"),
            patch.object(alerts.smtplib, "SMTP", side_effect=OSError("boom")),
            self.assertLogs(alerts.logger, level="WARNING") as cm,
        ):
            alerts._send_email(subject="s", body="b")  # must not raise

        self.assertTrue(any("alert email failed" in line for line in cm.output))


def _run_mocked() -> int:
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(TestNotifySessionExpired))
    suite.addTests(loader.loadTestsFromTestCase(TestSendEmailMocked))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


def _run_live() -> int:
    """Real SMTP send — only invoked via --live / --live-only (not unittest discovery)."""
    if not _smtp_configured():
        print(
            "Live send failed: set ALERT_EMAIL_TO, ALERT_EMAIL_FROM, and SMTP_HOST in .env",
            file=sys.stderr,
        )
        return 1
    if not (config.SMTP_USER and config.SMTP_PASSWORD):
        print(
            "Warning: SMTP_USER / SMTP_PASSWORD empty — Gmail usually requires an App Password.",
            file=sys.stderr,
        )

    # Bind current config into app.alerts (imported names are copies at import time).
    alerts.ALERT_EMAIL_TO = config.ALERT_EMAIL_TO
    alerts.ALERT_EMAIL_FROM = config.ALERT_EMAIL_FROM
    alerts.SMTP_HOST = config.SMTP_HOST
    alerts.SMTP_PORT = config.SMTP_PORT
    alerts.SMTP_USER = config.SMTP_USER
    alerts.SMTP_PASSWORD = config.SMTP_PASSWORD
    alerts.SMTP_USE_TLS = config.SMTP_USE_TLS

    print(
        f"Sending live session_expired alert to {config.ALERT_EMAIL_TO} "
        f"via {config.SMTP_HOST}:{config.SMTP_PORT} ...",
        flush=True,
    )

    class _Capture(logging.Handler):
        def __init__(self) -> None:
            super().__init__()
            self.messages: list[str] = []

        def emit(self, record: logging.LogRecord) -> None:
            self.messages.append(record.getMessage())

    capture = _Capture()
    alerts.logger.addHandler(capture)
    try:
        alerts.notify_session_expired(
            "test_alerts",
            "Live smoke test from tests.test_alerts — safe to ignore.",
        )
    finally:
        alerts.logger.removeHandler(capture)

    failed = [m for m in capture.messages if "alert email failed" in m]
    if failed:
        print("\n".join(failed), file=sys.stderr)
        return 1

    print(
        "SMTP accepted the message. Check Inbox and Spam for "
        f"{config.ALERT_EMAIL_TO}.",
        flush=True,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--live",
        action="store_true",
        help="Also send a real email via .env SMTP settings (check Gmail inbox/Spam).",
    )
    parser.add_argument(
        "--live-only",
        action="store_true",
        help="Skip mocked tests; only send the live email.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO)

    if args.live_only:
        return _run_live()

    code = _run_mocked()
    if args.live:
        live_code = _run_live()
        return code or live_code
    return code


if __name__ == "__main__":
    raise SystemExit(main())
